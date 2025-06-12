import helics as h
import time
import numpy as np
import pandas as pd
from collections import deque
import config

# ─── Load CSVs for voltage data ───────────────────────────────────────────
test_solar_data = pd.read_csv(f"{config.DATA_DIR}/solar_data.csv")
test_solar_data.columns = (
    test_solar_data.columns
        .str.replace('_pv$', '', regex=True)
        .str.replace('S', 's')
)
test_solar_data['time'] = test_solar_data.index

# ─── Helper: Normalizer ──────────────────────────────────────────────────
class Normalizer:
    def __init__(self, size, eps=1e-8):
        self.mean = np.zeros(size)
        self.var = np.ones(size)
        self.count = eps

    def normalize(self, x):
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.var += delta * delta2
        std = np.sqrt(self.var / self.count)
        return (x - self.mean) / (std + 1e-8)

# ─── Policies: Linear & MLP ───────────────────────────────────────────────
class LinearPolicy:
    def __init__(self, state_dim, action_dim):
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.theta = np.zeros((action_dim, state_dim))

    def set_policy(self, theta_flat):
        self.theta = theta_flat.reshape(self.action_dim, self.state_dim)

    def compute_action(self, state):
        x = state.flatten()
        return self.theta.dot(x)

class MLPPolicy:
    def __init__(self, state_dim, action_dim, hidden_dim=64):
        # two-layer MLP: W1, b1, W2, b2
        self.shapes = [
            (hidden_dim, state_dim),
            (hidden_dim,),
            (action_dim, hidden_dim),
            (action_dim,)
        ]
        sizes = [np.prod(s) for s in self.shapes]
        self.total_size = sum(sizes)
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.hidden_dim = hidden_dim
        self.theta = np.zeros(self.total_size)

    def _unpack(self, theta):
        params, idx = [], 0
        for shape in self.shapes:
            size = np.prod(shape)
            flat = theta[idx:idx+size]
            params.append(flat.reshape(shape))
            idx += size
        return params

    def set_policy(self, theta_flat):
        self.theta = theta_flat.copy()

    def compute_action(self, state):
        x = state.flatten()
        W1, b1, W2, b2 = self._unpack(self.theta)
        h1 = np.tanh(W1.dot(x) + b1)
        return W2.dot(h1) + b2

# ─── Federate that loads a trained model and applies it ────────────────────
def run_DRL_policy_federate(simulation_time, time_step=1.0, model_path="final_theta.npy"):
    # HELICS setup
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Adaptive_Controller_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo,
                                         h.HELICS_PROPERTY_TIME_DELTA,
                                         time_step)
    fed = h.helicsCreateValueFederate("Adaptive_Controller_Federate", fedinfo)

    # Subscriptions & publication
    sub_voltage = h.helicsFederateRegisterSubscription(fed,
        "OpenDSS_Federate/voltage_out", "")
    sub_healthy = h.helicsFederateRegisterSubscription(fed,
        "Attack_Federate/healthy_breakpoints", "")
    pub = h.helicsFederateRegisterPublication(fed,
        "adaptive_breakpoints", h.HELICS_DATA_TYPE_STRING, "")

    h.helicsFederateEnterExecutingMode(fed)
    current_time = 0.0

    # Determine state dim: nodes * features per node (5)
    node_names = [c for c in test_solar_data.columns if c != 'time']
    state_dim = len(node_names) * 5
    action_dim = 1

    # Load trained policy
    theta = np.load(model_path)
    # Choose policy type based on theta shape
    if theta.ndim == 1:
        policy = MLPPolicy(state_dim, action_dim, hidden_dim=getattr(config, 'HIDDEN_DIM', 64))
    else:
        policy = LinearPolicy(state_dim, action_dim)
    policy.set_policy(theta)
    normalizer = Normalizer(state_dim)

    # Initialize histories
    v_hist = {n: deque(maxlen=2) for n in node_names}
    psi_prev = {n: 0.0 for n in node_names}
    epsilon_history = {n: deque(maxlen=10) for n in node_names}

    # Time loop
    while current_time < simulation_time:
        next_time = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_time)

        # Get voltage input
        while not h.helicsInputIsUpdated(sub_voltage):
            time.sleep(0.001)
        vs = h.helicsInputGetString(sub_voltage)
        vd = eval(vs) if vs.strip().startswith('{') else {}

        # Build observation
        obs_list = []
        for n in node_names:
            vk = vd.get(n, 1.0)
            hist = v_hist[n]; hist.append(vk)
            if len(hist) < 2:
                obs_list.append([vk, vk, 0.0, 0.0, 0.0]); continue
            vkm1 = hist[-2]; psi_old = psi_prev[n]
            psik = ((vk - vkm1) - (config.m * time_step/2 - 1)*psi_old) / (1 + config.m * time_step/2)
            psi_prev[n] = psik
            epsk = config.gain * psik**2 if current_time >= config.startup_time else 0.0
            epsilon_history[n].append(epsk)
            yk = float(np.mean(epsilon_history[n]))
            obs_list.append([vk, vkm1, psi_old, epsk, yk])

        obs = np.array(obs_list, dtype=np.float64)
        flat = obs.flatten()
        x = normalizer.normalize(flat)

        # Compute action
        action = policy.compute_action(x)
        shift = float(action)  # scalar

        # Build adaptive breakpoints
        adaptive = {}
        healthy = {}  # assume healthy breakpoints come from attack federate if needed
        for n in node_names:
            segments = healthy.get(n, [{'pct':1.0, 'bp':[0.98,1.01,1.02,1.05,1.07]}])
            seg = segments[0].copy(); bp = seg['bp']
            seg['bp'] = [b - shift for b in bp]
            adaptive[n] = [seg]

        h.helicsPublicationPublishString(pub, str(adaptive))

    # Cleanup
    h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
    print("[Adaptive Controller Inference] Finalized.")
