import threading
import time
import helics as h
import numpy as np
import gymnasium as gym
from gymnasium import spaces
import pandas as pd
import config
from collections import deque
import logging

# ─── Federate modules ───────────────────────────────────────────────────────
from federates import (
    opendss_federate,
    voltage_consumer_federate,
    attack_federate,
    inverter_federate,
    logger_federate
)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)8s [%(threadName)s] %(message)s',
    datefmt='%H:%M:%S'
)

# ─── Load CSVs once, outside the class ───────────────────────────────────────
test_solar_data = pd.read_csv(f"{config.DATA_DIR}/solar_data.csv")
test_solar_data.columns = (
    test_solar_data.columns
        .str.replace('_pv$', '', regex=True)
        .str.replace('S', 's')
)
test_solar_data['time'] = test_solar_data.index

test_load_data = pd.read_csv(f"{config.DATA_DIR}/load_data.csv")
test_load_data.columns = test_load_data.columns.str.replace('S', 's')
test_load_data['time'] = test_load_data.index
test_load_data.sort_values('time', inplace=True)

breaking_points = pd.read_csv(f"{config.DATA_DIR}/solar_VV_breakpoints.csv")
breaking_points.columns = (
    breaking_points.columns
        .str.replace('_pv$', '', regex=True)
        .str.replace('S', 's')
)

max_solar = test_solar_data.drop(columns='time').max()
sbar_df = pd.DataFrame([max_solar])
sbar_df.columns = sbar_df.columns.str.replace('S', 's')

hacks_list = config.hacks_list
node_names = [c for c in test_solar_data.columns if c != 'time']


class DRLControllerEnv(gym.Env):
    """
    Each call to step(global_action) runs one full HELICS simulation from t=0 to t=sim_time.
    - `global_action` is now shape=(2,).
    - Inside the HELICS loop, every ACTION_INTERVAL sub‐steps, we recompute a new
      per‐node action by calling _node_policy(node_obs, global_action).
    - We still only return a single cumulative reward at the very end (done=True).
    """
    metadata = {"render_modes": []}

    def __init__(self, simulation_time, time_step, for_eval=False):
        super().__init__()

        # ─── Simulation parameters ──────────────────────────────────────────────
        self.sim_time = simulation_time
        self.dt = time_step
        self.node_names = node_names
        self.n_nodes = len(self.node_names)
        self.for_eval = for_eval  # If True, skip full teardown on exit

        # ─── Gym action/observation spaces ─────────────────────────────────────
        # Now action_space is just DIM=2: (dp_global, dq_global)
        self.action_space = spaces.Box(
            low=-1.0e-5, high=1.0e-5, shape=(2,), dtype=np.float64
        )
        # Observation per node is 5‐dim, but overall obs = (n_nodes, 5)
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(self.n_nodes, 5), dtype=np.float64
        )

        # ─── Controller parameters ─────────────────────────────────────────────
        self.startup_time = 50
        self.gain = 1e8
        self.m = 1

        # ─── Every ACTION_INTERVAL sub‐steps, recompute per‐node actions ───────
        self.action_interval = config.ACTION_INTERVAL  # e.g. 10

        # ─── Initialize per‐node histories ────────────────────────────────────
        self.v_hist = {n: deque(maxlen=2) for n in self.node_names}
        self.psi_prev = {n: 0.0 for n in self.node_names}
        self.epsilon_history = {n: deque(maxlen=10) for n in self.node_names}

        # ─── HELICS handles (initialized to None) ─────────────────────────────
        self.controller_on = True
        self.broker = None
        self.threads = []       # will hold all federate threads
        self.fed = None         # the HELICS ValueFederate for controller
        self.sub_voltage = None
        self.sub_healthy = None
        self.sub_flag = None
        self.pub = None
        self.current_time = 0.0

    def _thread_wrapper(self, fn, args):
        """
        Wrap federate launches so exceptions are logged.
        """
        def wrapped():
            try:
                fn(*args)
            except Exception as e:
                logging.exception(f"Error in {fn.__name__}: {e}")
        return wrapped

    def _teardown(self):
        """
        Finalize any existing HELICS federates & broker, join threads, free resources.
        """
        logging.debug("Tearing down HELICS simulation...")
        # 1) Finalize + disconnect controller federate
        if self.fed:
            try:
                h.helicsFederateFinalize(self.fed)
                logging.debug("Controller federate finalized.")
            except Exception:
                pass
            try:
                h.helicsFederateDisconnect(self.fed)
                logging.debug("Controller federate disconnected.")
            except Exception:
                pass
            h.helicsFederateFree(self.fed)
            self.fed = None

        # 2) Join all federate threads (timeout=5s)
        for i, t in enumerate(self.threads):
            if t.is_alive():
                logging.debug(f"Joining thread {i} ...")
                t.join(timeout=5.0)
        self.threads = []

        # 3) Disconnect + free the broker
        if self.broker:
            try:
                if h.helicsBrokerIsConnected(self.broker):
                    logging.debug("Disconnecting broker...")
                    h.helicsBrokerDisconnect(self.broker)
                    h.helicsBrokerWaitForDisconnect(self.broker, 5000)
                h.helicsBrokerFree(self.broker)
                logging.debug("Broker freed.")
            except Exception as e:
                logging.warning(f"Broker finalization failed: {e}")
            self.broker = None

    def reset(self, *, seed=None, options=None):
        """
        Reset() no longer launches HELICS. It just returns a dummy observation.
        All history‐clearing happens at the end of each step().
        """
        logging.debug("Reset called (no HELICS teardown here).")
        dummy_obs = np.zeros((self.n_nodes, 5), dtype=np.float64)
        return dummy_obs, {}

    def _node_policy(self, node_obs: np.ndarray, global_action: np.ndarray) -> np.ndarray:
        """
        Given a 5‐dim node_obs and the 2‐dim global_action, return a new 2‐dim action for that node.
        ----- CHANGE THIS to call your real policy (e.g. concatenate node_obs & global_action, feed it
        into a neural net, etc.). For now, we just add tiny random noise to global_action.
        """
        # Placeholder: return global_action + small noise
        noise = np.random.normal(scale=0.1, size=(2,))
        node_action = global_action + noise
        # Clip to action_space bounds:
        return np.clip(node_action, self.action_space.low, self.action_space.high)

    def step(self, global_action: np.ndarray):
        """
        RUN A FULL SIMULATION from t=0 to t=self.sim_time. Inside that loop,
        every self.action_interval sub‐steps, we recompute a per‐node action by calling
        _node_policy(node_obs, global_action). We only return one reward at the end.
        """
        # ─── 1) Teardown any old HELICS sim ───────────────────────────────────
        if self.fed is not None or self.broker is not None or len(self.threads) > 0:
            self._teardown()
            time.sleep(2)  # allow ports to free

        # ─── 2) Launch a brand‐new HELICS simulation ──────────────────────────
        logging.debug("Creating new HELICS broker (auto port)...")
        self.broker = h.helicsCreateBroker(
            'zmq', '', f'--federates=6 --loglevel=warning --autobroker'
        )
        logging.debug(f"Broker created. Connected: {h.helicsBrokerIsConnected(self.broker)}")

        # 2a) Spawn federate threads
        federates = [
            (voltage_consumer_federate.run_voltage_consumer_federate,
            (test_solar_data, test_load_data, self.node_names, self.sim_time, self.dt),),
            (opendss_federate.run_opendss_federate, ()),
            (
                attack_federate.run_attack_federate,
                (hacks_list, breaking_points, self.sim_time, self.dt),
            ),
            (
                inverter_federate.run_inverter_federate,
                (self.node_names, self.sim_time, self.dt, breaking_points, sbar_df),
            ),
            (logger_federate.run_logging_federate, (self.sim_time, self.dt)),
        ]
        for fn, args in federates:
            logging.debug(f"Starting thread for {fn.__name__} ...")
            t = threading.Thread(target=self._thread_wrapper(fn, args), daemon=True)
            t.start()
            self.threads.append(t)
            time.sleep(0.5)

        # 2b) Create and configure the controller federate
        logging.debug("Creating controller federate ...")
        fi = h.helicsCreateFederateInfo()
        core_name = f"core_{int(time.time())}"
        h.helicsFederateInfoSetCoreName(fi, core_name)
        h.helicsFederateInfoSetCoreTypeFromString(fi, 'zmq')
        h.helicsFederateInfoSetTimeProperty(fi, h.HELICS_PROPERTY_TIME_DELTA, self.dt)
        self.fed = h.helicsCreateValueFederate('Adaptive_Controller_Federate', fi)

        # Register subscriptions and publications
        self.sub_voltage = h.helicsFederateRegisterSubscription(self.fed, 'OpenDSS_Federate/voltage_out', '')
        self.sub_healthy = h.helicsFederateRegisterSubscription(self.fed, 'Attack_Federate/healthy_breakpoints', '')
        self.sub_flag = h.helicsFederateRegisterSubscription(self.fed, 'Attack_Federate/attack_flag', '')
        self.pub = h.helicsFederateRegisterPublication(self.fed, 'adaptive_breakpoints', h.HELICS_DATA_TYPE_STRING, '')

        # Enter executing mode
        h.helicsFederateEnterExecutingMode(self.fed)
        self.current_time = h.helicsFederateRequestTime(self.fed, 0.0)

        # ─── 3) RUN THE TIME‐LOOP FROM t=0 TO t=sim_time ───────────────────────
        total_reward = 0.0
        last_obs = None


        # Initialize per‐node actions by simply broadcasting the global_action to all nodes
        current_actions = np.tile(global_action.reshape(1, 2), (self.n_nodes, 1))

        while True:
            # 3a) Advance HELICS time by dt
            t_next = self.current_time + self.dt
            self.current_time = h.helicsFederateRequestTime(self.fed, t_next)

            # 3b) Check startup_time
            self.controller_on = (self.current_time >= self.startup_time)
            if not self.controller_on:
                # During startup, enforce zero action on all nodes
                actions_to_apply = np.zeros_like(current_actions)
            else:
                # Every action_interval sub‐steps, recompute per‐node actions
                if self.current_time % self.action_interval == 0:
                    # We already read obs last iteration; but to be safe, read again:
                    temp_obs = self._read_obs()  # shape = (n_nodes, 5)
                    # Build a new (n_nodes, 2) array
                    new_actions = np.zeros((self.n_nodes, 2), dtype=np.float64)
                    for i in range(self.n_nodes):
                        node_obs = temp_obs[i]  # shape=(5,)
                        # Compute node_i's action from node_obs and the *global_action*
                        dp_i, dq_i = self._node_policy(node_obs, global_action)
                        new_actions[i] = [dp_i, dq_i]

                        # Print actions for s701a and s701b
                        node_name = self.node_names[i]
                        if node_name in ("s701a", "-"):
                            print(f"Node {node_name}: dp = {dp_i:.4f}, dq = {dq_i:.4f}")
                    current_actions = new_actions

                actions_to_apply = current_actions

            # 3c) Read voltages & build obs array
            obs = self._read_obs()
            last_obs = obs.copy()

            # 3d) Instantaneous reward = – sum of y_k across nodes (obs[:,4])
            inst_reward = -float(np.sum(obs[:, 4]))
            total_reward += inst_reward

            # 3e) Termination check
            if self.current_time >= self.sim_time:
                break

            # 3f) Fetch “healthy breakpoints” from attack federate if updated
            healthy_msg = {}
            if h.helicsInputIsUpdated(self.sub_healthy):
                try:
                    healthy_str = h.helicsInputGetString(self.sub_healthy)
                    healthy_msg = eval(healthy_str) if healthy_str.strip().startswith('{') else {}
                except Exception:
                    logging.warning("Failed to parse healthy breakpoint input")

            # 3g) Build new adaptive breakpoints dictionary using actions_to_apply
            adaptive = {}
            for i, (n) in enumerate(self.node_names):
                dp, dq = actions_to_apply[i]
                segments = healthy_msg.get(
                    n,
                    [{'pct': 1.0, 'bp': [0.98, 1.01, 1.02, 1.05, 1.07]}]
                )
                bp = segments[0]['bp']
                if self.controller_on:
                    new_bp = [
                        type(bp[0])(bp[0] - dq),
                        type(bp[1])(bp[1] - dq),
                        type(bp[2])(bp[2] - dq),
                        type(bp[3])(bp[3] - dp),
                        type(bp[4])(bp[4] - dp)
                    ]
                else:
                    new_bp = bp
                segments[0]['bp'] = new_bp
                adaptive[n] = segments

            # 3h) Publish the adaptive breakpoints
            h.helicsPublicationPublishString(self.pub, str(adaptive))

        # ─── 4) Teardown HELICS ─────────────────────────────────────────────────
        self._teardown()

        # ─── 5) CLEAR ALL PER‐NODE HISTORIES (AFTER sim completes) ─────────────
        for n in self.node_names:
            self.v_hist[n].clear()
            self.psi_prev[n] = 0.0
            self.epsilon_history[n].clear()

        # ─── 6) Build “info” dict ──────────────────────────────────────────────
        info = {"total_reward": total_reward}

        # ─── 7) Return final_obs, cumulative reward, terminated=True ──────────
        terminated = True
        truncated = False
        return last_obs, total_reward, terminated, truncated, info

    def _read_obs(self):
        """
        Exactly as before: read voltages, compute high‐pass filter, eps, y_k, etc.
        Returns an (n_nodes × 5) NumPy array of [v_k, v_{k-1}, psi_{k-1}, eps_k, y_k].
        """
        timeout = 0
        while not h.helicsInputIsUpdated(self.sub_voltage) and timeout < 100:
            time.sleep(0.01)
            timeout += 1

        try:
            vs = h.helicsInputGetString(self.sub_voltage)
            vd = eval(vs) if vs.strip().startswith('{') else {}
        except Exception:
            vd = {}

        obs_list = []
        for n in self.node_names:
            alt = n[1:] if n.startswith('s') else n
            vk = vd.get(n, vd.get(alt, 1.0))

            hist = self.v_hist[n]
            hist.append(vk)
            if len(hist) < 2:
                obs_list.append([vk, vk, 0.0, 0.0, 0.0])
                continue

            vkm1 = hist[-2]
            psi_old = self.psi_prev[n]
            psik = ((vk - vkm1) - (self.m * self.dt / 2 - 1) * psi_old) \
                   / (1 + self.m * self.dt / 2)
            self.psi_prev[n] = psik

            epsk = self.gain * psik**2 if self.current_time > self.startup_time else 0.0
            self.epsilon_history[n].append(epsk)

            yk = float(np.mean(self.epsilon_history[n]))
            obs_list.append([vk, vkm1, psi_old, epsk, yk])

        return np.array(obs_list, dtype=np.float64)

    def close(self):
        """
        Public close(): just teardown if needed.
        """
        self._teardown()
