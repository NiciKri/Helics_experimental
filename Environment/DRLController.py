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
    Each call to step(actions) runs one full HELICS simulation from t=0 to t=sim_time.
    - `actions` is now shape=(n_nodes,): one scalar shift per node.
    - We hold those per-node shifts constant throughout the sim (zero during startup).
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
        self.for_eval = for_eval

        # ─── Gym action/observation spaces ─────────────────────────────────────
        # Now one action per node
        self.action_space = spaces.Box(
            low=-0.1,
            high=0.1,
            shape=(self.n_nodes,),
            dtype=np.float64
        )
        # Observation per node is 5-dim, overall obs = (n_nodes, 5)
        self.observation_space = spaces.Box(
            low=-np.inf,
            high=np.inf,
            shape=(self.n_nodes, 5),
            dtype=np.float64
        )

        # ─── Controller parameters ─────────────────────────────────────────────
        self.startup_time = 50
        self.gain = 1e8
        self.m = 1

        # ─── HELICS handles & histories ────────────────────────────────────────
        self.action_interval = config.ACTION_INTERVAL
        self.v_hist = {n: deque(maxlen=2) for n in self.node_names}
        self.psi_prev = {n: 0.0 for n in self.node_names}
        self.epsilon_history = {n: deque(maxlen=10) for n in self.node_names}

        self.controller_on = True
        self.broker = None
        self.threads = []
        self.fed = None
        self.sub_voltage = None
        self.sub_healthy = None
        self.sub_flag = None
        self.pub = None
        self.current_time = 0.0

    def _thread_wrapper(self, fn, args):
        def wrapped():
            try:
                fn(*args)
            except Exception as e:
                logging.exception(f"Error in {fn.__name__}: {e}")
        return wrapped

    def _teardown(self):
        if self.fed:
            try:
                h.helicsFederateDisconnect(self.fed)
            except Exception:
                pass
            h.helicsFederateFree(self.fed)
            self.fed = None

        for t in self.threads:
            if t.is_alive():
                t.join(timeout=5.0)
        self.threads = []

        if self.broker:
            try:
                if h.helicsBrokerIsConnected(self.broker):
                    h.helicsBrokerDisconnect(self.broker)
                    h.helicsBrokerWaitForDisconnect(self.broker, 5000)
                h.helicsBrokerFree(self.broker)
            except Exception as e:
                logging.warning(f"Broker finalization failed: {e}")
            self.broker = None

    def reset(self, *, seed=None, options=None):
        dummy_obs = np.zeros((self.n_nodes, 5), dtype=np.float64)
        return dummy_obs, {}

    def step(self, actions: np.ndarray):
        """
        RUN A FULL SIMULATION from t=0 to t=self.sim_time.
        `actions` should be an array of shape (n_nodes,), one shift per node.
        """
        # 1) Teardown any old HELICS sim
        if self.fed or self.broker or self.threads:
            self._teardown()
            time.sleep(2)  # allow ports to free

        # Validate incoming actions
        assert isinstance(actions, np.ndarray)
        assert actions.shape == (self.n_nodes,)
        current_actions = actions.copy()

        # 2) Launch broker + federates
        self.broker = h.helicsCreateBroker(
            'zmq', '', f'--federates=6 --loglevel=error --autobroker'
        )
        federates = [
            (voltage_consumer_federate.run_voltage_consumer_federate,
             (test_solar_data, test_load_data, self.node_names, self.sim_time, self.dt)),
            (opendss_federate.run_opendss_federate, ()),
            (attack_federate.run_attack_federate,
             (hacks_list, breaking_points, self.sim_time, self.dt)),
            (inverter_federate.run_inverter_federate,
             (self.node_names, self.sim_time, self.dt, breaking_points, sbar_df)),
            (logger_federate.run_logging_federate, (self.sim_time, self.dt)),
        ]
        for fn, args in federates:
            t = threading.Thread(target=self._thread_wrapper(fn, args), daemon=True)
            t.start()
            self.threads.append(t)
            time.sleep(0.5)

        # 3) Controller federate
        fi = h.helicsCreateFederateInfo()
        core_name = f"core_{int(time.time())}"
        h.helicsFederateInfoSetCoreName(fi, core_name)
        h.helicsFederateInfoSetCoreTypeFromString(fi, 'zmq')
        h.helicsFederateInfoSetTimeProperty(fi, h.HELICS_PROPERTY_TIME_DELTA, self.dt)
        self.fed = h.helicsCreateValueFederate('Adaptive_Controller_Federate', fi)

        self.sub_voltage = h.helicsFederateRegisterSubscription(
            self.fed, 'OpenDSS_Federate/voltage_out', ''
        )
        self.sub_healthy = h.helicsFederateRegisterSubscription(
            self.fed, 'Attack_Federate/healthy_breakpoints', ''
        )
        self.sub_flag = h.helicsFederateRegisterSubscription(
            self.fed, 'Attack_Federate/attack_flag', ''
        )
        self.pub = h.helicsFederateRegisterPublication(
            self.fed, 'adaptive_breakpoints', h.HELICS_DATA_TYPE_STRING, ''
        )

        h.helicsFederateEnterExecutingMode(self.fed)
        self.current_time = h.helicsFederateRequestTime(self.fed, 0.0)

        # 4) Time loop
        total_reward = 0.0
        last_obs = None

        while True:
            t_next = self.current_time + self.dt
            self.current_time = h.helicsFederateRequestTime(self.fed, t_next)

            # Controller off during startup
            self.controller_on = (self.current_time >= self.startup_time)
            if not self.controller_on:
                actions_to_apply = np.zeros_like(current_actions)
            else:
                actions_to_apply = current_actions

            # Read observations & accumulate reward
            obs = self._read_obs()
            last_obs = obs.copy()
            inst_reward = -float(np.sum(obs[:, 4]))
            total_reward += inst_reward

            if self.current_time >= self.sim_time:
                break

            # Fetch healthy breakpoints if updated
            healthy_msg = {}
            if h.helicsInputIsUpdated(self.sub_healthy):
                try:
                    healthy_str = h.helicsInputGetString(self.sub_healthy)
                    healthy_msg = eval(healthy_str) if healthy_str.strip().startswith('{') else {}
                except Exception:
                    logging.warning("Failed to parse healthy breakpoint input")

            # Build and publish adaptive breakpoints
            adaptive = {}
            for i, n in enumerate(self.node_names):
                shift_val = float(actions_to_apply[i])
                segments = healthy_msg.get(
                    n,
                    [{'pct': 1.0, 'bp': [0.98, 1.01, 1.02, 1.05, 1.07]}]
                )
                bp = segments[0]['bp']
                if self.controller_on:
                    segments[0]['bp'] = [bp_j - shift_val for bp_j in bp]
                adaptive[n] = segments

            h.helicsPublicationPublishString(self.pub, str(adaptive))

        # 5) Teardown HELICS & clear histories
        self._teardown()
        for n in self.node_names:
            self.v_hist[n].clear()
            self.psi_prev[n] = 0.0
            self.epsilon_history[n].clear()

        # 6) Return
        info = {"total_reward": total_reward}
        return last_obs, total_reward, True, False, info

    def _read_obs(self):
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
        self._teardown()
