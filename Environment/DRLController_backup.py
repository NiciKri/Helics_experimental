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

# Federate modules
from federates import (
    opendss_federate,
    voltage_consumer_federate,
    attack_federate,
    inverter_federate,
    logger_federate
)

# configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)8s [%(threadName)s] %(message)s',
    datefmt='%H:%M:%S'
)

# Load data
test_solar_data = pd.read_csv(f"{config.DATA_DIR}/solar_data.csv")
test_solar_data.columns = (
    test_solar_data.columns
        .str.replace('_pv$', '', regex=True)
        .str.replace('S', 's')
)
test_solar_data['time'] = test_solar_data.index

# Load data
test_load_data = pd.read_csv(f"{config.DATA_DIR}/load_data.csv")
test_load_data.columns = test_load_data.columns.str.replace('S', 's')
test_load_data['time'] = test_load_data.index
test_load_data.sort_values('time', inplace=True)

# Breakpoints data
breaking_points = pd.read_csv(f"{config.DATA_DIR}/solar_VV_breakpoints.csv")
breaking_points.columns = (
    breaking_points.columns
        .str.replace('_pv$', '', regex=True)
        .str.replace('S', 's')
)

# Max solar production per node -> sbar_df
max_solar = test_solar_data.drop(columns='time').max()
sbar_df = pd.DataFrame([max_solar])
sbar_df.columns = sbar_df.columns.str.replace('S', 's')

# Hacks list & node names
hacks_list = config.hacks_list
node_names = [c for c in test_solar_data.columns if c != 'time']


class DRLControllerEnv(gym.Env):
    metadata = {"render_modes": []}

    def __init__(self, simulation_time, time_step, for_eval=False):
        super().__init__()
        self.sim_time = simulation_time
        self.dt = time_step
        self.node_names = node_names
        self.n_nodes = len(self.node_names)
        self.for_eval = for_eval

        self.action_space = spaces.Box(
            low=-1.0, high=1.0, shape=(self.n_nodes, 2), dtype=np.float64
        )
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(self.n_nodes, 5), dtype=np.float64
        )

        # Controller parameters
        self.startup_time = 50
        self.gain = 1e8
        self.m = 1

        # Initialize per-node controller state
        self.v_hist = {n: deque(maxlen=2) for n in self.node_names}
        self.psi_prev = {n: 0.0 for n in self.node_names}
        self.epsilon_history = {n: deque(maxlen=10) for n in self.node_names}

        # HELICS simulation elements
        self.controller_on = True
        self.broker = None
        self.threads = []
        self.fed = None
        self.sub_voltage = None
        self.sub_healthy = None
        self.sub_flag = None
        self.pub = None
        self.current_time = 0.0
        self.teardown_lock = threading.Lock()
        self.reset_lock = threading.Lock()

    def _thread_wrapper(self, fn, args):
        def wrapped():
            try:
                fn(*args)
            except Exception as e:
                logging.exception(f"Error in {fn.__name__}: {e}")
        return wrapped

    def _teardown(self):
        with self.teardown_lock:
            logging.debug("Tearing down simulation...")
            if self.fed:
                try:
                    h.helicsFederateFinalize(self.fed)
                    logging.debug("Federate finalized.")
                except Exception:
                    pass
                try:
                    h.helicsFederateDisconnect(self.fed)
                    logging.debug("Federate disconnected.")
                except Exception:
                    pass
                h.helicsFederateFree(self.fed)
                self.fed = None

            for i, t in enumerate(self.threads):
                if t.is_alive():
                    logging.debug(f"Joining thread {i}...")
                    t.join(timeout=5.0)
            self.threads = []

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
        with self.reset_lock:
            logging.debug("Reset called.")
            if not self.for_eval:
                self._teardown()
                time.sleep(5)  # Allow ports to release

            logging.debug("Creating HELICS broker (auto port)...")
            self.broker = h.helicsCreateBroker(
                'zmq', '', f'--federates=6 --loglevel=warning --autobroker'
            )
            logging.debug(f"Broker created. Connected: {h.helicsBrokerIsConnected(self.broker)}")

            federates = [
                (
                    voltage_consumer_federate.run_voltage_consumer_federate,
                    (test_solar_data, test_load_data, self.node_names, self.sim_time, self.dt),
                ),
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
                logging.debug(f"Starting thread for {fn.__name__}...")
                t = threading.Thread(target=self._thread_wrapper(fn, args), daemon=True)
                t.start()
                self.threads.append(t)
                time.sleep(0.5)

            logging.debug("Creating and initializing controller federate...")
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

            for n in self.node_names:
                self.v_hist[n].clear()
                self.psi_prev[n] = 0.0
                self.epsilon_history[n].clear()

            obs = self._read_obs()
            return obs, {}

    def step(self, action):
        t_next = self.current_time + self.dt
        self.current_time = h.helicsFederateRequestTime(self.fed, t_next)

        self.controller_on = (self.current_time >= self.startup_time)
        if not self.controller_on:
            action = np.zeros_like(action)

        obs = self._read_obs()
        reward_array = -obs[:, 4]
        reward = float(np.sum(reward_array))

        terminated = self.current_time >= self.sim_time
        truncated = False
        info = {"node_rewards": reward}

        healthy_msg = {}
        if h.helicsInputIsUpdated(self.sub_healthy):
            try:
                healthy_str = h.helicsInputGetString(self.sub_healthy)
                healthy_msg = eval(healthy_str) if healthy_str.strip().startswith('{') else {}
            except Exception:
                logging.warning("Failed to parse healthy breakpoint input")

        adaptive = {}
        for n, (dp, dq) in zip(self.node_names, action):
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

        h.helicsPublicationPublishString(self.pub, str(adaptive))
        return obs, reward, terminated, truncated, info

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
            psik = ((vk - vkm1) - (self.m * self.dt / 2 - 1) * psi_old) / (1 + self.m * self.dt / 2)
            self.psi_prev[n] = psik
            epsk = self.gain * psik**2 if self.current_time > self.startup_time else 0.0
            self.epsilon_history[n].append(epsk)
            yk = float(np.mean(self.epsilon_history[n]))

            obs_list.append([vk, vkm1, psi_old, epsk, yk])

        return np.array(obs_list, dtype=np.float64)

    def close(self):
        self._teardown()
