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

# Federate modules: each of these contains a `run_*_federate` function
# that runs a HELICS federate in its own thread.
from federates import (
    opendss_federate,
    voltage_consumer_federate,
    attack_federate,
    inverter_federate,
    logger_federate
)

# Configure Python’s logging to show DEBUG‐level messages, including thread names
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)8s [%(threadName)s] %(message)s',
    datefmt='%H:%M:%S'
)

# ─── Load and preprocess CSV data ────────────────────────────────────────────

# Load solar data and rename columns: remove “_pv” suffix and force lowercase 's'
test_solar_data = pd.read_csv(f"{config.DATA_DIR}/solar_data.csv")
test_solar_data.columns = (
    test_solar_data.columns
        .str.replace('_pv$', '', regex=True)
        .str.replace('S', 's')
)
# Add a 'time' column (index serves as time step)
test_solar_data['time'] = test_solar_data.index

# Load load data similarly, rename, and sort by time
test_load_data = pd.read_csv(f"{config.DATA_DIR}/load_data.csv")
test_load_data.columns = test_load_data.columns.str.replace('S', 's')
test_load_data['time'] = test_load_data.index
test_load_data.sort_values('time', inplace=True)

# Load voltage‐violation breakpoints data and rename columns
breaking_points = pd.read_csv(f"{config.DATA_DIR}/solar_VV_breakpoints.csv")
breaking_points.columns = (
    breaking_points.columns
        .str.replace('_pv$', '', regex=True)
        .str.replace('S', 's')
)

# Compute maximum solar production per node (drop time column first)
max_solar = test_solar_data.drop(columns='time').max()
sbar_df = pd.DataFrame([max_solar])
# Ensure consistent column naming
sbar_df.columns = sbar_df.columns.str.replace('S', 's')

# Retrieve list of “hacks” (attack strategies) and node names from config
hacks_list = config.hacks_list
# Node names are all solar columns except 'time'
node_names = [c for c in test_solar_data.columns if c != 'time']


class DRLControllerEnv(gym.Env):
    """
    A Gymnasium environment that wraps a HELICS-based distribution‐system simulation.
    The environment launches multiple HELICS federates (OpenDSS, voltage consumer, attack, inverter, logger)
    in separate threads, creates a controller federate to adjust breakpoints dynamically,
    and steps through the HELICS time loop one dt at a time.
    """

    #metadata = {"render_modes": []}

    def __init__(self, simulation_time, time_step, for_eval=False):
        """
        :param simulation_time: total simulation horizon (seconds)
        :param time_step: time increment (seconds)
        :param for_eval: if True, do not teardown HELICS on reset (used for evaluation-only env)
        """
        super().__init__()

        # Simulation parameters
        self.sim_time = simulation_time
        self.dt = time_step
        self.node_names = node_names
        self.n_nodes = len(self.node_names)
        self.for_eval = for_eval  # If True, skip full teardown on reset

        # Action space: for each node, two continuous values (dp, dq)
        self.action_space = spaces.Box(
            low=-1.0, high=1.0, shape=(self.n_nodes, 2), dtype=np.float64
        )
        # Observation space: for each node, a vector of 5 floats
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(self.n_nodes, 5), dtype=np.float64
        )

        # Controller parameters
        self.startup_time = 50         # time (seconds) before controller activates
        self.gain = 1e8                # high‐pass filter gain
        self.m = 1                     # HPF coefficient

        # Per-node historical states for voltage processing
        self.v_hist = {n: deque(maxlen=2) for n in self.node_names}
        self.psi_prev = {n: 0.0 for n in self.node_names}
        self.epsilon_history = {n: deque(maxlen=10) for n in self.node_names}

        # HELICS handles initialized to None
        self.controller_on = True
        self.broker = None
        self.threads = []     # will hold threading.Thread objects for each federate
        self.fed = None       # HELICS ValueFederate handle for the controller
        self.sub_voltage = None
        self.sub_healthy = None
        self.sub_flag = None
        self.pub = None
        self.current_time = 0.0

        # Locks to prevent concurrent teardown/reset calls
        self.teardown_lock = threading.Lock()
        self.reset_lock = threading.Lock()

    def _thread_wrapper(self, fn, args):
        """
        Wrap the federate launch function so that any exception inside the thread is logged,
        preventing a silent thread crash.
        """
        def wrapped():
            try:
                fn(*args)
            except Exception as e:
                logging.exception(f"Error in {fn.__name__}: {e}")
        return wrapped

    def _teardown(self):
        """
        Safely finalize and free HELICS federates and the broker, and join any live threads.
        Called before starting a fresh simulation.
        """
        with self.teardown_lock:
            logging.debug("Tearing down simulation...")
            # 1) Finalize and disconnect controller federate if it exists
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

            # 2) Join all federate threads (with a timeout)
            for i, t in enumerate(self.threads):
                if t.is_alive():
                    logging.debug(f"Joining thread {i}...")
                    t.join(timeout=5.0)
            self.threads = []  # reset thread list

            # 3) Disconnect and free the broker if it exists
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
        Reset the environment and start a fresh HELICS simulation.
        Returns (observation, info) as required by Gymnasium.
        """
        with self.reset_lock:
            logging.debug("Reset called.")
            # If not in evaluation mode, tear down any existing HELICS simulation
            if not self.for_eval:
                self._teardown()
                time.sleep(5)  # allow ports to be released

            # 1) Create a new HELICS broker with auto‐assigned port
            logging.debug("Creating HELICS broker (auto port)...")
            self.broker = h.helicsCreateBroker(
                'zmq', '', f'--federates=6 --loglevel=warning --autobroker'
            )
            logging.debug(f"Broker created. Connected: {h.helicsBrokerIsConnected(self.broker)}")

            # 2) Launch each federate in its own daemon thread
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
                time.sleep(0.5)  # small stagger to avoid startup contention

            # 3) Create and configure the controller federate
            logging.debug("Creating and initializing controller federate...")
            fi = h.helicsCreateFederateInfo()
            core_name = f"core_{int(time.time())}"  # unique core name per connection
            h.helicsFederateInfoSetCoreName(fi, core_name)
            h.helicsFederateInfoSetCoreTypeFromString(fi, 'zmq')
            h.helicsFederateInfoSetTimeProperty(fi, h.HELICS_PROPERTY_TIME_DELTA, self.dt)
            self.fed = h.helicsCreateValueFederate('Adaptive_Controller_Federate', fi)

            # Register subscriptions and publications for the controller federate
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

            # Enter executing mode and request the initial time (0.0)
            h.helicsFederateEnterExecutingMode(self.fed)
            self.current_time = h.helicsFederateRequestTime(self.fed, 0.0)

            # 4) Clear any historical controller state
            for n in self.node_names:
                self.v_hist[n].clear()
                self.psi_prev[n] = 0.0
                self.epsilon_history[n].clear()

            # 5) Fetch the initial observation from HELICS
            obs = self._read_obs()
            return obs, {}  # Gymnasium expects (obs, info)

    def step(self, action):
        """
        Advance the HELICS simulation by one time step:
        1) Request time advancement from HELICS
        2) Optionally disable controller until startup time
        3) Read new observations (voltages)
        4) Compute reward based on epsilon history
        5) Publish new adaptive breakpoints for next time step
        Returns (obs, reward, terminated, truncated, info).
        """
        # 1) Advance HELICS time by dt
        t_next = self.current_time + self.dt
        self.current_time = h.helicsFederateRequestTime(self.fed, t_next)

        # 2) Only apply controller adjustments after startup phase
        self.controller_on = (self.current_time >= self.startup_time)
        if not self.controller_on:
            action = np.zeros_like(action)

        # 3) Read observations from the voltage subscription
        obs = self._read_obs()

        # 4) Compute reward: negative of the average “y_k” metric across nodes
        reward_array = -obs[:, 4]  # obs[:,4] is y_k for each node
        reward = float(np.sum(reward_array))

        # Termination condition: when current_time >= simulation horizon
        terminated = self.current_time >= self.sim_time
        truncated = False
        info = {"node_rewards": reward}

        # 5) Fetch “healthy breakpoints” input if updated
        healthy_msg = {}
        if h.helicsInputIsUpdated(self.sub_healthy):
            try:
                healthy_str = h.helicsInputGetString(self.sub_healthy)
                healthy_msg = eval(healthy_str) if healthy_str.strip().startswith('{') else {}
            except Exception:
                logging.warning("Failed to parse healthy breakpoint input")

        # 6) Build new adaptive breakpoints (dp, dq adjustments) per node
        adaptive = {}
        for n, (dp, dq) in zip(self.node_names, action):
            segments = healthy_msg.get(
                n,
                [{'pct': 1.0, 'bp': [0.98, 1.01, 1.02, 1.05, 1.07]}]
            )
            bp = segments[0]['bp']

            if self.controller_on:
                # Adjust breakpoints by dp, dq after startup
                new_bp = [
                    type(bp[0])(bp[0] - dq),
                    type(bp[1])(bp[1] - dq),
                    type(bp[2])(bp[2] - dq),
                    type(bp[3])(bp[3] - dp),
                    type(bp[4])(bp[4] - dp)
                ]
            else:
                # Keep breakpoints unchanged during startup
                new_bp = bp

            segments[0]['bp'] = new_bp
            adaptive[n] = segments

        # 7) Publish the adaptive breakpoints string to HELICS
        h.helicsPublicationPublishString(self.pub, str(adaptive))

        return obs, reward, terminated, truncated, info

    def _read_obs(self):
        """
        Internal helper to read voltage observations from HELICS.
        Wait up to 100 * 0.01 s for an updated message; if none arrives, default vd = {}.
        Returns an (n_nodes × 5) NumPy array:
          [v_k, v_{k-1}, psi_{k-1}, eps_k, y_k] for each node.
        """
        timeout = 0
        # Wait until the subscription has an updated value or timeout
        while not h.helicsInputIsUpdated(self.sub_voltage) and timeout < 100:
            time.sleep(0.01)
            timeout += 1

        # Retrieve the voltage string (dictionary of voltages per node)
        try:
            vs = h.helicsInputGetString(self.sub_voltage)
            vd = eval(vs) if vs.strip().startswith('{') else {}
        except Exception:
            vd = {}

        obs_list = []
        for n in self.node_names:
            # Some nodes are named like 's701a'; alt = '701a' in that case
            alt = n[1:] if n.startswith('s') else n
            vk = vd.get(n, vd.get(alt, 1.0))  # default voltage = 1.0 if missing

            hist = self.v_hist[n]
            hist.append(vk)
            if len(hist) < 2:
                # Not enough history yet: return zeros for psi, eps, y
                obs_list.append([vk, vk, 0.0, 0.0, 0.0])
                continue

            # Compute high‐pass filter psi_k
            vkm1 = hist[-2]
            psi_old = self.psi_prev[n]
            psik = ((vk - vkm1) - (self.m * self.dt / 2 - 1) * psi_old) \
                   / (1 + self.m * self.dt / 2)
            self.psi_prev[n] = psik

            # Compute epsilon only after startup_time; otherwise 0
            epsk = self.gain * psik**2 if self.current_time > self.startup_time else 0.0
            self.epsilon_history[n].append(epsk)

            # y_k is the running mean of the last few epsilons
            yk = float(np.mean(self.epsilon_history[n]))
            obs_list.append([vk, vkm1, psi_old, epsk, yk])

        return np.array(obs_list, dtype=np.float64)

    def close(self):
        """
        Close the environment by tearing down the HELICS simulation.
        Ensures federates are finalized and the broker is freed.
        """
        self._teardown()
