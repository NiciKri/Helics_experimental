import threading
import time
import helics as h
import numpy as np
import gym
from gym import spaces
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

# configure logging for debug
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)8s [%(threadName)s] %(message)s',
    datefmt='%H:%M:%S'
)

# Load scenario data once
solar_data = pd.read_csv(f"{config.DATA_DIR}/solar_data.csv").assign(time=lambda df: df.index)
load_data = (
    pd.read_csv(f"{config.DATA_DIR}/load_data.csv")
    .assign(time=lambda df: df.index)
    .sort_values('time')
)
breaking_points = pd.read_csv(f"{config.DATA_DIR}/solar_VV_breakpoints.csv")
# Clean up column names
breaking_points.columns = breaking_points.columns.str.replace('_pv$', '', regex=True)
breaking_points.columns = breaking_points.columns.str.replace('S', 's')
# max solar and sbar
max_solar = solar_data.drop(columns='time').max()
sbar_df = pd.DataFrame([max_solar])
sbar_df.columns = sbar_df.columns.str.replace('S', 's')
# hacks list and node names
hacks_list = config.hacks_list
node_names = [c for c in solar_data.columns if c != 'time']

class DRLControllerEnv(gym.Env):
    """Gym wrapper around HELICS co-simulation with a DRL-based controller federate."""
    metadata = {'render.modes': []}

    def __init__(self, simulation_time, time_step):
        super().__init__()
        # OBS: [vk, vkm1, psi_prev, epsilonk, yk]
        self.observation_space = spaces.Box(-np.inf, np.inf, shape=(5,), dtype=np.float32)
        # ACT: [Δp, Δq]
        max_delta = 1.0
        self.action_space = spaces.Box(
            low=np.array([-max_delta, -max_delta], np.float32),
            high=np.array([+max_delta, +max_delta], np.float32),
            dtype=np.float32
        )
        self.sim_time = simulation_time
        self.dt = time_step

        # internal state
        self.v_hist = deque(maxlen=2)
        self.psi_prev = 0.0
        self.epsilon_history = deque(maxlen=10)
        self.startup_time = 50
        self.gain = 1e8
        self.m = 1  # high-pass filter
        self.current_time = 0.0

        # HELICS attributes
        self.broker = None
        self.threads = []
        self.fed = None
        self.sub_voltage = None
        self.sub_healthy = None
        self.sub_flag = None
        self.pub = None

    def _thread_wrapper(self, fn, args):
        """Wrap federate functions to log entry/exit."""
        def wrapped():
            #logging.debug(f"FEDERATE START {fn.__name__}")
            try:
                fn(*args)
            except:
                print(f"Error in {fn.__name__}")
            #finally:
                #logging.debug(f"FEDERATE EXIT  {fn.__name__}")
        return wrapped

    def _teardown(self):
        logging.debug("ENTER teardown_simulation")
        # finalize controller
        if getattr(self, 'fed', None):
            #logging.debug("disconnecting controller federate")
            h.helicsFederateDisconnect(self.fed)
            h.helicsFederateFinalize(self.fed)
            self.fed = None
            #logging.debug("controller federate finalized")
        # kill old broker
        if getattr(self, 'broker', None) and h.helicsBrokerIsConnected(self.broker):
            #logging.debug("disconnecting broker")
            h.helicsBrokerDisconnect(self.broker)
            h.helicsBrokerFree(self.broker)
            self.broker = None
            logging.debug("broker freed")
        # join threads
        for t in getattr(self, 'threads', []):
            #logging.debug(f"joining thread {t.name}")
            t.join(timeout=5.0)
            #logging.debug(f"thread {t.name} joined")
        self.threads = []
        logging.debug("EXIT teardown_simulation")

    def reset(self):
        #logging.debug("=== RESET start ===")
        # Tear down previous simulation
        if getattr(self, "broker", None):
            #logging.debug("Existing broker detected; tearing down")
            self._teardown()

        # -- Start broker --
        #logging.debug("Creating new HELICS broker")
        self.broker = h.helicsCreateBroker('zmq', '', f'--federates=6 --loglevel=warning')
        logging.debug(f"Broker handle: {self.broker}")
        time.sleep(1)

        # -- Launch non-controller federates --
        funcs = [
            (voltage_consumer_federate.run_voltage_consumer_federate,
             (solar_data, load_data, node_names, self.sim_time, self.dt)),
            (opendss_federate.run_opendss_federate, ()),
            (attack_federate.run_attack_federate,
             (hacks_list, breaking_points, self.sim_time, self.dt)),
            (inverter_federate.run_inverter_federate,
             (node_names, self.sim_time, self.dt, breaking_points, sbar_df)),
            (logger_federate.run_logging_federate,
             (self.sim_time, self.dt)),
        ]
        for fn, args in funcs:
            t = threading.Thread(
                target=self._thread_wrapper(fn, args),
                name=fn.__name__,
                daemon=True
            )
            #logging.debug(f"Starting thread {fn.__name__}")
            t.start()
            self.threads.append(t)
            time.sleep(0.5)

        # -- Create controller federate --
        #logging.debug("Creating controller federate")
        fi = h.helicsCreateFederateInfo()
        h.helicsFederateInfoSetCoreName(fi, 'Adaptive_Controller_Federate')
        h.helicsFederateInfoSetCoreTypeFromString(fi, 'zmq')
        h.helicsFederateInfoSetTimeProperty(fi, h.HELICS_PROPERTY_TIME_DELTA, self.dt)
        self.fed = h.helicsCreateValueFederate('Adaptive_Controller_Federate', fi)
        self.sub_voltage = h.helicsFederateRegisterSubscription(
            self.fed, 'OpenDSS_Federate/voltage_out', '')
        self.sub_healthy = h.helicsFederateRegisterSubscription(
            self.fed, 'Attack_Federate/healthy_breakpoints', '')
        self.sub_flag = h.helicsFederateRegisterSubscription(
            self.fed, 'Attack_Federate/attack_flag', '')
        self.pub = h.helicsFederateRegisterPublication(
            self.fed, 'adaptive_breakpoints', h.HELICS_DATA_TYPE_STRING, '')
        h.helicsFederateEnterExecutingMode(self.fed)
        self.current_time = h.helicsFederateRequestTime(self.fed, 0.0)
        #logging.debug("Controller federate entered execution mode")

        # initial observation
        obs = self._read_obs()
        #logging.debug("=== RESET complete ===")
        return obs

    def step(self, action):
        # Publish action as breakpoints
        dp, dq = float(action[0]), float(action[1])
        adaptive = {n.lower(): [{'pct': 1.0, 'bp': [-dq, -dq, -dq, +dp, +dp]}] for n in node_names}
        #logging.debug(f"Publishing action: dp={dp}, dq={dq}")
        h.helicsPublicationPublishString(self.pub, str(adaptive))

        # time advance
        t_next = self.current_time + self.dt
        self.current_time = h.helicsFederateRequestTime(self.fed, t_next)

        obs = self._read_obs()
        reward = -float(obs[4])
        done = self.current_time >= self.sim_time
        logging.debug(f"Step done: time={self.current_time}, reward={reward}, done={done}")
        return obs, reward, done, {}

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

        key = node_names[0].lower()
        vk = vd.get(key, 1.0)
        self.v_hist.append(vk)
        if len(self.v_hist) < 2:
            return np.array([vk, vk, 0.0, 0.0, 0.0], dtype=np.float32)

        vkm1 = self.v_hist[-2]
        psikm1 = self.psi_prev
        dt, m, gain = self.dt, self.m, self.gain
        psi_k = (vk - vkm1 - (m * dt / 2 - 1) * psikm1) / (1 + m * dt / 2)
        self.psi_prev = psi_k
        eps_k = gain * psi_k**2 
        # if self.current_time > self.startup_time else 0.0
        self.epsilon_history.append(eps_k)
        yk = np.mean(self.epsilon_history) if self.epsilon_history else 0.0

        return np.array([vk, vkm1, psikm1, eps_k, yk], dtype=np.float32)

    def close(self):
        logging.debug("CLOSE called")
        self._teardown()