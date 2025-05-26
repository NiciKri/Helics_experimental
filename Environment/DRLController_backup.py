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
test_solar_data = pd.read_csv(f"{config.DATA_DIR}/solar_data.csv").assign(time=lambda df: df.index)
test_load_data = (
    pd.read_csv(f"{config.DATA_DIR}/load_data.csv")
    .assign(time=lambda df: df.index)
    .sort_values('time')
)
breaking_points = pd.read_csv(f"{config.DATA_DIR}/solar_VV_breakpoints.csv")
# Clean up column names
breaking_points.columns = breaking_points.columns.str.replace('_pv$', '', regex=True)
breaking_points.columns = breaking_points.columns.str.replace('S', 's')
# max solar and sbar
max_solar = test_solar_data.drop(columns='time').max()
sbar_df = pd.DataFrame([max_solar])
sbar_df.columns = sbar_df.columns.str.replace('S', 's')
# hacks list and node names
hacks_list = config.hacks_list
# normalize node names to match breaking_points columns (Option A)
node_names = [
    c.replace('_pv', '').lower()
    for c in test_solar_data.columns
    if c != 'time'
]

class DRLControllerEnv(gym.Env):
    """Gym wrapper around HELICS co-simulation with a DRL-based controller federate."""
    metadata = {'render.modes': []}

    def __init__(self, simulation_time, time_step):
        super().__init__()
        # simulation parameters
        self.sim_time = simulation_time
        self.dt = time_step

        # nodes
        self.node_names = node_names
        self.n_nodes = len(self.node_names)

        # Action: one (Δp, Δq) per node
        max_delta = 1.0
        self.action_space = spaces.Box(
            low=-max_delta,
            high=+max_delta,
            shape=(self.n_nodes, 2),
            dtype=np.float32
        )

        # Observation: [vk, v(k-1), psi, epsilon, y] for each node
        self.observation_space = spaces.Box(
            low=-np.inf,
            high=+np.inf,
            shape=(self.n_nodes, 5),
            dtype=np.float32
        )

        # controller parameters
        self.startup_time = 50
        self.gain = 1e8
        self.m = 1  # high-pass filter

        # per-node state
        self.v_hist = {n: deque(maxlen=2) for n in self.node_names}
        self.psi_prev = {n: 0.0 for n in self.node_names}
        self.epsilon_history = {n: deque(maxlen=10) for n in self.node_names}

        # HELICS attributes
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
            except Exception:
                print(f"Error in {fn.__name__}")
        return wrapped

    def _teardown(self):
        logging.debug("ENTER teardown_simulation")
        if self.fed:
            h.helicsFederateDisconnect(self.fed)
            h.helicsFederateFinalize(self.fed)
            self.fed = None
        if self.broker and h.helicsBrokerIsConnected(self.broker):
            h.helicsBrokerDisconnect(self.broker)
            h.helicsBrokerFree(self.broker)
            self.broker = None
            logging.debug("broker freed")
        for t in self.threads:
            t.join(timeout=5.0)
        self.threads = []
        logging.debug("EXIT teardown_simulation")

    def reset(self):
        # teardown if needed
        if self.broker:
            self._teardown()

        # start broker
        self.broker = h.helicsCreateBroker('zmq', '', f'--federates=6 --loglevel=warning')
        logging.debug(f"Broker handle: {self.broker}")
        time.sleep(1)

        # launch non-controller federates
        funcs = [
            (voltage_consumer_federate.run_voltage_consumer_federate,
             (test_solar_data, test_load_data, self.node_names, self.sim_time, self.dt)),
            (opendss_federate.run_opendss_federate, ()),
            (attack_federate.run_attack_federate,
             (hacks_list, breaking_points, self.sim_time, self.dt)),
            (inverter_federate.run_inverter_federate,
             (self.node_names, self.sim_time, self.dt, breaking_points, sbar_df)),
            (logger_federate.run_logging_federate,
             (self.sim_time, self.dt)),
        ]
        for fn, args in funcs:
            t = threading.Thread(target=self._thread_wrapper(fn, args), daemon=True)
            t.start()
            self.threads.append(t)
            time.sleep(0.5)

        # create controller federate
        fi = h.helicsCreateFederateInfo()
        h.helicsFederateInfoSetCoreName(fi, 'Adaptive_Controller_Federate')
        h.helicsFederateInfoSetCoreTypeFromString(fi, 'zmq')
        h.helicsFederateInfoSetTimeProperty(fi, h.HELICS_PROPERTY_TIME_DELTA, self.dt)
        self.fed = h.helicsCreateValueFederate('Adaptive_Controller_Federate', fi)
        self.sub_voltage = h.helicsFederateRegisterSubscription(self.fed, 'OpenDSS_Federate/voltage_out', '')
        self.sub_healthy = h.helicsFederateRegisterSubscription(self.fed, 'Attack_Federate/healthy_breakpoints', '')
        self.sub_flag = h.helicsFederateRegisterSubscription(self.fed, 'Attack_Federate/attack_flag', '')
        self.pub = h.helicsFederateRegisterPublication(self.fed, 'adaptive_breakpoints', h.HELICS_DATA_TYPE_STRING, '')
        h.helicsFederateEnterExecutingMode(self.fed)
        self.current_time = h.helicsFederateRequestTime(self.fed, 0.0)

        # clear per-node state
        for n in self.node_names:
            self.v_hist[n].clear()
            self.psi_prev[n] = 0.0
            self.epsilon_history[n].clear()

        return self._read_obs()

    def step(self, action):
        # 1) fetch the latest healthy breakpoints from the Attack Federate
        if h.helicsInputIsUpdated(self.sub_healthy):
            healthy_vs = h.helicsInputGetString(self.sub_healthy)
            # expect a dict-like string: { 'node1': [ {pct:…, bp:[…]}, … ], … }
            healthy_msg = eval(healthy_vs) if healthy_vs.strip().startswith('{') else {}
        else:
            healthy_msg = {}

        # 2) build adaptive breakpoints per node based on healthy segments + DRL offsets
        adaptive = {}
        for n, (dp, dq) in zip(self.node_names, action):
            # pull whatever segments Attack Federate just published, or default
            if n in healthy_msg:
                segments = healthy_msg[n]
            else:
                # fallback to your original breaking_points CSV
                if n in breaking_points.columns:
                    default_bp = breaking_points[n].dropna().tolist()
                else:
                    default_bp = [0.98, 1.01, 1.02, 1.05, 1.07]
                segments = [{'pct': 1.0, 'bp': default_bp}]

            # apply only your DRL agent’s (dp,dq) to the healthy segment
            bp = segments[0]['bp']
            if self.controller_on:
                new_bp = [
                    bp[0] - dq,
                    bp[1] - dq,
                    bp[2] - dq,
                    bp[3] + dp,
                    bp[4] + dp
                ]
            # if controller_on is False, don't adjust breakpoints
            elif not self.controller_on:
                new_bp = [
                    bp[0],
                    bp[1],
                    bp[2],
                    bp[3],
                    bp[4]
                ]

            segments[0]['bp'] = new_bp

            adaptive[n] = segments

        # 3) publish the adjusted breakpoints back to the Inverter Federate
        h.helicsPublicationPublishString(self.pub, str(adaptive))

        # 4) advance time in HELICS
        t_next = self.current_time + self.dt
        self.current_time = h.helicsFederateRequestTime(self.fed, t_next)

        # 5) observe & reward
        obs = self._read_obs()  # shape (n_nodes,5)
        reward = -float(np.sum(obs[:, 4]))        # minimize “energy” metric
        done   = self.current_time >= self.sim_time
        logging.debug(f"Step done: time={self.current_time}, reward={reward}, done={done}")
        return obs, reward, done, {}


    def _read_obs(self):
        # wait for voltage update
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
            vk = vd.get(n, 1.0)
            hist = self.v_hist[n]
            hist.append(vk)

            if len(hist) < 2:
                obs_list.append([vk, vk, 0.0, 0.0, 0.0])
                continue

            vkm1 = hist[-2]
            psi_old = self.psi_prev[n]

            # high-pass filter and energy
            psik = ((vk - vkm1) - (self.m * self.dt / 2 - 1) * psi_old) / (1 + self.m * self.dt / 2)
            self.psi_prev[n] = psik
            epsk = self.gain * psik**2 if self.current_time > self.startup_time else 0.0
            self.epsilon_history[n].append(epsk)
            yk = float(np.mean(self.epsilon_history[n]))

            obs_list.append([vk, vkm1, psi_old, epsk, yk])

        return np.array(obs_list, dtype=np.float32)

    def close(self):
        logging.debug("CLOSE called")
        self._teardown()
