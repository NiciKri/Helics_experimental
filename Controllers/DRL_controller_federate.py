import helics as h
import time
import numpy as np
from collections import deque
import gym            # OpenAI Gym
import config
import os
from Environment import DRLControllerEnv

def run_DRL_controller_federate(healthy_breakpoints_df, node_names, simulation_time, time_step=1.0):
    # 1) HELICS setup
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Adaptive_Controller_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, time_step)
    fed = h.helicsCreateValueFederate("Adaptive_Controller_Federate", fedinfo)
    voltage_sub = h.helicsFederateRegisterSubscription(fed, "OpenDSS_Federate/voltage_out", "")
    healthy_sub = h.helicsFederateRegisterSubscription(fed, "Attack_Federate/healthy_breakpoints", "")
    attack_flag_sub = h.helicsFederateRegisterSubscription(fed, "Attack_Federate/attack_flag", "")
    pub = h.helicsFederateRegisterPublication(fed, "adaptive_breakpoints", h.HELICS_DATA_TYPE_STRING, "")
    h.helicsFederateEnterExecutingMode(fed)

    # 2) Pre-load healthy breakpoints
    healthy_breakpoints_df.columns = healthy_breakpoints_df.columns.str.strip().str.lower()
    healthy_breakpoints = {
        col: healthy_breakpoints_df[col].dropna().tolist()
        for col in healthy_breakpoints_df.columns if len(healthy_breakpoints_df[col].dropna()) >= 5
    }

    # 3) Controller parameters
    delay_timer = 1
    threshold = 0.5
    startup_time = 50
    adaptive_gain = 500
    delta_t = 1
    high_pass_filter = 1
    gain = 1e8
    sliding_window = 10

    save_nodes = [n.lower() for n in ['s701a', 's701b', 's701c', 's727c']]

    # Initialize controller state per node
    controller_state = {
        node.lower(): {
            'v_hist': deque(maxlen=2),
            'psi': np.zeros(delay_timer + 1),
            'epsilon': np.zeros(delay_timer + 1),
            'y': np.zeros(delay_timer + 1),
            'up': np.zeros(2),
            'uq': np.zeros(2),
            'time_counter': 0
        }
        for node in node_names
    }

    # Histories for saving
    epsilon_history = {node.lower(): [] for node in node_names}
    y_history = {node.lower(): [] for node in node_names}
    up_history = {node.lower(): [] for node in node_names}
    uq_history = {node.lower(): [] for node in node_names}

    # 4) Initialize Gym environment and agent
    env = DRLControllerEnv(simulation_time=simulation_time, time_step=time_step)  # replace with your actual Gym environment
    observation = env.reset()
    # If you have a policy/agent, initialize it here:
    # agent = YourRLAgent(...)

    episode_reward = 0.0
    episode_length = 0

    current_time = 0.0
    while current_time < simulation_time:
        # Receive voltages
        voltage_data = {}
        timeout = 0
        while not h.helicsInputIsUpdated(voltage_sub) and timeout < 100:
            time.sleep(0.01)
            timeout += 1
        try:
            vs = h.helicsInputGetString(voltage_sub)
            voltage_data = eval(vs) if vs.strip().startswith('{') else {}
        except:
            voltage_data = {}

        # Receive healthy breakpoints and attack flag
        healthy_data = {}
        hbt = 0
        while not h.helicsInputIsUpdated(healthy_sub) and hbt < 100:
            time.sleep(0.01)
            hbt += 1
        try:
            hs = h.helicsInputGetString(healthy_sub)
            healthy_data = eval(hs) if hs.strip().startswith('{') else {}
        except:
            healthy_data = {}

        flag_timeout = 0
        while not h.helicsInputIsUpdated(attack_flag_sub) and flag_timeout < 100:
            time.sleep(0.01)
            flag_timeout += 1
        try:
            attack_flag = h.helicsInputGetBoolean(attack_flag_sub)
        except:
            attack_flag = False

        adaptive_breakpoints = {}
        for node in node_names:
            key = node.lower()
            state = controller_state[key]

            # Append current voltage
            v_k = voltage_data.get(key, voltage_data.get(key[1:], 1.0))
            v_km1 = state['v_hist'][-1] if state['v_hist'] else v_k
            state['v_hist'].append(v_k)
            tc = state['time_counter']

            segments = healthy_data.get(key, [{
                "pct": 1.0,
                "bp": healthy_breakpoints.get(key, [0.98, 1.01, 1.02, 1.05, 1.07])
            }])
            new_segments = [seg.copy() for seg in segments]

            if len(state['v_hist']) >= 2:
                vk = state['v_hist'][-1]
                vkm1 = state['v_hist'][-2]
                psikm1 = state['psi'][tc - 1] if tc > 0 else 0

                # High-pass filter and energy
                psik = (vk - vkm1 - (high_pass_filter * delta_t / 2 - 1) * psikm1) \
                       / (1 + high_pass_filter * delta_t / 2)
                epsilonk = gain * psik**2 if current_time > startup_time else 0
                epsilon_history[key].append(epsilonk)

                recent_eps = epsilon_history[key][-sliding_window:]
                yk = sum(recent_eps) / len(recent_eps)
                y_history[key].append(yk)

                state['psi'][tc] = psik
                state['epsilon'][tc] = epsilonk
                state['y'][tc] = yk

                # Build Gym observation
                observation = np.array([vk, vkm1, psikm1, epsilonk, yk], dtype=np.float32)

                # Get action from Gym env or agent
                action = env.action_space.sample()
                # action = agent.select_action(observation)

                # Step environment
                new_obs, reward, done, info = env.step(action)
                episode_reward += reward
                episode_length += 1

                # Optional learning
                # agent.record(observation, action, reward, new_obs, done)
                # agent.learn_if_ready()

                # Map action to breakpoints 
                delta_p, delta_q = action  # assuming 2-vector
                old_bps = new_segments[0]['bp']
                new_bps = np.array([
                    old_bps[0] - delta_q,
                    old_bps[1] - delta_q,
                    old_bps[2] - delta_q,
                    old_bps[3] + delta_p,
                    old_bps[4] + delta_p,
                ])
                new_segments[0]['bp'] = new_bps.tolist()

                if done:
                    print(f"[Adaptive Controller] Episode done: reward={episode_reward}, length={episode_length}")
                    observation = env.reset()
                    episode_reward = 0
                    episode_length = 0
                else:
                    observation = new_obs

                state['time_counter'] = 0
            else:
                state['time_counter'] += 1

            adaptive_breakpoints[key] = new_segments

        # Publish and advance time
        h.helicsPublicationPublishString(pub, str(adaptive_breakpoints))
        next_time = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_time)

    # Cleanup
    env.close()
    h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
    print("[Adaptive Controller Federate] Finalized.")

    # Save histories
    output_dir = os.path.join(config.BASE_DIR, "outputs")
    os.makedirs(output_dir, exist_ok=True)
    for key in save_nodes:
        np.save(os.path.join(output_dir, f"epsilon_values_{key}.npy"), np.array(epsilon_history[key]))
        np.save(os.path.join(output_dir, f"y_values_{key}.npy"), np.array(y_history[key]))
        np.save(os.path.join(output_dir, f"up_values_{key}.npy"), np.array(up_history[key]))
        np.save(os.path.join(output_dir, f"uq_values_{key}.npy"), np.array(uq_history[key]))
