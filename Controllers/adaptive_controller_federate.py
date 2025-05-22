import helics as h
import time
import numpy as np
from collections import deque
import config
import os

def run_adaptive_controller_federate(healthy_breakpoints_df, node_names, simulation_time, time_step=1.0):
    # HELICS setup
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Adaptive_Controller_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, time_step)

    fed = h.helicsCreateValueFederate("Adaptive_Controller_Federate", fedinfo)
    voltage_sub = h.helicsFederateRegisterSubscription(fed, "OpenDSS_Federate/voltage_out", "")
    #attack_sub = h.helicsFederateRegisterSubscription(fed, "Attack_Federate/breakpoints_attack", "")
    healthy_sub = h.helicsFederateRegisterSubscription(fed, "Attack_Federate/healthy_breakpoints", "")
    attack_flag_sub = h.helicsFederateRegisterSubscription(fed, "Attack_Federate/attack_flag", "")
    pub = h.helicsFederateRegisterPublication(fed, "adaptive_breakpoints", h.HELICS_DATA_TYPE_STRING, "")

    h.helicsFederateEnterExecutingMode(fed)

    # Pre-load healthy breakpoints
    healthy_breakpoints_df.columns = healthy_breakpoints_df.columns.str.strip().str.lower()
    healthy_breakpoints = {
        col: healthy_breakpoints_df[col].dropna().tolist()
        for col in healthy_breakpoints_df.columns if len(healthy_breakpoints_df[col].dropna()) >= 5
    }

    # Controller parameters
    delay_timer = 1
    threshold = 0.5
    #threshold = 1e6
    startup_time = 50
    adaptive_gain = 500
    delta_t = 1
    high_pass_filter = 1
    gain = 1e8
    sliding_window = 10

    save_nodes = ['s701a']

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

        flag_timeout = 0
        while not h.helicsInputIsUpdated(attack_flag_sub) and flag_timeout < 100:
            time.sleep(0.01)
            flag_timeout += 1
        try:
            attack_flag = h.helicsInputGetBoolean(attack_flag_sub)
        except:
            attack_flag = False

        # Receive attack data
        attack_data = {}
        attack_timeout = 0
        while not h.helicsInputIsUpdated(healthy_sub) and attack_timeout < 100:
            time.sleep(0.01)
            attack_timeout += 1
        try:
            ao = h.helicsInputGetString(healthy_sub)
            attack_data = eval(ao) if ao.strip().startswith('{') else {}
        except:
            attack_data = {}

        adaptive_breakpoints = {}

        for node in node_names:
            key = node.lower()
            state = controller_state[key]
            # Append current voltage
            v_k = voltage_data.get(key, voltage_data.get(key[1:] if key.startswith('s') else key, 1.0))
            state['v_hist'].append(v_k)
            tc = state['time_counter']

            segments = attack_data.get(key, [
                {"pct": 1.0, "bp": healthy_breakpoints.get(key, [0.98, 1.01, 1.02, 1.05, 1.07])}
            ])
            new_segments = [seg.copy() for seg in segments]

            if len(state['v_hist']) >= 2:
                vk = state['v_hist'][-1]
                vkm1 = state['v_hist'][-2]
                psikm1 = state['psi'][tc-1] if tc > 0 else 0

                # High-pass filter output and energy computation
                psik = (vk - vkm1 - (high_pass_filter * delta_t / 2 - 1) * psikm1) / (1 + high_pass_filter * delta_t / 2)
                epsilonk = gain * (psik ** 2) if current_time > startup_time else 0
                epsilon_history[key].append(epsilonk)

                recent_eps = epsilon_history[key][-sliding_window:]
                yk = sum(recent_eps) / len(recent_eps)
                y_history[key].append(yk)

                # Store intermediate states
                state['psi'][tc] = psik
                state['epsilon'][tc] = epsilonk
                state['y'][tc] = yk

                # Delay-based update
                if (delay_timer != 0 and tc + 1 == delay_timer) or delay_timer == 0:
                    vk_del = state['psi'][tc]
                    vkmdelay = state['psi'][0]
                    up_old = state['up'][0]
                    uq_old = state['uq'][0]

                    # Compute new control offsets
                    state['up'][1] = adaptive_control(adaptive_gain, vk_del, vkmdelay, up_old, threshold, yk, current_time, startup_time)
                    state['uq'][1] = adaptive_control(adaptive_gain, vk_del, vkmdelay, uq_old, threshold, yk, current_time, startup_time)

                    if not attack_flag:
                        state['up'][1] = 0.0
                        state['uq'][1] = 0.0

                    # Record up and uq
                    up_history[key].append(state['up'][1])
                    uq_history[key].append(state['uq'][1])

                    # Shift old values
                    state['up'][0] = state['up'][1]
                    state['uq'][0] = state['uq'][1]

                    # Adjust breakpoints
                    if new_segments:
                        old_bps = new_segments[0]['bp']
                        if len(old_bps) == 5:
                            new_bps = np.array([
                                old_bps[0] - state['uq'][1],
                                old_bps[1] - state['uq'][1],
                                old_bps[2] - state['uq'][1],
                                old_bps[3] - state['up'][1],
                                old_bps[4] - state['up'][1],
                            ])
                            new_segments[0]['bp'] = new_bps.tolist()
                            if key == "s701a":
                                print(f"[Adaptive Controller] Node {key}: Δq={state['uq'][1]:.6f}, Δp={state['up'][1]:.6f}, new breakpoints={new_bps.tolist()}")
                    state['time_counter'] = 0
                else:
                    state['time_counter'] += 1

            adaptive_breakpoints[key] = new_segments

        # print publication for s701a
        if "s701a" in adaptive_breakpoints:
            print(f"[Adaptive Controller] Node s701a: {adaptive_breakpoints['s701a']}")

        # Publish and advance time
        h.helicsPublicationPublishString(pub, str(adaptive_breakpoints))
        next_time = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_time)

    # Disconnect and finalize
    h.helicsFederateDisconnect(fed)
    h.helicsFederateFinalize(fed)
    print("[Adaptive Controller Federate] Finalized.")

    # Save histories
    output_dir = os.path.join(config.BASE_DIR, "outputs")
    os.makedirs(output_dir, exist_ok=True)
    for key in save_nodes:
        np.save(os.path.join(output_dir, f"epsilon_values_{key}.npy"), np.array(epsilon_history[key]))
        np.save(os.path.join(output_dir, f"y_values_{key}.npy"), np.array(y_history[key]))
        np.save(os.path.join(output_dir, f"up_values_{key}.npy"), np.array(up_history[key]))
        np.save(os.path.join(output_dir, f"uq_values_{key}.npy"), np.array(uq_history[key]))


def adaptive_control(adaptive_gain, vk, vkmdelay, ukmdelay, threshold, yk, current_time, startup_time=50):
    if yk > threshold and current_time > startup_time:
        return 0.5 * adaptive_gain * (vk**2 + vkmdelay**2) + ukmdelay
    else:
        return ukmdelay
