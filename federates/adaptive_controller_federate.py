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
    attack_sub = h.helicsFederateRegisterSubscription(fed, "Attack_Federate/breakpoints_attack", "")
    pub = h.helicsFederateRegisterPublication(fed, "adaptive_breakpoints", h.HELICS_DATA_TYPE_STRING, "")

    h.helicsFederateEnterExecutingMode(fed)

    # Pre-load healthy breakpoints
    healthy_breakpoints_df.columns = healthy_breakpoints_df.columns.str.strip().str.lower()
    healthy_breakpoints = {
        col: healthy_breakpoints_df[col].dropna().tolist()
        for col in healthy_breakpoints_df.columns if len(healthy_breakpoints_df[col].dropna()) >= 5
    }

    # Controller parameters
    delay_timer = 5
    threshold = 0.002
    threshold = 0.05
    startup_time = 50
    adaptive_gain = 1000
    delta_t = 1
    high_pass_filter = 1
    gain = 1e8
    sliding_window = 50
    

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

    y_history = {node.lower(): [] for node in node_names}

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

        # Receive attack data
        attack_data = {}
        attack_timeout = 0
        while not h.helicsInputIsUpdated(attack_sub) and attack_timeout < 100:
            time.sleep(0.01)
            attack_timeout += 1
        try:
            ao = h.helicsInputGetString(attack_sub)
            attack_data = eval(ao) if ao.strip().startswith('{') else {}
        except:
            attack_data = {}

        adaptive_breakpoints = {}

        for node in node_names:
            key = node.lower()

            v_k = voltage_data.get(key, voltage_data.get(key[1:] if key.startswith('s') else key, 1.0))
            controller_state[key]['v_hist'].append(v_k)

            state = controller_state[key]
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
                epsilonk = gain * (psik ** 2)
                yk = epsilonk
                for n in range(sliding_window-1):
                    yk += y_history[key][-n] if len(y_history[key]) > n else 0.0
                yk /= sliding_window

                # Store intermediate states
                state['psi'][tc] = psik
                state['epsilon'][tc] = epsilonk
                state['y'][tc] = yk

                # Zero out during startup
                if current_time < startup_time:
                    yk = 0.0
                    state['psi'][tc] = 0.0
                    state['epsilon'][tc] = 0.0
                    state['y'][tc] = 0.0

                # Save y for plotting
                y_history[key].append(yk)

                # Delay-based update every delay_timer steps
                if (delay_timer != 0 and tc + 1 == delay_timer) or delay_timer == 0:
                    vk = state['psi'][tc]
                    vkmdelay = state['psi'][0]
                    up_old = state['up'][0]
                    uq_old = state['uq'][0]

                    # Compute new control offsets
                    state['up'][1] = adaptive_control(adaptive_gain, vk, vkmdelay, up_old, threshold, yk, current_time, startup_time)
                    state['uq'][1] = adaptive_control(adaptive_gain, vk, vkmdelay, uq_old, threshold, yk, current_time, startup_time)
                                                     

                    # Shift old values
                    state['up'][0] = state['up'][1]
                    state['uq'][0] = state['uq'][1]

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

                            # Print the breakpoint shift
                            if key == "s701a":
                                print(f"[Adaptive Controller] Node {key}: Δq={state['uq'][1]:.6f}, Δp={state['up'][1]:.6f}, new breakpoints={new_bps.tolist()}")

                    # Reset counter
                    state['time_counter'] = 0
                else:
                    state['time_counter'] += 1

            adaptive_breakpoints[key] = new_segments

        h.helicsPublicationPublishString(pub, str(adaptive_breakpoints))

        # Advance time
        next_time = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_time)

    # Disconnect and finalize
    h.helicsFederateDisconnect(fed)
    h.helicsFederateFinalize(fed)
    print("[Adaptive Controller Federate] Finalized.")

    # Save y_history for later plotting
    y_output_dir = os.path.join(config.BASE_DIR, "outputs")
    os.makedirs(y_output_dir, exist_ok=True)
    for key, y_vals in y_history.items():
        np.save(os.path.join(y_output_dir, f"y_values_{key}.npy"), np.array(y_vals))


def adaptive_control(adaptive_gain, vk, vkmdelay, ukmdelay, threshold, yk, current_time, startup_time=50):
    if yk > threshold and current_time > startup_time:
        return 0.5 * adaptive_gain * (vk**2 + vkmdelay**2) + ukmdelay
    else:
        return ukmdelay
