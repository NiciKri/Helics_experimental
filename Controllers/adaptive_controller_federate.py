import helics as h  # Import HELICS library for co-simulation functionality
import time  # Import time module for sleeping/pause functionality
import numpy as np  # Import NumPy for numerical operations and array handling
from collections import deque  # Import deque for fixed-length history buffers
import config  # Import configuration parameters (e.g., TIME_STEP, SIMULATION_TIME, BASE_DIR)
import os  # Import os module for file and directory operations

def run_adaptive_controller_federate(healthy_breakpoints_df, node_names, simulation_time, time_step=1.0):
    """
    Create and run an Adaptive Controller Federate that:
    - Subscribes to voltage output from the OpenDSS Federate.
    - Subscribes to “healthy” breakpoint segments and attack flag from the Attack Federate.
    - Computes adaptive breakpoint adjustments based on a high-pass filter of voltage measurements.
    - Publishes updated breakpoint segments back to the Attack Federate.
    - Saves time series of internal controller variables (ε, y, up, uq) for selected nodes.

    Parameters:
    - healthy_breakpoints_df: pandas DataFrame of “healthy” breakpoint values per node (columns = nodes, rows = breakpoint points)
    - node_names: List of node names (strings) to run the controller on (e.g., ['s701a', 's702a', ...])
    - simulation_time: Total simulation duration (in same units as TIME_STEP)
    - time_step: Time increment for HELICS time requests (default: 1.0)
    """

    # --- HELICS Federate setup ---
    fedinfo = h.helicsCreateFederateInfo()  # Create a FederateInfo object to configure the HELICS core
    h.helicsFederateInfoSetCoreName(fedinfo, "Adaptive_Controller_Federate")  # Name this federate
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")  # Use ZeroMQ for message passing
    # Set the federate’s time delta to control how often it advances simulation time
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, time_step)

    # Create a value federate with the above configuration
    fed = h.helicsCreateValueFederate("Adaptive_Controller_Federate", fedinfo)
    # Subscribe to voltage output published by the OpenDSS Federate
    voltage_sub = h.helicsFederateRegisterSubscription(fed, "OpenDSS_Federate/voltage_out", "")
    # Subscribe to the “healthy_breakpoints” published by the Attack Federate
    healthy_sub = h.helicsFederateRegisterSubscription(fed, "Attack_Federate/healthy_breakpoints", "")
    # Subscribe to the “attack_flag” boolean published by the Attack Federate
    attack_flag_sub = h.helicsFederateRegisterSubscription(fed, "Attack_Federate/attack_flag", "")
    # Create a publication to send new breakpoint segments to the Attack Federate
    pub = h.helicsFederateRegisterPublication(fed, "adaptive_breakpoints", h.HELICS_DATA_TYPE_STRING, "")

    # Enter executing mode to begin HELICS time-advancement loop
    h.helicsFederateEnterExecutingMode(fed)

    # --- Pre-load “healthy” breakpoint segments into a dictionary ---
    # Standardize column names: strip whitespace and convert to lowercase
    healthy_breakpoints_df.columns = healthy_breakpoints_df.columns.str.strip().str.lower()
    # Build a dictionary mapping each node to its list of healthy breakpoint values,
    # but only if there are at least 5 non-NaN breakpoint points for that node.
    healthy_breakpoints = {
        col: healthy_breakpoints_df[col].dropna().tolist()
        for col in healthy_breakpoints_df.columns
        if len(healthy_breakpoints_df[col].dropna()) >= 5
    }

    # --- Controller parameters ---
    delay_timer = 1       # Delay length (number of time steps)
    if config.adaptive_controller_on:
        threshold = 0.5
    else:
        threshold = 1e6
    startup_time = 50     # Time (in same units as time_step) before adaptive law kicks in
    adaptive_gain = 500   # Gain parameter for adaptive breakpoint computation
    delta_t = 1           # Time step used in high-pass filter (matches time_step)
    high_pass_filter = 1  # High-pass filter coefficient
    gain = 1e8            # Scaling factor for energy term (ε)
    sliding_window = 10   # Window size for computing y (average of recent ε values)

    save_nodes = ['s701a']  # List of nodes for which to save ε, y, up, uq histories

    # --- Initialize controller state for each node ---
    # Each node’s state contains:
    # - v_hist: last two voltage measurements (deque of length 2)
    # - psi: array of length (delay_timer + 1) for high-pass filter state
    # - epsilon: array of length (delay_timer + 1) for energy term
    # - y: array of length (delay_timer + 1) for running average of ε
    # - up, uq: arrays of length 2 to store previous (index 0) and current (index 1) control offsets
    # - time_counter: integer counter for delay indexing
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

    # Histories for saving controller variables for each node (empty lists to append to)
    epsilon_history = {node.lower(): [] for node in node_names}
    y_history = {node.lower(): [] for node in node_names}
    up_history = {node.lower(): [] for node in node_names}
    uq_history = {node.lower(): [] for node in node_names}

    current_time = 0.0  # Initialize simulation time

    # --- Main HELICS time-advancement loop ---
    while current_time < simulation_time:
        # -------- Retrieve voltage data from OpenDSS Federate --------
        voltage_data = {}
        timeout = 0
        # Wait until voltage subscription is updated (or timeout after ~1 second)
        while not h.helicsInputIsUpdated(voltage_sub) and timeout < 100:
            time.sleep(0.01)
            timeout += 1
        try:
            vs = h.helicsInputGetString(voltage_sub)  # Voltage string, expected to be dict literal
            voltage_data = eval(vs) if vs.strip().startswith('{') else {}
        except:
            voltage_data = {}

        # -------- Retrieve attack_flag (boolean) from Attack Federate --------
        flag_timeout = 0
        while not h.helicsInputIsUpdated(attack_flag_sub) and flag_timeout < 100:
            time.sleep(0.01)
            flag_timeout += 1
        try:
            attack_flag = h.helicsInputGetBoolean(attack_flag_sub)
        except:
            attack_flag = False

        # -------- Retrieve “healthy_breakpoints” from Attack Federate --------
        attack_data = {}
        attack_timeout = 0
        while not h.helicsInputIsUpdated(healthy_sub) and attack_timeout < 100:
            time.sleep(0.01)
            attack_timeout += 1
        try:
            ao = h.helicsInputGetString(healthy_sub)  # String literal of dict: {node: [segment dicts], ...}
            attack_data = eval(ao) if ao.strip().startswith('{') else {}
        except:
            attack_data = {}

        adaptive_breakpoints = {}  # Dictionary to collect new breakpoint segments to publish

        # -------- Loop over each node and run adaptive control logic --------
        for node in node_names:
            key = node.lower()
            state = controller_state[key]

            # Append the latest voltage measurement to the node’s history:
            # If voltage_data lacks this node key, default to 1.0 p.u.
            v_k = voltage_data.get(key, voltage_data.get(key[1:] if key.startswith('s') else key, 1.0))
            state['v_hist'].append(v_k)

            tc = state['time_counter']  # Delay index

            # Get existing breakpoint segments for this node from attack_data,
            # or use the “healthy” breakpoints if no attack data is provided.
            segments = attack_data.get(
                key,
                [{"pct": 1.0, "bp": healthy_breakpoints.get(key, [0.98, 1.01, 1.02, 1.05, 1.07])}]
            )
            # Copy segments list so we can modify it without altering original
            new_segments = [seg.copy() for seg in segments]

            # Only proceed with adaptive computations if we have at least 2 voltage samples
            if len(state['v_hist']) >= 2:
                vk = state['v_hist'][-1]   # Current voltage
                vkm1 = state['v_hist'][-2] # Previous voltage
                psikm1 = state['psi'][tc - 1] if tc > 0 else 0  # Previous psi state

                # -------- High-pass filter to compute psi_k --------
                # psi_k = [vk - vkm1 - (α·Δt/2 - 1)·psi_{k-1}] / [1 + α·Δt/2]
                psik = (
                    vk - vkm1 - (high_pass_filter * delta_t / 2 - 1) * psikm1
                ) / (1 + high_pass_filter * delta_t / 2)
                # Compute energy term epsilon_k = gain · (psi_k)^2 after startup_time
                epsilonk = gain * (psik ** 2) if current_time > startup_time else 0
                epsilon_history[key].append(epsilonk)

                # Compute y_k as the average of the most recent sliding_window ε values
                recent_eps = epsilon_history[key][-sliding_window:]
                yk = sum(recent_eps) / len(recent_eps)
                y_history[key].append(yk)

                # Store computed states into state arrays for psi, ε, and y
                state['psi'][tc] = psik
                state['epsilon'][tc] = epsilonk
                state['y'][tc] = yk

                # -------- Delay-based update condition --------
                # If delay_timer == 0 or the counter has reached delay_timer, update control offsets
                if (delay_timer != 0 and tc + 1 == delay_timer) or delay_timer == 0:
                    vk_del = state['psi'][tc]     # Filtered difference at current time
                    vkmdelay = state['psi'][0]    # Filtered difference at delayed index
                    up_old = state['up'][0]       # Previous up offset
                    uq_old = state['uq'][0]       # Previous uq offset

                    # Compute new control offsets (up and uq) via adaptive law
                    state['up'][1] = adaptive_control(
                        adaptive_gain, vk_del, vkmdelay, up_old, threshold, yk, current_time, startup_time
                    )
                    state['uq'][1] = adaptive_control(
                        adaptive_gain, vk_del, vkmdelay, uq_old, threshold, yk, current_time, startup_time
                    )

                    # If no attack_flag is set, zero out adaptive offsets
                    if not attack_flag:
                        state['up'][1] = 0.0
                        state['uq'][1] = 0.0

                    # Record up and uq for history
                    up_history[key].append(state['up'][1])
                    uq_history[key].append(state['uq'][1])

                    # Shift new offsets into “old” positions for next iteration
                    state['up'][0] = state['up'][1]
                    state['uq'][0] = state['uq'][1]

                    # -------- Adjust breakpoint segments based on up/uq offsets --------
                    if new_segments:
                        old_bps = new_segments[0]['bp']  # List of 5 original breakpoints
                        if len(old_bps) == 5:
                            # Subtract uq from first three breakpoints, up from last two
                            new_bps = np.array([
                                old_bps[0] - state['uq'][1],
                                old_bps[1] - state['uq'][1],
                                old_bps[2] - state['uq'][1],
                                old_bps[3] - state['up'][1],
                                old_bps[4] - state['up'][1],
                            ])
                            new_segments[0]['bp'] = new_bps.tolist()
                            # Print debug info for node “s701a” (only once per time step)
                            if key == "s701a":
                                print(
                                    f"[Adaptive Controller] Node {key}: Δq={state['uq'][1]:.6f}, "
                                    f"Δp={state['up'][1]:.6f}, new breakpoints={new_bps.tolist()}"
                                )
                    state['time_counter'] = 0  # Reset counter after applying delay-based update
                else:
                    state['time_counter'] += 1  # Increment counter until delay_timer is reached

            adaptive_breakpoints[key] = new_segments  # Store updated segments for this node

        # -------- Publish the adaptive breakpoint segments as a string --------
        h.helicsPublicationPublishString(pub, str(adaptive_breakpoints))

        # Advance HELICS time to the next step
        next_time = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_time)

    # --- Disconnect and free HELICS federate resources ---
    h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
    print("[Adaptive Controller Federate] Finalized.")

    # --- Save histories for selected nodes into .npy files ---
    output_dir = os.path.join(config.BASE_DIR, "outputs")
    os.makedirs(output_dir, exist_ok=True)
    for key in save_nodes:
        # Convert each history list to a NumPy array and save
        np.save(os.path.join(output_dir, f"epsilon_values_{key}.npy"), np.array(epsilon_history[key]))
        np.save(os.path.join(output_dir, f"y_values_{key}.npy"), np.array(y_history[key]))
        np.save(os.path.join(output_dir, f"up_values_{key}.npy"), np.array(up_history[key]))
        np.save(os.path.join(output_dir, f"uq_values_{key}.npy"), np.array(uq_history[key]))


def adaptive_control(adaptive_gain, vk, vkmdelay, ukmdelay, threshold, yk, current_time, startup_time=50):
    """
    Adaptive control law that updates either up or uq offset:
    - If yk exceeds threshold AND current_time > startup_time:
        return 0.5 * adaptive_gain * (vk^2 + vkmdelay^2) + previous_offset
    - Else, retain the previous offset (ukmdelay).

    Parameters:
    - adaptive_gain: Gain constant for adaptation
    - vk: Current high-pass filter output
    - vkmdelay: Delayed high-pass filter output (past reading)
    - ukmdelay: Previous offset (either up_old or uq_old)
    - threshold: Activation threshold for adaptation
    - yk: Running average of recent ε values
    - current_time: Current simulation time
    - startup_time: Time before adaptation activates (default: 50)
    """
    if yk > threshold and current_time > startup_time:
        return 0.5 * adaptive_gain * (vk**2 + vkmdelay**2) + ukmdelay
    else:
        return ukmdelay
