import helics as h
import time
import math
from collections import deque
#import numpy as np
import config  # Import project-specific configuration (e.g., Sbar_scaling)

# ─── Global Constants ─────────────────────────────────────────────────────────

# Default Volt-Var control breakpoints (pu voltages)
DEFAULT_CONTROL_SETTING = [0.98, 1.01, 1.02, 1.05, 1.07]

# Low-pass filter coefficients for measurement and output smoothing
LOW_PASS_FILTER_MEASURE = 1.2    # “m” coefficient for the measurement LPF
LOW_PASS_FILTER_OUTPUT = 0.1     # “o” coefficient for the output LPF

# Default apparent power rating per segment (will be scaled by config.Sbar_scaling)
S_BAR = 200.0

# Minimum solar irradiance (e.g., kW) needed to allow active injection
SOLAR_MIN_VALUE = 5.0

# Default simulation time step (seconds)
DELTA_T = 1.0


def initialize_node_state():
    """
    Create and return a dictionary holding filter states for one inverter segment:
      - 'p_set', 'q_set': raw (pre-filtered) P/Q setpoints (last two values)
      - 'p_out', 'q_out': filtered (output) P/Q values (last two values)
      - 'lpf_v': filtered voltage history (last two values, initial 1.0 pu)
    """
    return {
        'p_set': deque([0, 0], maxlen=2),
        'q_set': deque([0, 0], maxlen=2),
        'p_out': deque([0, 0], maxlen=2),
        'q_out': deque([0, 0], maxlen=2),
        'lpf_v': deque([1.0, 1.0], maxlen=2)
    }


def calculate_injection_for_node(state,
                                 current_time,
                                 measured_voltage,
                                 measured_solar,
                                 delta_t=DELTA_T,
                                 control_setting=DEFAULT_CONTROL_SETTING,
                                 lpf_m=LOW_PASS_FILTER_MEASURE,
                                 lpf_o=LOW_PASS_FILTER_OUTPUT,
                                 Sbar=S_BAR,
                                 solar_min=SOLAR_MIN_VALUE):
    """
    Compute active (p_out) and reactive (q_out) injections for a single inverter segment,
    given:
      - state: dictionary of deques for P/Q setpoints, filtered P/Q, and filtered voltage
      - current_time: current simulation time (not used in this calculation except for reference)
      - measured_voltage (vk): instantaneous node voltage (pu)
      - measured_solar (ms): instantaneous solar irradiance (same units as Sbar)
      - delta_t: time step for filtering
      - control_setting: list of 5 Volt-Var breakpoints [v1, v2, v3, v4, v5]
      - lpf_m: coefficient used in the voltage LPF
      - lpf_o: coefficient used in the P/Q output LPF
      - Sbar: apparent power rating for this segment
      - solar_min: minimum solar threshold to allow active injection

    Logical steps:
      1) Low-pass filter the measured voltage (vk) to get a smoothed value.
      2) If measured_solar ≥ solar_min and filtered voltage ≤ v5:
           • Set raw P (pk) = measured_solar.
           • Compute available reactive (q_avail = sqrt(max(Sbar² - pk², 0))).
           • Use piecewise Volt-Var control:
             - v ≤ v1: qk = +q_avail
             - v1 < v ≤ v2: ramp down from +q_avail → 0
             - v2 < v ≤ v3: qk = 0
             - v3 < v ≤ v4: ramp down from 0 → -q_avail
             - v4 < v ≤ v5: curtail P, sink reactive up to -√(Sbar² - p²)
         Else if filtered voltage > v5: sink full reactive (qk = -Sbar).
      3) Low-pass filter the raw P/Q setpoints to produce p_out / q_out.
      4) Update state deques accordingly and return (p_out, q_out).
    """
    vk = measured_voltage
    vkm1 = state['lpf_v'][-1]  # previous filtered voltage

    # ─── 1) Low-Pass Filter the Voltage ────────────────────────────────────────
    low_pass_filter_v = (
        delta_t * lpf_m * (vk + vkm1)
        - (delta_t * lpf_m - 2) * vkm1
    ) / (2 + delta_t * lpf_m)

    # Initialize raw setpoints
    pk = 0.0
    qk = 0.0

    # ─── 2) Volt-Var Logic When Solar ≥ Minimum and Voltage ≤ v5 ──────────────
    if measured_solar >= solar_min and low_pass_filter_v <= control_setting[-1]:
        # Active injection equals available solar
        pk = measured_solar
        try:
            # Reactive capability: sqrt(Sbar^2 - pk^2), clipped to zero if negative
            q_avail = math.sqrt(max(Sbar**2 - pk**2, 0))
        except Exception:
            q_avail = 0.0

        # Piecewise definition based on filtered voltage and breakpoints:
        if low_pass_filter_v <= control_setting[0]:
            # v ≤ v1: inject maximum reactive
            qk = q_avail

        elif low_pass_filter_v <= control_setting[1]:
            # v1 < v ≤ v2: linear ramp from +q_avail → 0
            c = q_avail / (control_setting[1] - control_setting[0])
            qk = c * (control_setting[1] - low_pass_filter_v)

        elif low_pass_filter_v <= control_setting[2]:
            # v2 < v ≤ v3: unity PF (no reactive)
            qk = 0.0

        elif low_pass_filter_v <= control_setting[3]:
            # v3 < v ≤ v4: linear ramp from 0 → -q_avail (sink reactive)
            c = q_avail / (control_setting[3] - control_setting[2])
            qk = -c * (low_pass_filter_v - control_setting[2])

        else:
            # v4 < v ≤ v5: curtail active injection and sink reactive
            d = measured_solar / (control_setting[4] - control_setting[3])
            pk = d * (low_pass_filter_v - control_setting[3])
            try:
                qk = -math.sqrt(max(Sbar**2 - pk**2, 0))
            except Exception:
                qk = 0.0

    # ─── If Voltage > v5: Sink Full Reactive ───────────────────────────────────
    elif low_pass_filter_v > control_setting[-1]:
        qk = -Sbar

    # ─── 3) Low-Pass Filter Raw P/Q Setpoints → p_out / q_out ─────────────────
    state['p_set'].append(pk)    # append raw P setpoint
    state['q_set'].append(qk)    # append raw Q setpoint

    # Trapezoidal integration for P output filter
    p_out = (
        delta_t * lpf_o * (state['p_set'][-1] + state['p_set'][-2])
        - (delta_t * lpf_o - 2) * state['p_out'][-1]
    ) / (2 + delta_t * lpf_o)

    # Trapezoidal integration for Q output filter
    q_out = (
        delta_t * lpf_o * (state['q_set'][-1] + state['q_set'][-2])
        - (delta_t * lpf_o - 2) * state['q_out'][-1]
    ) / (2 + delta_t * lpf_o)

    # Update filtered output deques
    state['p_out'].append(p_out)
    state['q_out'].append(q_out)
    # Update filtered voltage history
    state['lpf_v'].append(low_pass_filter_v)

    return p_out, q_out


def run_inverter_federate(node_names,
                          simulation_time=30,
                          time_step=1.0,
                          breakpoints_df=None,
                          sbar_df=None):
    """
    Launch the Inverter Federate. For each time step until simulation_time:
      1) Receive voltage data from OpenDSS_Federate (subscription with timeout)
      2) Receive solar irradiance data from Voltage_Consumer_Federate
      3) Receive healthy breakpoints override from Adaptive_Controller_Federate (non-blocking)
      4) Receive attacked breakpoints override from Attack_Federate (non-blocking)
      5) Combine healthy + attack overrides (if any) otherwise use original breakpoints
      6) For each node:
           a) Split its available solar and Sbar across segments (healthy vs. attacked)
           b) Compute P/Q injections per segment via calculate_injection_for_node
           c) Sum segment injections into node-level total
      7) Publish the node-level injections as a dictionary string to HELICS
      8) Advance HELICS time by delta_t and repeat

    :param node_names: list of node identifiers (strings)
    :param simulation_time: total HELICS simulation horizon (seconds)
    :param time_step: HELICS time step (seconds)
    :param breakpoints_df: DataFrame of original breakpoints per node (optional)
    :param sbar_df: DataFrame of original Sbar values per node (optional)
    """
    delta_t = time_step

    # ─── 1) HELICS Setup ────────────────────────────────────────────────────────

    # Create FederateInfo and configure core properties
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Inverter_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, delta_t)

    # Create the value federate for the inverter
    fed = h.helicsCreateValueFederate("Inverter_Federate", fedinfo)

    # Register a publication for “injections” (stringified dictionary of {node: {p, q}})
    pub = h.helicsFederateRegisterPublication(fed,
                                               "injections",
                                               h.HELICS_DATA_TYPE_STRING,
                                               "")

    # Register subscriptions for voltage, solar, healthy overrides, and attack overrides
    voltage_sub = h.helicsFederateRegisterSubscription(fed,
                                                       "OpenDSS_Federate/voltage_out",
                                                       "")
    solar_sub = h.helicsFederateRegisterSubscription(fed,
                                                     "Voltage_Consumer_Federate/solar",
                                                     "")
    healthy_bp_sub = h.helicsFederateRegisterSubscription(fed,
                                                          "Adaptive_Controller_Federate/adaptive_breakpoints",
                                                          "")
    hacked_bp_sub = h.helicsFederateRegisterSubscription(fed,
                                                         "Attack_Federate/breakpoints_attack",
                                                         "")

    # Enter executing mode to allow time stepping
    h.helicsFederateEnterExecutingMode(fed)

    # ─── 2) Load Original Breakpoints and Sbar DataFrames ───────────────────────

    node_breakpoints = {}
    if breakpoints_df is not None:
        # Normalize column names (e.g., lowercasing, stripping whitespace)
        breakpoints_df.columns = breakpoints_df.columns.str.strip().str.lower()
        for col in breakpoints_df.columns:
            vals = breakpoints_df[col].dropna().tolist()
            # We need at least 5 values to form a valid breakpoint list
            if len(vals) >= 5:
                node_breakpoints[col] = [float(x) for x in vals]

    node_sbar = {}
    if sbar_df is not None and sbar_df.shape[0] >= 1:
        row = sbar_df.iloc[0]
        for col, val in row.items():
            # Scale by project’s Sbar_scaling factor
            node_sbar[str(col).strip().lower()] = float(val) * config.Sbar_scaling

    # ─── 3) Initialize Per-Node State Storage ──────────────────────────────────

    # node_states[node] = list of segment states for that node (each segment has its own filters)
    node_states = {node.lower(): [] for node in node_names}

    # Caches for the last received “override” segmentation (healthy or attacked)
    last_override_segments = {}
    healthy_override = {}
    attack_override = {}
    for node in node_names:
        key = node.lower()
        # Use original breakpoints or default if none provided
        orig_bp = node_breakpoints.get(key, DEFAULT_CONTROL_SETTING)
        # Start with a single segment owning 100% of power
        last_override_segments[key] = [{"pct": 1.0, "bp": orig_bp}]

    # ─── 4) Time-Stepping Loop ───────────────────────────────────────────────────

    current_time = 0.0
    while current_time < simulation_time:
        # — Step 4.1) Receive voltage data (dictionary) with a ~1s timeout ───────
        voltage_data = {}
        voltage_timeout = 0
        while not h.helicsInputIsUpdated(voltage_sub) and voltage_timeout < 100:
            time.sleep(0.01)
            voltage_timeout += 1
        try:
            vs = h.helicsInputGetString(voltage_sub)
            voltage_data = eval(vs) if vs.strip().startswith('{') else {}
        except Exception:
            voltage_data = {}

        # — Step 4.2) Receive solar data (dictionary) with a ~1s timeout ─────────
        solar_data = {}
        solar_timeout = 0
        while not h.helicsInputIsUpdated(solar_sub) and solar_timeout < 100:
            time.sleep(0.01)
            solar_timeout += 1
        try:
            ss = h.helicsInputGetString(solar_sub)
            solar_data = eval(ss) if ss.strip().startswith('{') else {}
        except Exception:
            solar_data = {}

        # — Step 4.3) Check for healthy breakpoint override (non-blocking) ──────
        if h.helicsInputIsUpdated(healthy_bp_sub):
            try:
                ho = h.helicsInputGetString(healthy_bp_sub)
                healthy_override = eval(ho) or {}
            except Exception:
                healthy_override = {}
            for key, segs in healthy_override.items():
                # Cache the segments for this node
                last_override_segments[key] = segs

        # — Step 4.4) Check for attacked breakpoint override (non-blocking) ─────
        if h.helicsInputIsUpdated(hacked_bp_sub):
            try:
                ao = h.helicsInputGetString(hacked_bp_sub)
                attack_override = eval(ao) or {}
            except Exception:
                attack_override = {}
            for key, segs in attack_override.items():
                last_override_segments[key] = segs

        # — Step 4.5) Combine healthy + attack overrides for each node ──────────
        for key in node_states.keys():
            orig_bp = node_breakpoints.get(key, DEFAULT_CONTROL_SETTING)
            base = [{"pct": 1.0, "bp": orig_bp}]
            healthy_segs = healthy_override.get(key, base)
            hacked_segs = attack_override.get(key, [])
            last_override_segments[key] = healthy_segs + hacked_segs

        # — Step 4.6) Compute injections for each node ──────────────────────────
        injections = {}
        for node in node_names:
            key = node.lower()

            # 4.6a) Determine measured voltage (fallback to 1.0 pu)
            mv = voltage_data.get(key, voltage_data.get(key[1:] if key.startswith('s') else key, 1.0))
            # 4.6b) Determine measured solar (fallback to 0.0)
            ms = solar_data.get(key, 0.0)

            # 4.6c) Get original breakpoints and Sbar (scaled) for that node
            orig_bp = node_breakpoints.get(key, DEFAULT_CONTROL_SETTING)
            orig_sbar = node_sbar.get(key, S_BAR * config.Sbar_scaling)

            # 4.6d) Retrieve current override segments or default if none
            segments = last_override_segments.get(key, [{"pct": 1.0, "bp": orig_bp}])

            # Debug printing for one example node (s701a)
            if key == 's701a':
                print(f"[Time {current_time}] Node {key} segments:")
                for idx, seg in enumerate(segments):
                    pct = seg.get("pct", 0.0)
                    bp = seg.get("bp", orig_bp)
                    print(f"  Segment {idx}: pct={pct}, breakpoints={bp}")

            # 4.6e) Ensure node_states has one state struct per segment
            states = node_states[key]
            if len(states) < len(segments):
                # If new segments appeared, append fresh state dicts
                for _ in range(len(segments) - len(states)):
                    states.append(initialize_node_state())
            elif len(states) > len(segments):
                # If fewer segments exist now, remove extra states
                node_states[key] = states[:len(segments)]
                states = node_states[key]

            # 4.6f) Sum injections across each segment
            p_total = 0.0
            q_total = 0.0
            for idx, seg in enumerate(segments):
                pct = seg.get("pct", 0.0)
                bp = seg.get("bp", orig_bp)
                solar_seg = ms * pct            # fraction of node’s solar for this segment
                sbar_seg = orig_sbar * pct      # fraction of node’s Sbar for this segment
                state = states[idx]             # state dictionary for this segment

                # Compute P/Q for this segment
                p_seg, q_seg = calculate_injection_for_node(
                    state,
                    current_time,
                    mv,
                    solar_seg,
                    delta_t,
                    bp,
                    LOW_PASS_FILTER_MEASURE,
                    LOW_PASS_FILTER_OUTPUT,
                    sbar_seg,
                    SOLAR_MIN_VALUE
                )
                p_total += p_seg
                q_total += q_seg

            injections[key] = {"p": p_total, "q": q_total}

        # — Step 4.7) Publish the combined injections dictionary to HELICS ───────
        h.helicsPublicationPublishString(pub, str(injections))

        # — Step 4.8) Advance HELICS time by delta_t and repeat ────────────────
        next_time = current_time + delta_t
        current_time = h.helicsFederateRequestTime(fed, next_time)

    # ─── 5) Finalize HELICS Federate ────────────────────────────────────────────

    h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
    #print("[Inverter Federate] Finalized.")
