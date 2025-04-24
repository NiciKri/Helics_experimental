import helics as h
import time
import math
from collections import deque
import numpy as np
import config  # Import the configuration

# Control parameters for inverter/PV device logic.
DEFAULT_CONTROL_SETTING = [0.98, 1.01, 1.02, 1.05, 1.07]
LOW_PASS_FILTER_MEASURE = 1.2    # lpf measure coefficient (m)
LOW_PASS_FILTER_OUTPUT = 0.1     # lpf output coefficient (o)
S_BAR = 200.0                    # Default apparent power rating (SBAR)
SOLAR_MIN_VALUE = 5.0            # Minimum solar irradiance threshold
DELTA_T = 1.0                    # Default time step

def initialize_node_state():
    """Initialize and return a state dictionary for one segment of one node."""
    return {
        'p_set': deque([0, 0], maxlen=2),
        'q_set': deque([0, 0], maxlen=2),
        'p_out': deque([0, 0], maxlen=2),
        'q_out': deque([0, 0], maxlen=2),
        'lpf_v': deque([1.0, 1.0], maxlen=2)
    }

def calculate_injection_for_node(state, current_time, measured_voltage, measured_solar,
                                 delta_t=DELTA_T,
                                 control_setting=DEFAULT_CONTROL_SETTING,
                                 lpf_m=LOW_PASS_FILTER_MEASURE,
                                 lpf_o=LOW_PASS_FILTER_OUTPUT,
                                 Sbar=S_BAR,
                                 solar_min=SOLAR_MIN_VALUE):
    """
    Compute active (p) and reactive (q) power injections for a given state and control curve.
    Applies low-pass filtering and Volt-Var logic.
    """
    vk = measured_voltage
    vkm1 = state['lpf_v'][-1]
    # Low-pass filter for voltage
    low_pass_filter_v = (delta_t * lpf_m * (vk + vkm1) - (delta_t * lpf_m - 2) * vkm1) / (2 + delta_t * lpf_m)

    pk = 0.0
    qk = 0.0
    if measured_solar >= solar_min and low_pass_filter_v <= control_setting[-1]:
        pk = measured_solar
        try:
            q_avail = math.sqrt(max(Sbar**2 - pk**2, 0))
        except:
            q_avail = 0.0
        # Volt-Var control segments
        if low_pass_filter_v <= control_setting[0]:
            qk = q_avail
        elif low_pass_filter_v <= control_setting[1]:
            c = q_avail / (control_setting[1] - control_setting[0])
            qk = c * (control_setting[1] - low_pass_filter_v)
        elif low_pass_filter_v <= control_setting[2]:
            qk = 0.0
        elif low_pass_filter_v <= control_setting[3]:
            c = q_avail / (control_setting[3] - control_setting[2])
            qk = -c * (low_pass_filter_v - control_setting[2])
        else:
            d = measured_solar / (control_setting[4] - control_setting[3])
            pk = d * (low_pass_filter_v - control_setting[3])
            try:
                qk = -math.sqrt(max(Sbar**2 - pk**2, 0))
            except:
                qk = 0.0
    elif low_pass_filter_v > control_setting[-1]:
        # Above max voltage, sink reactive
        qk = -Sbar

    # Update filters
    state['p_set'].append(pk)
    state['q_set'].append(qk)
    p_out = (delta_t * lpf_o * (state['p_set'][-1] + state['p_set'][-2]) - (delta_t * lpf_o - 2) * state['p_out'][-1]) / (2 + delta_t * lpf_o)
    q_out = (delta_t * lpf_o * (state['q_set'][-1] + state['q_set'][-2]) - (delta_t * lpf_o - 2) * state['q_out'][-1]) / (2 + delta_t * lpf_o)
    state['p_out'].append(p_out)
    state['q_out'].append(q_out)
    state['lpf_v'].append(low_pass_filter_v)

    return p_out, q_out

def run_inverter_federate(node_names, simulation_time=30, time_step=1.0,
                          breakpoints_df=None, sbar_df=None):
    """
    Run inverter federate, computing injections for each segment (healthy + attacks).
    Always use the last breakpoints received from the broker; original breakpoints are backups.
    """
    delta_t = time_step

    # HELICS setup
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Inverter_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, delta_t)

    fed = h.helicsCreateValueFederate("Inverter_Federate", fedinfo)
    pub = h.helicsFederateRegisterPublication(fed, "injections", h.HELICS_DATA_TYPE_STRING, "")
    voltage_sub = h.helicsFederateRegisterSubscription(fed, "OpenDSS_Federate/voltage_out", "")
    solar_sub = h.helicsFederateRegisterSubscription(fed, "Voltage_Consumer_Federate/solar", "")
    attack_sub = h.helicsFederateRegisterSubscription(fed, "Attack_Federate/breakpoints_attack", "")

    h.helicsFederateEnterExecutingMode(fed)

    # Load breakpoints and Sbar
    node_breakpoints = {}
    if breakpoints_df is not None:
        breakpoints_df.columns = breakpoints_df.columns.str.strip().str.lower()
        for col in breakpoints_df.columns:
            vals = breakpoints_df[col].dropna().tolist()
            if len(vals) >= 5:
                node_breakpoints[col] = [float(x) for x in vals]

    node_sbar = {}
    if sbar_df is not None and sbar_df.shape[0] >= 1:
        row = sbar_df.iloc[0]
        for col, val in row.items():
            node_sbar[str(col).strip().lower()] = float(val) * config.Sbar_scaling

    # State storage: node -> list of states per segment
    node_states = {node.lower(): [] for node in node_names}

    # Cache for last received segments per node (override or original)
    last_override_segments = {}
    for node in node_names:
        key = node.lower()
        orig_bp = node_breakpoints.get(key, DEFAULT_CONTROL_SETTING)
        last_override_segments[key] = [{"pct": 1.0, "bp": orig_bp}]

    current_time = 0.0
    while current_time < simulation_time:
        # Receive voltage with timeout
        voltage_data = {}
        voltage_timeout = 0
        while not h.helicsInputIsUpdated(voltage_sub) and voltage_timeout < 100:
            time.sleep(0.01)
            voltage_timeout += 1
        try:
            vs = h.helicsInputGetString(voltage_sub)
            voltage_data = eval(vs) if vs.strip().startswith('{') else {}
        except:
            voltage_data = {}

        # Receive solar with timeout
        solar_data = {}
        solar_timeout = 0
        while not h.helicsInputIsUpdated(solar_sub) and solar_timeout < 100:
            time.sleep(0.01)
            solar_timeout += 1
        try:
            ss = h.helicsInputGetString(solar_sub)
            solar_data = eval(ss) if ss.strip().startswith('{') else {}
        except:
            solar_data = {}

        # Check for attack override (no blocking)
        if h.helicsInputIsUpdated(attack_sub):
            try:
                ao = h.helicsInputGetString(attack_sub)
                attack_override = eval(ao) or {}
            except:
                attack_override = {}
            # Update cached segments with latest override
            for key, segs in attack_override.items():
                last_override_segments[key] = segs

        injections = {}
        for node in node_names:
            key = node.lower()

            # Measurements
            mv = voltage_data.get(key, voltage_data.get(key[1:] if key.startswith('s') else key, 1.0))
            ms = solar_data.get(key, 0.0)

            # Original control and Sbar
            orig_bp = node_breakpoints.get(key, DEFAULT_CONTROL_SETTING)
            orig_sbar = node_sbar.get(key, S_BAR * config.Sbar_scaling)

            # Segments list: use last known override or original as backup
            segments = last_override_segments.get(key, [{"pct": 1.0, "bp": orig_bp}])

            # Align state list length
            states = node_states[key]
            if len(states) < len(segments):
                for _ in range(len(segments) - len(states)):
                    states.append(initialize_node_state())
            elif len(states) > len(segments):
                node_states[key] = states[:len(segments)]
                states = node_states[key]

            # Compute injections by segment
            p_total = 0.0
            q_total = 0.0
            for idx, seg in enumerate(segments):
                pct = seg.get("pct", 0.0)
                bp = seg.get("bp", orig_bp)
                solar_seg = ms * pct
                sbar_seg = orig_sbar * pct
                state = states[idx]
                p_seg, q_seg = calculate_injection_for_node(
                    state, current_time, mv, solar_seg,
                    delta_t, bp, LOW_PASS_FILTER_MEASURE, LOW_PASS_FILTER_OUTPUT,
                    sbar_seg, SOLAR_MIN_VALUE
                )
                p_total += p_seg
                q_total += q_seg

            injections[key] = {"p": p_total, "q": q_total}

        # Publish injections
        h.helicsPublicationPublishString(pub, str(injections))

        # Advance time
        next_time = current_time + delta_t
        current_time = h.helicsFederateRequestTime(fed, next_time)

    h.helicsFederateDisconnect(fed)
    h.helicsFederateFinalize(fed)
    print("[Inverter Federate] Finalized.")
