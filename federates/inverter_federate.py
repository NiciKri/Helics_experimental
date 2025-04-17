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
    """Initialize and return a state dictionary for one node."""
    state = {
        'p_set': deque([0, 0], maxlen=2),
        'q_set': deque([0, 0], maxlen=2),
        'p_out': deque([0, 0], maxlen=2),
        'q_out': deque([0, 0], maxlen=2),
        'lpf_v': deque([1.0, 1.0], maxlen=2)
    }
    return state


def calculate_injection_for_node(state, current_time, measured_voltage, measured_solar,
                                 delta_t=DELTA_T,
                                 control_setting=DEFAULT_CONTROL_SETTING,
                                 lpf_m=LOW_PASS_FILTER_MEASURE,
                                 lpf_o=LOW_PASS_FILTER_OUTPUT,
                                 Sbar=S_BAR,
                                 solar_min=SOLAR_MIN_VALUE):
    """
    Compute active (p) and reactive (q) power injections.
    Applies a low-pass filter to the voltage and uses the inverter control curve.
    Active injections are returned as positive numbers, and the node‐specific
    SBAR (apparent power rating) is applied.
    """
    vk = measured_voltage
    vkm1 = state['lpf_v'][-1]
    # Low-pass filter for voltage
    low_pass_filter_v = (delta_t * lpf_m * (vk + vkm1) - (delta_t * lpf_m - 2) * state['lpf_v'][-1]) / (2 + delta_t * lpf_m)

    pk = 0.0
    qk = 0.0
    if measured_solar >= solar_min:
        if low_pass_filter_v <= control_setting[-1]:
            pk = measured_solar
            try:
                q_avail = math.sqrt(max(Sbar**2 - pk**2, 0))
            except:
                q_avail = 0.0
            # Volt-Var control segments
            if low_pass_filter_v <= control_setting[0]:
                qk = q_avail
            elif control_setting[0] < low_pass_filter_v <= control_setting[1]:
                c = q_avail / (control_setting[1] - control_setting[0])
                qk = c * (control_setting[1] - low_pass_filter_v)
            elif control_setting[1] < low_pass_filter_v <= control_setting[2]:
                qk = 0.0
            elif control_setting[2] < low_pass_filter_v <= control_setting[3]:
                c = q_avail / (control_setting[3] - control_setting[2])
                qk = -c * (low_pass_filter_v - control_setting[2])
            elif control_setting[3] < low_pass_filter_v < control_setting[4]:
                d = measured_solar / (control_setting[4] - control_setting[3])
                pk = d * (low_pass_filter_v - control_setting[3])
                try:
                    qk = -math.sqrt(max(Sbar**2 - pk**2, 0))
                except:
                    qk = 0.0
        else:
            pk = 0.0
            qk = -Sbar

    state['p_set'].append(pk)
    state['q_set'].append(qk)
    p_out_new = (delta_t * lpf_o * (state['p_set'][-1] + state['p_set'][-2]) - (delta_t * lpf_o - 2) * state['p_out'][-1]) / (2 + delta_t * lpf_o)
    q_out_new = (delta_t * lpf_o * (state['q_set'][-1] + state['q_set'][-2]) - (delta_t * lpf_o - 2) * state['q_out'][-1]) / (2 + delta_t * lpf_o)
    state['p_out'].append(p_out_new)
    state['q_out'].append(q_out_new)
    state['lpf_v'].append(low_pass_filter_v)

    return measured_solar, p_out_new, q_out_new


def run_inverter_federate(node_names, simulation_time=30, time_step=1.0,
                          breakpoints_df=None, sbar_df=None):
    """
    Run the inverter federate using node-specific control breakpoints and SBAR values.
    Integrates attack overrides when published by the Attack_Federate.
    """
    delta_t = time_step

    # HELICS federate setup
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

    # Initialize state
    node_states = {node.lower(): initialize_node_state() for node in node_names}

    # Load node-specific breakpoints
    node_breakpoints = {}
    if breakpoints_df is not None:
        breakpoints_df.columns = breakpoints_df.columns.str.strip().str.lower()
        for col in breakpoints_df.columns:
            vals = breakpoints_df[col].dropna().tolist()
            if len(vals) == 5:
                node_breakpoints[col] = [float(x) for x in vals]

    # Load node-specific SBAR
    node_sbar = {}
    if sbar_df is not None:
        if sbar_df.shape[0] == 1:
            for col, val in sbar_df.iloc[0].items():
                node_sbar[col.strip().lower()] = float(val)
        elif "node" in sbar_df.columns:
            for _, row in sbar_df.iterrows():
                node_sbar[str(row['node']).strip().lower()] = float(row['sbar'])

    # Track default usage
    default_count = sum(1 for n in node_names if n.lower() not in node_sbar)
    print(f"Nodes using default SBAR: {default_count}/{len(node_names)}")

    # Attack overrides (empty until first message)
    attack_override = {}

    current_time = 0.0
    while current_time < simulation_time:
        # Receive voltage
        voltage_timeout = 0
        while not h.helicsInputIsUpdated(voltage_sub) and voltage_timeout < 100:
            time.sleep(0.01); voltage_timeout += 1
        voltage_data = {}
        vs = h.helicsInputGetString(voltage_sub)
        try:
            voltage_data = eval(vs) if vs.strip().startswith('{') else {}
        except:
            pass

        # Receive solar
        solar_timeout = 0
        while not h.helicsInputIsUpdated(solar_sub) and solar_timeout < 100:
            time.sleep(0.01); solar_timeout += 1
        solar_data_msg = h.helicsInputGetString(solar_sub)
        solar_data = {}
        try:
            solar_data = eval(solar_data_msg) if solar_data_msg.strip().startswith('{') else {}
        except:
            pass

        # Check for attack override
        if h.helicsInputIsUpdated(attack_sub):
            atk_str = h.helicsInputGetString(attack_sub)
            try:
                attack_override = eval(atk_str)
            except:
                attack_override = {}

        injections = {}
        for node in node_names:
            key = node.lower()
            # Base settings
            control_setting = node_breakpoints.get(key, DEFAULT_CONTROL_SETTING)
            sbar_val = node_sbar.get(key, S_BAR) * config.Sbar_scaling

            # Apply attack if exists
            if key in attack_override:
                atk = attack_override[key]
                bp_list = atk.get("bp", None)
                hack_pct = atk.get("hack_pct", 0.0)
                if bp_list is not None:
                    control_setting = bp_list
                sbar_val = sbar_val * (1.0 - hack_pct)

            # Retrieve measurements
            if key not in voltage_data and key.startswith('s'):
                measured_voltage = voltage_data.get(key[1:], 1.0)
            else:
                measured_voltage = voltage_data.get(key, 1.0)
            measured_solar = solar_data.get(key, 0.0)

            state = node_states[key]
            _, p_out, q_out = calculate_injection_for_node(
                state, current_time, measured_voltage, measured_solar,
                delta_t, control_setting,
                LOW_PASS_FILTER_MEASURE, LOW_PASS_FILTER_OUTPUT,
                sbar_val, SOLAR_MIN_VALUE
            )
            injections[key] = {"p": p_out, "q": q_out}

        # Publish injections
        h.helicsPublicationPublishString(pub, str(injections))

        # Advance time
        next_time = current_time + delta_t
        current_time = h.helicsFederateRequestTime(fed, next_time)

    h.helicsFederateFinalize(fed)
    print("[Inverter Federate] Finalized.")
