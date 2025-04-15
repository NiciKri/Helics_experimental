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
        # Low-pass filtered voltage; initialize with nominal voltage (1.0 pu)
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
    solar_irr = measured_solar

    low_pass_filter_v = (delta_t * lpf_m * (vk + vkm1) - (delta_t * lpf_m - 2) * state['lpf_v'][-1]) / (2 + delta_t * lpf_m)

    pk = 0.0
    qk = 0.0
    if solar_irr >= solar_min:
        #print("low_pass_filter_v", low_pass_filter_v)
        if low_pass_filter_v <= control_setting[4]:
            pk = solar_irr  # Active injection taken as positive.
            try:
                q_avail = math.sqrt(max(Sbar**2 - pk**2, 0))
            except Exception:
                q_avail = 0.0

            #print(f"Control setting: 0:{control_setting[0]}, 1: {control_setting[1]}, 2: {control_setting[2]}, 3: {control_setting[3]}, 4: {control_setting[4]}")
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
                d = solar_irr / (control_setting[4] - control_setting[3])
                pk = d * (low_pass_filter_v - control_setting[3])
                try:
                    qk = -math.sqrt(max(Sbar**2 - pk**2, 0))
                except Exception:
                    qk = 0.0
        elif low_pass_filter_v >= control_setting[4]:
            pk = 0.0
            qk = -Sbar

    state['p_set'].append(pk)
    state['q_set'].append(qk)
    p_out_new = (delta_t * LOW_PASS_FILTER_OUTPUT * (state['p_set'][-1] + state['p_set'][-2])
                 - (delta_t * LOW_PASS_FILTER_OUTPUT - 2) * state['p_out'][-1]) / (2 + delta_t * LOW_PASS_FILTER_OUTPUT)
    q_out_new = (delta_t * LOW_PASS_FILTER_OUTPUT * (state['q_set'][-1] + state['q_set'][-2])
                 - (delta_t * LOW_PASS_FILTER_OUTPUT - 2) * state['q_out'][-1]) / (2 + delta_t * LOW_PASS_FILTER_OUTPUT)
    state['p_out'].append(p_out_new)
    state['q_out'].append(q_out_new)
    state['lpf_v'].append(low_pass_filter_v)
    
    return solar_irr, p_out_new, q_out_new

def run_inverter_federate(node_names, simulation_time=30, time_step=1.0,
                          breakpoints_df=None, sbar_df=None):
    """
    Run the inverter federate using node-specific control breakpoints and SBAR values.
    If a node's breakpoints or SBAR value are not provided, the default values are used.
    """
    delta_t = time_step
    
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Inverter_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, delta_t)
    
    fed = h.helicsCreateValueFederate("Inverter_Federate", fedinfo)
    pub = h.helicsFederateRegisterPublication(fed, "injections", h.HELICS_DATA_TYPE_STRING, "")
    
    voltage_sub = h.helicsFederateRegisterSubscription(fed, "OpenDSS_Federate/voltage_out", "")
    solar_sub = h.helicsFederateRegisterSubscription(fed, "Voltage_Consumer_Federate/solar", "")
    
    h.helicsFederateEnterExecutingMode(fed)
    
    # Initialize state for each node.
    node_states = {node.lower(): initialize_node_state() for node in node_names}
    
    # Build mapping for node-specific breakpoint settings.
        # Build mapping for node-specific breakpoint settings.
    node_breakpoints = {}
    if breakpoints_df is not None:
        # Since the file is wide (node names as columns and five rows), use this branch:
        # Strip and convert column names to lower-case.
        breakpoints_df.columns = breakpoints_df.columns.str.strip().str.lower()
        for col in breakpoints_df.columns:
            try:
                # Get the five breakpoint values from the column.
                settings = breakpoints_df[col].dropna().tolist()
                if len(settings) == 5:
                    node_breakpoints[col] = [float(x) for x in settings]
                else:
                    print(f"[WARN] Column '{col}' does not have exactly 5 entries; got: {settings}")
            except Exception as e:
                print(f"[WARN] Invalid breakpoint values for node '{col}': {e}")

        # Debug: print loaded breakpoints.
        print("Loaded node-specific breakpoints:")
        for node, settings in node_breakpoints.items():
            print(f"  {node}: {settings}")
    # Build mapping for node-specific SBAR values.
    node_sbar = {}
    if sbar_df is not None:
        # If the DataFrame has just one row, use that row.
        if sbar_df.shape[0] == 1:
            for col, value in sbar_df.iloc[0].items():
                try:
                    node_sbar[col.strip().lower()] = float(value)
                except Exception as e:
                    print(f"[WARN] Invalid SBAR value for node '{col}': {e}")
        else:
            if "node" in sbar_df.columns:
                for _, row in sbar_df.iterrows():
                    node_name = str(row['node']).strip().lower()
                    try:
                        node_sbar[node_name] = float(row['sbar'])
                    except Exception as e:
                        print(f"[WARN] Invalid SBAR value for node '{node_name}': {e}")
    
    # Count the number of nodes that use the default SBAR value.
    default_sbar_count = sum(1 for node in node_names if node.lower() not in node_sbar)
    print(f"Number of nodes using default SBAR value: {default_sbar_count} out of {len(node_names)}")
    
    current_time = 0
    while current_time < simulation_time:
        # Retrieve voltage data.
        voltage_timeout = 0
        while not h.helicsInputIsUpdated(voltage_sub) and voltage_timeout < 100:
            time.sleep(0.01)
            voltage_timeout += 1
        voltage_str = h.helicsInputGetString(voltage_sub)
        try:
            voltage_data = eval(voltage_str) if voltage_str.strip().startswith('{') else {}
        except Exception as e:
            print(f"[ERROR] Failed to parse voltage data: {e}")
            voltage_data = {}
        
        # Retrieve solar production data.
        solar_timeout = 0
        while not h.helicsInputIsUpdated(solar_sub) and solar_timeout < 100:
            time.sleep(0.01)
            solar_timeout += 1
        solar_str = h.helicsInputGetString(solar_sub)
        try:
            solar_data = eval(solar_str) if solar_str.strip().startswith('{') else {}
        except Exception as e:
            print(f"[ERROR] Failed to parse solar production data: {e}")
            solar_data = {}
        
        injections = {}
        for node in node_names:
            key = node.lower()
            control_setting = node_breakpoints.get(key, DEFAULT_CONTROL_SETTING)
            sbar_value = node_sbar.get(key, S_BAR)*config.Sbar_scaling
            # Get measured voltage.
            if key not in voltage_data and key.startswith('s'):
                measured_voltage = voltage_data.get(key[1:], 1.0)
            else:
                measured_voltage = voltage_data.get(key, 1.0)
            # Get measured solar.
            measured_solar = solar_data.get(key, 0.0)
            
            state = node_states[key]
            solar_irr, p_injection, q_injection = calculate_injection_for_node(
                state, current_time, measured_voltage, measured_solar,
                delta_t=delta_t,
                control_setting=control_setting,
                lpf_m=LOW_PASS_FILTER_MEASURE,
                lpf_o=LOW_PASS_FILTER_OUTPUT,
                Sbar=sbar_value,
                solar_min=SOLAR_MIN_VALUE,
            )
            injections[key] = {"p": p_injection, "q": q_injection}
        
        h.helicsPublicationPublishString(pub, str(injections))
        
        next_time = current_time + delta_t
        granted_time = h.helicsFederateRequestTime(fed, next_time)
        current_time = granted_time

    
    h.helicsFederateFinalize(fed)
    print("[Inverter Federate] Finalized.")
