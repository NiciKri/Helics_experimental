import helics as h
from opendssdirect import dss
import time
import os
import config  # Import configuration

# Convert CSV node names to the DSS naming convention.
def csv_to_dss_name(csv_name):
    if not csv_name.startswith('S') and not csv_name.startswith('s'):
        csv_name = 'S' + csv_name
    return csv_name.lower()

def run_opendss_federate():
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "OpenDSS_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, config.TIME_STEP)
    
    fed = h.helicsCreateValueFederate("OpenDSS_Federate", fedinfo)
    # Subscription for net demand from the Voltage Consumer Federate.
    sub = h.helicsFederateRegisterSubscription(fed, "Voltage_Consumer_Federate/load", "")
    # New subscription for inverter injections from the Inverter Federate.
    inverter_sub = h.helicsFederateRegisterSubscription(fed, "Inverter_Federate/injections", "")
    # Publication for voltage output.
    pub = h.helicsFederateRegisterPublication(fed, "voltage_out", h.HELICS_DATA_TYPE_STRING, "")
    
    h.helicsFederateEnterExecutingMode(fed)
    
    # Load the IEEE37 DSS file.
    dss.Command(f"Redirect {config.BASE_DIR}/data/ieee37.dss")
    print("Loads in DSS after redirect:", dss.Loads.AllNames())
    print("Buses in DSS:", dss.Circuit.AllBusNames())
    
    # If available, create a mapping for each load’s initial reactive power.
    # This assumes that each load is defined in OpenDSS with both kW and kVAR.
    initial_reactive = {}
    for load_name in dss.Loads.AllNames():
        dss.Loads.Name(load_name)
        try:
            # Attempt to read the reactive power; if not available, default to 0.
            reactive_val = dss.Loads.kvar()  
        except Exception as e:
            print(f"[WARN] Could not retrieve kvar for load {load_name}: {e}")
            reactive_val = 0
        initial_reactive[load_name] = reactive_val

    current_time = 0
    while current_time < config.SIMULATION_TIME:
        next_time = current_time + config.TIME_STEP
        granted_time = h.helicsFederateRequestTime(fed, next_time)
        
        # Wait for load update from the load publisher.
        timeout_counter = 0
        while not h.helicsInputIsUpdated(sub):
            time.sleep(0.01)
            timeout_counter += 1
            if timeout_counter > 100:
                print(f"[WARN] No load received at t={granted_time}")
                break
        
        load_str = h.helicsInputGetString(sub)
        load = {}
        if load_str.strip().startswith('{'):
            try:
                load = eval(load_str)
            except Exception as e:
                print(f"[ERROR] Failed to parse load: {e}")
        else:
            print(f"[WARN] Invalid load string: '{load_str}'")
        
        # Wait briefly for inverter injection data.
        inverter_timeout = 0
        while not h.helicsInputIsUpdated(inverter_sub) and inverter_timeout < 100:
            time.sleep(0.01)
            inverter_timeout += 1
        
        inverter_injections_str = h.helicsInputGetString(inverter_sub)
        inverter_injections = {}
        if inverter_injections_str.strip().startswith('{'):
            try:
                inverter_injections = eval(inverter_injections_str)
            except Exception as e:
                print(f"[ERROR] Failed to parse inverter injections: {e}")
        else:
            print(f"[WARN] Invalid inverter injection string: '{inverter_injections_str}'")
        
        # Process net demand and adjust using inverter active and reactive power injections.
        print_flag = True  # flag to control printing of load values
        for bus, kw in load.items():
            dss_bus = csv_to_dss_name(bus)
            modified_kw = kw
            # Get the original reactive load (if available) or set a default (e.g., 0).
            modified_kvar = initial_reactive.get(dss_bus, 0)
            if dss_bus in inverter_injections:
                try:
                    p_inj = float(inverter_injections[dss_bus].get('p', 0))
                    q_inj = float(inverter_injections[dss_bus].get('q', 0))
                    modified_kw = kw - p_inj
                    modified_kvar = modified_kvar - q_inj
                    if print_flag:
                        print(f"[INFO] t={granted_time} | Node {dss_bus}: load={kw}, inverter p_injection={p_inj}, "
                              f"modified load={modified_kw}, inverter q_injection={q_inj}, modified kvar load={modified_kvar}")
                        print_flag = False  # Only print once per time step
                except Exception as e:
                    print(f"[ERROR] Error processing inverter injection for {dss_bus}: {e}")
            
            if dss_bus in dss.Loads.AllNames():
                dss.Loads.Name(dss_bus)
                dss.Loads.kW(modified_kw)
                try:
                    # Update the reactive power demand as well.
                    dss.Loads.kvar(modified_kvar)
                except Exception as e:
                    print(f"[WARN] Unable to update kvar for {dss_bus}: {e}")
            else:
                print(f"[INFO] Load {dss_bus} not found. Skipping.")
        
        # Solve the power flow in OpenDSS.
        dss.Solution.Solve()
        
        # Collect voltage values.
        voltage_dict = {}
        bus_names = dss.Circuit.AllBusNames()
        for bus in bus_names:
            dss.Circuit.SetActiveBus(bus)
            voltage_data = dss.Bus.puVmagAngle()
            num_phases = dss.Bus.NumNodes()
            for i in range(num_phases):
                phase_label = chr(ord('a') + i)
                key = bus.lower() + phase_label
                voltage_dict[key] = voltage_data[2 * i]
        
        # Publish the voltage data.
        h.helicsPublicationPublishString(pub, str(voltage_dict))
        current_time = granted_time
    
    h.helicsFederateFinalize(fed)
    print("[OpenDSS Federate] Finalized.")
