# federates/opendss_federate.py

import helics as h
from opendssdirect import dss
import time
import os
import config  # Import configuration

# Use config.BASE_DIR for paths, config.SIMULATION_TIME and config.TIME_STEP for simulation parameters
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
    pub = h.helicsFederateRegisterPublication(fed, "voltage_out", h.HELICS_DATA_TYPE_STRING, "")
    sub = h.helicsFederateRegisterSubscription(fed, "Voltage_Consumer_Federate/net_demand", "")
    
    h.helicsFederateEnterExecutingMode(fed)
    
    # Load the IEEE37 DSS file
    dss.Command(f"Redirect {config.BASE_DIR}/data/ieee37.dss")
    print("Loads in DSS after redirect:", dss.Loads.AllNames())
    print("Buses in DSS:", dss.Circuit.AllBusNames())
    
    current_time = 0
    while current_time < config.SIMULATION_TIME:
        next_time = current_time + config.TIME_STEP
        granted_time = h.helicsFederateRequestTime(fed, next_time)
        #print(f"[OpenDSS] Granted time: {granted_time}")
        
        # Wait for net_demand value to be published
        timeout_counter = 0
        while not h.helicsInputIsUpdated(sub):
            time.sleep(0.01)
            timeout_counter += 1
            if timeout_counter > 100:
                print(f"[WARN] No net_demand received at t={granted_time}")
                break

        net_demand_str = h.helicsInputGetString(sub)
        try:
            if net_demand_str.strip().startswith('{'):
                net_demand = eval(net_demand_str)
                if isinstance(net_demand, dict):
                    #if 's701a' in net_demand:
                    #    print(f"[OpenDSS] Received net demand at time {granted_time}: {net_demand['s701a']}")
                    #else:
                    #    print(f"[OpenDSS] Net demand does not contain key 's701a'. Keys received: {list(net_demand.keys())}")
                    for bus, kw in net_demand.items():
                        dss_bus = csv_to_dss_name(bus)
                        if dss_bus in dss.Loads.AllNames():
                            dss.Loads.Name(dss_bus)
                            dss.Loads.kW(kw)
                        else:
                            print(f"[INFO] Load {dss_bus} not found. Skipping.")
            else:
                print(f"[WARN] Invalid net_demand string: '{net_demand_str}'")
        except Exception as e:
            print(f"[ERROR] Failed to parse net_demand: {e}")
        
        dss.Solution.Solve()
        
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
        
        h.helicsPublicationPublishString(pub, str(voltage_dict))
        current_time = granted_time
    
    h.helicsFederateFinalize(fed)
    print("[OpenDSS Federate] Finalized.")
