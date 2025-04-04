import helics as h
from opendssdirect import dss
import pandas as pd
import time
import threading
import os
import ast

# Set common simulation parameters
simulation_time = 300
time_step = 1.0

# Set the working directory
base_dir = r"C:/Users/nicol/Helics_experimental"
os.chdir(base_dir)

# =============================================================================
# Mapping Functions
# =============================================================================
def csv_to_dss_name(csv_name):
    if not csv_name.startswith('S') and not csv_name.startswith('s'):
        csv_name = 'S' + csv_name
    return csv_name.lower()

def dss_to_csv_name(dss_name):
    return dss_name.capitalize()

# =============================================================================
# HELICS Broker Setup
# =============================================================================
def start_broker():
    global broker
    broker = h.helicsCreateBroker("zmq", "", "--federates=2")
    print("Broker started.")

broker_thread = threading.Thread(target=start_broker, daemon=True)
broker_thread.start()

# Wait for the broker to be fully connected before proceeding
while True:
    try:
        if 'broker' in globals() and h.helicsBrokerIsConnected(broker):
            print("Broker is connected.")
            break
    except Exception as e:
        print(f"Waiting for broker initialization: {e}")
    time.sleep(0.1)

# =============================================================================
# Data Loading and Helper Function
# =============================================================================
solar_data = pd.read_csv(r"C:\Users\nicol\Helics_experimental\data\solar_data.csv")
solar_data.columns = solar_data.columns.str.replace('_pv$', '', regex=True)
solar_data['time'] = solar_data.index

load_data = pd.read_csv(r"C:\Users\nicol\Helics_experimental\data\load_data.csv")
load_data['time'] = load_data.index
load_data.sort_values('time', inplace=True)

node_names = [col for col in solar_data.columns if col != 'time']

def get_values_at_time(t, df):
    if t in df['time'].values:
        row = df[df['time'] == t].iloc[0]
    else:
        row = df.iloc[-1]
    return row.drop('time').to_dict()

# =============================================================================
# Main Simulation with Asynchronous Time Requests
# =============================================================================
def main_simulation():
    # ----------------------------
    # Create OpenDSS Federate (publisher of voltages, subscriber of net demand)
    # ----------------------------
    fedinfo_dss = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo_dss, "OpenDSS_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo_dss, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo_dss, h.HELICS_PROPERTY_TIME_DELTA, time_step)
    fed_dss = h.helicsCreateValueFederate("OpenDSS_Federate", fedinfo_dss)
    pub_dss = h.helicsFederateRegisterPublication(fed_dss, "voltage_out", h.HELICS_DATA_TYPE_STRING, "")
    sub_dss = h.helicsFederateRegisterSubscription(fed_dss, "Voltage_Consumer_Federate/net_demand", "")
    h.helicsFederateEnterExecutingMode(fed_dss)

    # Load the IEEE37 DSS file
    ret = dss.Command(f"Redirect {base_dir}/data/ieee37.dss")
    print("DSS Command Return:", ret)
    print("Loads in DSS:", dss.Loads.AllNames())
    print("Buses in DSS:", dss.Circuit.AllBusNames())

    # ----------------------------
    # Create Voltage Consumer Federate (publisher of net demand, subscriber of voltages)
    # ----------------------------
    fedinfo_v = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo_v, "Voltage_Consumer_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo_v, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo_v, h.HELICS_PROPERTY_TIME_DELTA, time_step)
    fed_v = h.helicsCreateValueFederate("Voltage_Consumer_Federate", fedinfo_v)
    pub_v = h.helicsFederateRegisterPublication(fed_v, "net_demand", h.HELICS_DATA_TYPE_STRING, "")
    sub_v = h.helicsFederateRegisterSubscription(fed_v, "OpenDSS_Federate/voltage_out", "")
    h.helicsFederateEnterExecutingMode(fed_v)
    time.sleep(1)  # Allow additional initialization time

    voltage_timeseries = []
    current_time = 0

    while current_time < simulation_time:
        next_time = current_time + time_step

        # ===========================================================
        # Phase 1: Consumer Publishes Net Demand
        # ===========================================================
        solar_values = get_values_at_time(current_time, solar_data)
        load_values = get_values_at_time(current_time, load_data)
        net_demand = {node.lower(): load_values.get(node, 0) - solar_values.get(node, 0)
                      for node in node_names}
        print(f"[Consumer] Time: {current_time} | Net Demand (s701a): {net_demand.get('s701a', 'N/A')}")
        h.helicsPublicationPublishString(pub_v, str(net_demand))

        # ===========================================================
        # Phase 2: OpenDSS Receives Net Demand, Processes, and Publishes Voltage
        # ===========================================================
        timeout_counter = 0
        while not h.helicsInputIsUpdated(sub_dss) and timeout_counter < 100:
            time.sleep(0.01)
            timeout_counter += 1
        net_demand_str = h.helicsInputGetString(sub_dss)
        if net_demand_str.strip().startswith('{'):
            try:
                # Use ast.literal_eval for safe parsing
                net_demand_data = ast.literal_eval(net_demand_str)
                if isinstance(net_demand_data, dict):
                    if 's701a' in net_demand_data:
                        print(f"[OpenDSS] Received net demand at t={next_time}: {net_demand_data['s701a']}")
                    else:
                        print(f"[OpenDSS] Net demand keys: {list(net_demand_data.keys())}")
                    for bus, kw in net_demand_data.items():
                        dss_bus = csv_to_dss_name(bus)
                        if dss_bus in dss.Loads.AllNames():
                            dss.Loads.Name(dss_bus)
                            dss.Loads.kW(kw)
                        else:
                            print(f"[INFO] Load {dss_bus} not found. Skipping.")
                else:
                    print("[WARN] net_demand is not a dict")
            except Exception as e:
                print(f"[ERROR] Parsing net_demand failed: {e}")
        else:
            print(f"[WARN] Invalid net_demand string: '{net_demand_str}'")

        dss.Solution.Solve()

        voltage_dict = {}
        for bus in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus)
            voltage_data = dss.Bus.puVmagAngle()
            num_phases = dss.Bus.NumNodes()
            for i in range(num_phases):
                phase_label = chr(ord('a') + i)
                key = bus.lower() + phase_label
                voltage_dict[key] = voltage_data[2 * i]
        h.helicsPublicationPublishString(pub_dss, str(voltage_dict))
        print(f"[OpenDSS] Published voltage data at t={next_time}")

        # ===========================================================
        # Phase 3: Both Federates Request Time Advance Concurrently (Asynchronously)
        # ===========================================================
        h.helicsFederateRequestTimeAsync(fed_v, next_time)
        h.helicsFederateRequestTimeAsync(fed_dss, next_time)
        granted_time_v = h.helicsFederateRequestTimeComplete(fed_v)
        granted_time_dss = h.helicsFederateRequestTimeComplete(fed_dss)
        print(f"[Main] Advanced to time {next_time} (consumer: {granted_time_v}, dss: {granted_time_dss})")

        # ===========================================================
        # Phase 4: Consumer Receives Voltage Update
        # ===========================================================
        voltage_timeout = 0
        while not h.helicsInputIsUpdated(sub_v) and voltage_timeout < 100:
            time.sleep(0.01)
            voltage_timeout += 1
        voltage_str = h.helicsInputGetString(sub_v)
        if voltage_str.strip().startswith('{'):
            try:
                # Use ast.literal_eval for safe parsing
                voltage_data = ast.literal_eval(voltage_str)
                if isinstance(voltage_data, dict):
                    voltage_data_csv = {dss_to_csv_name(key): value for key, value in voltage_data.items()}
                    voltage_data_csv['time'] = next_time
                    voltage_timeseries.append(voltage_data_csv.copy())
                    print(f"[Consumer] Time: {next_time} | Voltages (701a): {voltage_data_csv.get('701a', 'N/A')}")
                else:
                    print("[WARN] Voltage data is not a dict")
            except Exception as e:
                print(f"[ERROR] Evaluating voltage data failed: {e}")
        else:
            print(f"[WARN] Malformed voltage string: '{voltage_str}'")

        current_time = next_time

    # Finalize federates and clean up
    h.helicsFederateFinalize(fed_v)
    print("[Voltage Consumer Federate] Finalized.")
    h.helicsFederateFinalize(fed_dss)
    print("[OpenDSS Federate] Finalized.")

    try:
        pd.DataFrame(voltage_timeseries).to_csv("voltage_timeseries.csv", index=False)
        print("[Voltage Data] Saved to 'voltage_timeseries.csv'")
    except Exception as e:
        print(f"[ERROR] Could not save voltage data: {e}")

    if 'broker' in globals() and h.helicsBrokerIsConnected(broker):
        h.helicsBrokerDisconnect(broker)
        h.helicsBrokerFree(broker)
    print("Simulation complete. Broker closed.")

if __name__ == "__main__":
    main_simulation()
