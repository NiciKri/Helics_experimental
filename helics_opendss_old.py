import helics as h 
from opendssdirect import dss
import pandas as pd
import time
import threading
import matplotlib as plt
import os

base_dir = r"C:/Users/nicol/Helics_experimental"
os.chdir(base_dir)

def start_broker():
    global broker
    broker = h.helicsCreateBroker("zmq", "", "--federates=2")

# --- Step 1: Debug printing actual bus, node, and load names ---
def debug_print_bus_and_nodes():
    print("=== Debug: Bus Names and Nodes ===")
    for bus in dss.Circuit.AllBusNames():
        dss.Circuit.SetActiveBus(bus)
        nodes = dss.Bus.Nodes()
        print(f"Bus: {bus}, Nodes: {nodes}")
    print("=== Debug: Load Names and their Bus ===")
    for name in dss.Loads.AllNames():
        dss.Loads.Name(name)
        try:
            # Use BusName() because Bus() is not available
            bus_name = dss.Loads.BusName()
        except Exception as e:
            bus_name = f"Error: {e}"
        print(f"Load: {name}, Bus: {bus_name}")

# --- Build node mappings based on actual names ---
def build_node_mappings():
    # Step 1: Inspect actual bus and node names
    debug_print_bus_and_nodes()
    
    forward = {}  # Mapping: dss_label -> custom_labela
    reverse = {}  # Mapping: custom_label -> dss_labelaa

    # Adjust the mapping based on your circuit naming. Here, we assume that
    # the custom label should be in the form "S{bus}{node}", with the node letter uppercased.
    for bus in dss.Circuit.AllBusNames():
        dss.Circuit.SetActiveBus(bus)
        nodes = dss.Bus.Nodes()
        for node in nodes:
            dss_label = f"{bus}.{node}"
            # Change here if your CSV headers are different (e.g., "S701A" instead of "S701a")
            custom_label = f"S{bus}{node.upper()}"
            forward[dss_label] = custom_label
            reverse[custom_label] = dss_label
    return forward, reverse

# Start HELICS broker
broker_thread = threading.Thread(target=start_broker, daemon=True)
broker_thread.start()
print("[Broker] Started")
time.sleep(1)

def run_opendss_federate():
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "OpenDSS_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, 1.0)

    fed = h.helicsCreateValueFederate("OpenDSS_Federate", fedinfo)

    pub = h.helicsFederateRegisterPublication(fed, "voltage_out", h.HELICS_DATA_TYPE_STRING, "")
    sub = h.helicsFederateRegisterSubscription(fed, "Voltage_Consumer_Federate/net_demand", "")

    h.helicsFederateEnterExecutingMode(fed)

    # Load and redirect the IEEE37 circuit file
    dss.Command(f"Redirect {base_dir}/data/ieee37.dss")
    node_mapping, reverse_node_mapping = build_node_mappings()

    time_step = 0
    while time_step < 10:
        granted_time = h.helicsFederateRequestTime(fed, time_step)

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
                    print(f"[Time {granted_time}] Received net demand:")
                    for custom_label, kw in net_demand.items():
                        dss_label = reverse_node_mapping.get(custom_label)
                        if not dss_label:
                            print(f"  ⚠ No mapping found for {custom_label}")
                            continue

                        bus, _ = dss_label.split(".")
                        dss.Circuit.SetActiveBus(bus)

                        applied = False
                        for name in dss.Loads.AllNames():
                            dss.Loads.Name(name)
                            # Use BusName() instead of Bus()
                            if dss.Loads.BusName().split('.')[0] == bus:
                                dss.Loads.kW(kw)
                                print(f"  ✅ Applied {kw:.2f} kW to load '{name}' at bus {bus}")
                                applied = True
                                break
                        if not applied:
                            print(f"  ❌ No load found at bus {bus} for net demand {kw:.2f}")
            else:
                print(f"[WARN] Invalid net_demand string: '{net_demand_str}'")
        except Exception as e:
            print(f"[ERROR] Failed to parse net_demand: {e}")

        dss.Solution.Solve()

        if not dss.Solution.Converged():
            print(f"[ERROR] Power flow did not converge at time {granted_time}")

        # Publish voltages using custom node names
        voltage_dict = {}
        for bus in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus)
            pu_va = dss.Bus.puVmagAngle()
            voltages = pu_va[::2]

            if not voltages or any(v > 10 or v < 0 for v in voltages):
                continue

            nodes = dss.Bus.Nodes()
            for i, node in enumerate(nodes):
                dss_label = f"{bus}.{node}"
                custom_label = node_mapping.get(dss_label, dss_label)
                voltage_dict[custom_label] = voltages[i] if i < len(voltages) else None

        # Filter invalid voltages before publishing
        import math
        clean_voltage_dict = {
            k: v for k, v in voltage_dict.items() if v is not None and math.isfinite(v)
        }

        h.helicsPublicationPublishString(pub, str(clean_voltage_dict))

        time_step += 1

    h.helicsFederateFinalize(fed)
    print("[OpenDSS Federate] Finalized.")

# Load data
solar_data = pd.read_csv(r"C:\\Users\\nicol\\Helics_experimental\\data\\solar_data.csv")
solar_data.columns = solar_data.columns.str.replace('_pv$', '', regex=True)
solar_data['time'] = solar_data.index

load_data = pd.read_csv(r"C:\\Users\\nicol\\Helics_experimental\\data\\load_data.csv")
load_data['time'] = load_data.index
load_data.sort_values('time', inplace=True)

node_names = [col for col in solar_data.columns if col != 'time']

def get_values_at_time(t, df):
    if t in df['time'].values:
        row = df[df['time'] == t].iloc[0]
    else:
        row = df.iloc[-1]
    return row.drop('time').to_dict()

def run_voltage_consumer_federate():
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Voltage_Consumer_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, 1.0)

    fed = h.helicsCreateValueFederate("Voltage_Consumer_Federate", fedinfo)

    pub = h.helicsFederateRegisterPublication(fed, "net_demand", h.HELICS_DATA_TYPE_STRING, "")
    sub = h.helicsFederateRegisterSubscription(fed, "OpenDSS_Federate/voltage_out", "")

    h.helicsFederateEnterExecutingMode(fed)

    time.sleep(1)

    current_time = 0
    end_time = 20
    time_step = 1
    voltage_timeseries = []

    while current_time < end_time:
        if h.helicsBrokerIsConnected(broker):
            print("[Broker] Still connected.")
        else:
            print("[Broker] Disconnected too early!")

        solar_values = get_values_at_time(current_time, solar_data)
        load_values = get_values_at_time(current_time, load_data)
        net_demand = {node: load_values.get(node, 0) - solar_values.get(node, 0) for node in node_names}
        print(f"Time: {current_time} | Net Demand: {net_demand.get('S742b', 'NA')}")

        h.helicsPublicationPublishString(pub, str(net_demand))

        next_time = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_time)

        try:
            voltage_str = h.helicsInputGetString(sub)
            if voltage_str.strip().startswith('{'):
                voltage_data = eval(voltage_str)
                if isinstance(voltage_data, dict):
                    voltage_data['time'] = current_time
                    voltage_timeseries.append(voltage_data.copy())
                    print(f"Time: {current_time} | Voltages: {voltage_data.get('S742b', 'NA')}")
            else:
                print(f"[WARN] Malformed voltage string: '{voltage_str}'")
        except Exception as e:
            print(f"[ERROR] Failed to receive voltage data: {e}")

    h.helicsFederateFinalize(fed)
    print("[Voltage Consumer Federate] Finalized.")

    try:
        voltage_df = pd.DataFrame(voltage_timeseries)
        voltage_df.to_csv("voltage_timeseries.csv", index=False)
        print("[Voltage Data] Saved to 'voltage_timeseries.csv'")
    except Exception as e:
        print(f"[ERROR] Could not save voltage data: {e}")

# Start federates
opendss_thread = threading.Thread(target=run_opendss_federate)
consumer_thread = threading.Thread(target=run_voltage_consumer_federate)

opendss_thread.start()
time.sleep(1.0)
consumer_thread.start()

opendss_thread.join()
consumer_thread.join()

print("Simulation complete. Broker closed.")

if 'broker' in globals() and h.helicsBrokerIsConnected(broker):
    h.helicsBrokerDisconnect(broker)
    h.helicsBrokerFree(broker)
