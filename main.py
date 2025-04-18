import threading
import time
import os
import helics as h
import pandas as pd
import config  # Import the configuration

# Custom federates
from federates import opendss_federate, voltage_consumer_federate, inverter_federate, attack_federate

# =============================================================================
# Working directory & data loading
# =============================================================================
os.chdir(config.BASE_DIR)

# Solar data
solar_data = pd.read_csv(f"{config.DATA_DIR}/solar_data.csv")
solar_data.columns = solar_data.columns.str.replace('_pv$', '', regex=True)
solar_data.columns = solar_data.columns.str.replace('S', 's')
solar_data['time'] = solar_data.index

# Max solar production per node -> sbar_df
node_names = [col for col in solar_data.columns if col != 'time']
max_solar = solar_data[node_names].max()
max_solar_df = pd.DataFrame([max_solar])
output_csv_path = os.path.join(config.DATA_DIR, "max_solar_production.csv")
max_solar_df.to_csv(output_csv_path, index=False)
sbar_df = pd.read_csv(output_csv_path)

# Load data
load_data = pd.read_csv(f"{config.DATA_DIR}/load_data.csv")
load_data.columns = load_data.columns.str.replace('S', 's')
load_data['time'] = load_data.index
load_data.sort_values('time', inplace=True)

# Breakpoints data
breaking_points = pd.read_csv(f"{config.DATA_DIR}/solar_VV_breakpoints.csv")
breaking_points.columns = breaking_points.columns.str.replace('_pv$', '', regex=True)
breaking_points.columns = breaking_points.columns.str.replace('S', 's')

# =============================================================================
# Define attacks
# =============================================================================
# List of hack definitions: [start_time, end_time, hack_pct, bp_override, devices]
# - start_time (int or None)
# - end_time (int or None)
# - hack_pct (float between 0 and 1, or None)
# - bp_override (list of 5 floats, float offset, or None)
# - devices (list of device names, int count, float fraction, or None)

# Example: attack all inverters from t=100 to t=200, X% capacity reduction, no explicit bp override
print("node names", node_names)
#hack_nodes = ["s701a", "s701b", "s701c", "s713a", "s713b", "s713c"]
hack_nodes = ["s701a", "s701b"]
#hack_nodes = node_names  # Uncomment to attack all nodes
#bp_override = [0.95, 0.95, 0.95, 0.95, 0.95]  # Example breakpoint override
bp_override = 0.5  # Example breakpoint override

hacks_list = [
    [100, 200, 0.8, bp_override, hack_nodes],
]

# =============================================================================
# HELICS Broker Setup
# =============================================================================
def start_broker():
    global broker
    broker = h.helicsCreateBroker("zmq", "", f"--federates=4 --loglevel=warning")

broker_thread = threading.Thread(target=start_broker, daemon=True)
broker_thread.start()
time.sleep(1)

# =============================================================================
# Launch federates
# =============================================================================

# Voltage consumer
consumer_thread = threading.Thread(
    target=voltage_consumer_federate.run_voltage_consumer_federate,
    args=(solar_data, load_data, node_names, config.SIMULATION_TIME, config.TIME_STEP)
)

# OpenDSS
opendss_thread = threading.Thread(
    target=opendss_federate.run_opendss_federate
)

# Attack federate
attack_thread = threading.Thread(
    target=attack_federate.run_attack_federate,
    args=(hacks_list, breaking_points, config.SIMULATION_TIME, config.TIME_STEP)
)

# Inverter federate
inverter_thread = threading.Thread(
    target=inverter_federate.run_inverter_federate,
    args=(node_names, config.SIMULATION_TIME, config.TIME_STEP, breaking_points, sbar_df)
)

# Start in sequence
consumer_thread.start()
time.sleep(1.0)
opendss_thread.start()
time.sleep(0.5)
attack_thread.start()
time.sleep(0.5)
inverter_thread.start()

# Wait for completion
consumer_thread.join()
opendss_thread.join()
attack_thread.join()
inverter_thread.join()

# Shutdown broker
if 'broker' in globals() and h.helicsBrokerIsConnected(broker):
    h.helicsBrokerDisconnect(broker)
    h.helicsBrokerFree(broker)

print("Simulation complete. Broker closed.")
