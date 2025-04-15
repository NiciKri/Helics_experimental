import threading
import time
import os
import helics as h
import pandas as pd
import config  # Import the configuration

# Set the working directory using the configuration
os.chdir(config.BASE_DIR)

# Import federates from the package
from federates import opendss_federate, voltage_consumer_federate, inverter_federate

# =============================================================================
# Data Loading
# =============================================================================
# Import solar production data. Remove the '_pv' suffix if present and 
# replace all occurrences of capital "S" with lower-case "s".
solar_data = pd.read_csv(f"{config.DATA_DIR}/solar_data.csv")
solar_data.columns = solar_data.columns.str.replace('_pv$', '', regex=True)
solar_data.columns = solar_data.columns.str.replace('S', 's')
solar_data['time'] = solar_data.index

# Compute the maximum solar production for each node.
# Assume node names are the columns in solar_data except for 'time'
node_names = [col for col in solar_data.columns if col != 'time']
max_solar = solar_data[node_names].max()

# Create a single-row DataFrame where column names are the node names
# and the single row contains the max solar production (used as SBAR per node).
max_solar_df = pd.DataFrame([max_solar])

# Save this DataFrame to a CSV file in the data folder.
output_csv_path = os.path.join(config.DATA_DIR, "max_solar_production.csv")
max_solar_df.to_csv(output_csv_path, index=False)
print("Max solar production per node saved to", output_csv_path)

# Immediately read back the file to create the sbar_df DataFrame.
sbar_df = pd.read_csv(output_csv_path)

# Import load data. Convert any capital "S" in the column names to lower-case.
load_data = pd.read_csv(f"{config.DATA_DIR}/load_data.csv")
load_data.columns = load_data.columns.str.replace('S', 's')
load_data['time'] = load_data.index
load_data.sort_values('time', inplace=True)

# Import the solar voltage breakpoints data and convert all capital "S" to lower-case.
breaking_points = pd.read_csv(f"{config.DATA_DIR}/solar_VV_breakpoints.csv")
breaking_points.columns = breaking_points.columns.str.replace('_pv$', '', regex=True)
breaking_points.columns = breaking_points.columns.str.replace('S', 's')

# =============================================================================
# HELICS Broker Setup
# =============================================================================
def start_broker():
    global broker
    broker = h.helicsCreateBroker("zmq", "", "--federates=3 --loglevel=warning")

broker_thread = threading.Thread(target=start_broker, daemon=True)
broker_thread.start()
time.sleep(1)  # Allow broker to initialize

# =============================================================================
# Running the Federates
# =============================================================================
# Launch the voltage consumer federate in its own thread.
consumer_thread = threading.Thread(
    target=voltage_consumer_federate.run_voltage_consumer_federate,
    args=(solar_data, load_data, node_names, config.SIMULATION_TIME, config.TIME_STEP)
)

# Launch the OpenDSS federate in its own thread.
opendss_thread = threading.Thread(target=opendss_federate.run_opendss_federate)

# Launch the inverter federate in its own thread.
# Pass both the breakpoints DataFrame and the sbar_df (node-specific SBAR values).
inverter_thread = threading.Thread(
    target=inverter_federate.run_inverter_federate,
    args=(node_names, config.SIMULATION_TIME, config.TIME_STEP, breaking_points, sbar_df)
)

# Start federates.
consumer_thread.start()
time.sleep(1.0)  # Ensure the consumer starts publishing before OpenDSS starts.
opendss_thread.start()
time.sleep(0.5)  # Optional delay for proper initialization.
inverter_thread.start()

# Wait for all federate threads to complete.
consumer_thread.join()
opendss_thread.join()
inverter_thread.join()

# =============================================================================
# Shutdown Broker
# =============================================================================
if 'broker' in globals() and h.helicsBrokerIsConnected(broker):
    h.helicsBrokerDisconnect(broker)
    h.helicsBrokerFree(broker)

print("Simulation complete. Broker closed.")
