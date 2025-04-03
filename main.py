import threading
import time
import os
import helics as h
import pandas as pd
import config  # Import your configuration

# Set the working directory using the configuration
os.chdir(config.BASE_DIR)

# Import federates from the package
from federates import opendss_federate, voltage_consumer_federate

# =============================================================================
# Data Loading
# =============================================================================
solar_data = pd.read_csv(f"{config.DATA_DIR}/solar_data.csv")
solar_data.columns = solar_data.columns.str.replace('_pv$', '', regex=True)
solar_data['time'] = solar_data.index

load_data = pd.read_csv(f"{config.DATA_DIR}/load_data.csv")
load_data['time'] = load_data.index
load_data.sort_values('time', inplace=True)

# Assume node names are the columns in solar_data except for 'time'
node_names = [col for col in solar_data.columns if col != 'time']

# =============================================================================
# HELICS Broker Setup
# =============================================================================
def start_broker():
    global broker
    broker = h.helicsCreateBroker("zmq", "", "--federates=2 --loglevel=warning")

broker_thread = threading.Thread(target=start_broker, daemon=True)
broker_thread.start()
time.sleep(1)  # Allow broker to initialize

# =============================================================================
# Running the Federates
# =============================================================================
# Start the voltage consumer federate (which uses the CSV data) in its own thread.
consumer_thread = threading.Thread(
    target=voltage_consumer_federate.run_voltage_consumer_federate,
    args=(solar_data, load_data, node_names, config.SIMULATION_TIME, config.TIME_STEP)
)

# Start the OpenDSS federate in its own thread.
opendss_thread = threading.Thread(target=opendss_federate.run_opendss_federate)

consumer_thread.start()
time.sleep(1.0)  # Ensure the consumer is publishing before OpenDSS starts
opendss_thread.start()

consumer_thread.join()
opendss_thread.join()

# =============================================================================
# Shutdown Broker
# =============================================================================
if 'broker' in globals() and h.helicsBrokerIsConnected(broker):
    h.helicsBrokerDisconnect(broker)
    h.helicsBrokerFree(broker)

print("Simulation complete. Broker closed.")
