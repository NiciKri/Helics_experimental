import threading
import time
import os
import helics as h
import pandas as pd
import config  # Import the configuration

# Import data loading utility
from utils import load_solar_data, load_load_data, load_breaking_points

# Custom federates
from federates import opendss_federate, voltage_consumer_federate, inverter_federate, attack_federate, logger_federate

# Controllers
from Controllers import adaptive_controller_federate, DRL_controller_federate, ASRController

"""Simple code to run the simulation with all federates. Configurations are set in config.py."""

# =============================================================================
# Working directory & data loading
# =============================================================================
os.chdir(config.BASE_DIR)

# Solar data
solar_data = load_solar_data(config.DATA_DIR, config.solar_scaling_factor, config.start_time)
node_names = [col for col in solar_data.columns if col != 'time']
max_solar = solar_data[node_names].max()
max_solar_df = pd.DataFrame([max_solar])
output_csv_path = os.path.join(config.DATA_DIR, "max_solar_production.csv")
max_solar_df.to_csv(output_csv_path, index=False)
sbar_df = pd.read_csv(output_csv_path)

# Load data
load_data = load_load_data(config.DATA_DIR, config.load_scaling_factor, config.start_time)

# Breakpoints data
breaking_points = load_breaking_points(config.DATA_DIR)

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

hacks_list = config.hacks_list

# =============================================================================
# HELICS Broker Setup
# =============================================================================
def start_broker():
    global broker
    broker = h.helicsCreateBroker("zmq", "", f"--federates=6 --loglevel=warning")

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

# Adaptive Controller
adaptive_controller_thread = threading.Thread(
    target=adaptive_controller_federate.run_adaptive_controller_federate,
    args=(breaking_points, node_names, config.SIMULATION_TIME, config.TIME_STEP)
)
"""adaptive_controller_thread = threading.Thread(
    target=DRL_controller_federate.run_DRL_controller_federate,
    args=(breaking_points, node_names, config.SIMULATION_TIME, config.TIME_STEP)
)"""
"""adaptive_controller_thread = threading.Thread(
    target=ASRController.run_DRL_policy_federate,
    args=(config.SIMULATION_TIME, config.TIME_STEP, "final_theta.npy")
)"""

# Inverter federate
inverter_thread = threading.Thread(
    target=inverter_federate.run_inverter_federate,
    args=(node_names, config.SIMULATION_TIME, config.TIME_STEP, breaking_points, sbar_df)
)

logger_thread = threading.Thread(
    target=logger_federate.run_logging_federate,
    args=(config.SIMULATION_TIME, config.TIME_STEP)
)


# Start in sequence
consumer_thread.start()
time.sleep(1.0)
opendss_thread.start()
time.sleep(0.5)
attack_thread.start()
time.sleep(0.5)
adaptive_controller_thread.start()
time.sleep(0.5)
inverter_thread.start()
time.sleep(0.5)
logger_thread.start()
print("All federates started.")


# Wait for completion
consumer_thread.join()
opendss_thread.join()
attack_thread.join()
adaptive_controller_thread.join()
inverter_thread.join()
logger_thread.join() # INFO: I don't know why this is needed, but it doesn't work without it
print("Logger thread completed.")


# Shutdown broker
if 'broker' in globals() and h.helicsBrokerIsConnected(broker):
    h.helicsBrokerDisconnect(broker)
    h.helicsBrokerFree(broker)

print("Simulation complete. Broker closed.")
