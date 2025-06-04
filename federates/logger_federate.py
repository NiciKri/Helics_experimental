import helics as h  # Import the HELICS library for co-simulation
import pandas as pd  # Import pandas for DataFrame creation and CSV export
import time  # Import time module (not used directly, but could be useful for timing)
import os  # Import os module for file and directory operations
import config  # Assumes config.py defines SIMULATION_TIME, TIME_STEP, DATA_DIR, and SAVE_LOGS

# List of fully-qualified publication topics that this federate will subscribe to
PUB_TOPICS = [
    "Voltage_Consumer_Federate/load",
    "Voltage_Consumer_Federate/solar",
    "OpenDSS_Federate/voltage_out",
    "Attack_Federate/breakpoints_attack",
    "Attack_Federate/healthy_breakpoints",
    "Attack_Federate/attack_flag",
    "Adaptive_Controller_Federate/adaptive_breakpoints",
    "Inverter_Federate/injections"
]


def run_logging_federate(simulation_time=config.SIMULATION_TIME,
                         time_step=config.TIME_STEP,
                         save_flag=config.SAVE_LOGS):
    """
    Run a HELICS value federate that subscribes to a set of topics, logs all
    received values over time, and optionally saves the combined log to CSV.

    Parameters:
    - simulation_time: Total simulation duration (from config.SIMULATION_TIME)
    - time_step: Time increment for each HELICS time request (from config.TIME_STEP)
    - save_flag: Boolean flag indicating whether to save logs to disk (from config.SAVE_LOGS)
    """
    # --- HELICS setup ---
    fedinfo = h.helicsCreateFederateInfo()  # Create a FederateInfo object
    h.helicsFederateInfoSetCoreName(fedinfo, "Logging_Federate")  # Name this federate
    # Use ZeroMQ as the core type for message passing
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    # Set the time delta property so HELICS knows the desired time step
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, time_step)

    # Create a value federate with the given name and federate info
    fed = h.helicsCreateValueFederate("Logging_Federate", fedinfo)

    # Register subscriptions for each topic in PUB_TOPICS
    subscriptions = {}
    for topic in PUB_TOPICS:
        # The second argument "" means we accept any data type (string by default)
        subscriptions[topic] = h.helicsFederateRegisterSubscription(fed, topic, "")

    # Tell HELICS to enter execution mode (start the actual simulation)
    h.helicsFederateEnterExecutingMode(fed)

    records = []  # List to store each time-step's collected data as a dict
    current_time = 0.0  # Initialize simulation time

    # Main loop: advance time in increments of time_step until simulation_time
    while current_time < simulation_time:
        # Request the next time step from HELICS
        next_time = current_time + time_step
        granted_time = h.helicsFederateRequestTime(fed, next_time)
        current_time = granted_time  # HELICS may grant a time equal to or beyond next_time

        # Prepare a dictionary entry for this time step, starting with the time value
        entry = {"time": current_time}

        # Iterate over all subscriptions and collect the latest published value if updated
        for topic, sub in subscriptions.items():
            if h.helicsInputIsUpdated(sub):
                try:
                    # Retrieve the value as a string; booleans or other types convert to string
                    val = h.helicsInputGetString(sub)
                except Exception:
                    # If any error occurs in retrieving the value, set it to None
                    val = None
            else:
                # If the subscription was not updated since the last request, set to None
                val = None
            entry[topic] = val  # Store the value (or None) under the topic key

        records.append(entry)  # Append this time step's data to the records list

    # After exiting the time loop, optionally save logs to CSV
    if save_flag:
        # Convert the list of dictionaries to a pandas DataFrame
        df = pd.DataFrame(records)
        # Construct the directory path where logs will be stored: <DATA_DIR>/logs
        log_dir = os.path.join(config.DATA_DIR, "logs")
        os.makedirs(log_dir, exist_ok=True)  # Create the directory if it doesn't exist
        out_path = os.path.join(log_dir, "combined_log.csv")  # Full path to output CSV
        df.to_csv(out_path, index=False)  # Write DataFrame to CSV without row indices
        print(f"[Logging Federate] Saved log to {out_path}")

    # Disconnect and free the federate resources
    h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
    print("[Logging Federate] Finalized.")
