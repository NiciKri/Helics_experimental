import helics as h  # Import the HELICS library for co-simulation
import pandas as pd  # Import pandas for DataFrame creation and CSV export
import time  # Import time module for sleeping/pause functionality

# Helper function to convert OpenDSS bus-phase names (e.g., "bus1a") to CSV naming convention (e.g., "Bus1A")
def dss_to_csv_name(dss_name):
    return dss_name.capitalize()


# Retrieve a dictionary of values from a DataFrame at a specific time t.
# If t exactly matches a row's 'time' value, return that row (excluding the 'time' column).
# Otherwise, return the last row in the DataFrame.
def get_values_at_time(t, df):
    if t in df['time'].values:
        row = df[df['time'] == t].iloc[0]
    else:
        row = df.iloc[-1]
    return row.drop('time').to_dict()


def run_voltage_consumer_federate(solar_data, load_data, node_names, simulation_time, time_step=1.0):
    """
    Create and run the Voltage Consumer Federate that:
    - Publishes load and solar data at each time step.
    - Subscribes to voltage output from the OpenDSS Federate.
    - Converts received voltage data to CSV naming convention and stores a time series.
    
    Parameters:
    - solar_data: pandas DataFrame containing solar injection time series (columns: 'time' + node columns)
    - load_data: pandas DataFrame containing load demand time series (columns: 'time' + node columns)
    - node_names: List of node names (not directly used here, but could validate data)
    - simulation_time: Total duration of simulation (in same units as DataFrames' 'time' column)
    - time_step: Time increment for HELICS time requests (default: 1.0)
    """

    # --- HELICS setup ---
    fedinfo = h.helicsCreateFederateInfo()  # Create a FederateInfo object for configuration
    h.helicsFederateInfoSetCoreName(fedinfo, "Voltage_Consumer_Federate")  # Name this federate
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")  # Use ZeroMQ as the core type
    # Set the time delta property so HELICS knows the time step for this federate
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, time_step)

    # Create a value federate with the given name and configuration
    fed = h.helicsCreateValueFederate("Voltage_Consumer_Federate", fedinfo)

    # Register a publication for load data (will be published as string-encoded dict)
    pub_load = h.helicsFederateRegisterPublication(fed, "load", h.HELICS_DATA_TYPE_STRING, "")
    # Register a publication for solar data (also string-encoded dict)
    pub_solar = h.helicsFederateRegisterPublication(fed, "solar", h.HELICS_DATA_TYPE_STRING, "")
    # Register a subscription to receive voltage output from the OpenDSS Federate
    sub = h.helicsFederateRegisterSubscription(fed, "OpenDSS_Federate/voltage_out", "")

    # Enter execution mode to begin time advancement
    h.helicsFederateEnterExecutingMode(fed)
    time.sleep(1)  # Pause briefly to ensure other federates' publications are ready

    current_time = 0  # Initialize simulation time
    voltage_timeseries = []  # List to store received voltage data over time

    # Main loop: advance time until simulation_time
    while current_time < simulation_time:
        # Retrieve solar and load values for the current time (as dicts)
        solar_values = get_values_at_time(current_time, solar_data)
        load_values = get_values_at_time(current_time, load_data)

        # Publish the load and solar dictionaries as strings
        h.helicsPublicationPublishString(pub_load, str(load_values))
        h.helicsPublicationPublishString(pub_solar, str(solar_values))

        # Request the next time step from HELICS
        next_time = current_time + time_step
        granted_time = h.helicsFederateRequestTime(fed, next_time)
        current_time = granted_time  # Update current time to the granted HELICS time

        # Wait up to ~1 second (100 × 0.01 s) for a voltage update
        voltage_timeout = 0
        while not h.helicsInputIsUpdated(sub) and voltage_timeout < 100:
            time.sleep(0.01)
            voltage_timeout += 1

        # If an update is available, retrieve and process it
        if h.helicsInputIsUpdated(sub):
            voltage_str = h.helicsInputGetString(sub)
            # Expecting a dictionary literal as a string, e.g., "{'bus1a': 1.02, 'bus1b': 1.01, ...}"
            if voltage_str.strip().startswith('{'):
                try:
                    voltage_data = eval(voltage_str)  # Convert string to Python dict
                    if isinstance(voltage_data, dict):
                        # Convert each OpenDSS bus-phase key to CSV naming convention
                        voltage_data_csv = {
                            dss_to_csv_name(key): value
                            for key, value in voltage_data.items()
                        }
                        voltage_data_csv['time'] = current_time  # Add time stamp to the dict
                        # Append a copy of this time step’s voltage data to the list
                        voltage_timeseries.append(voltage_data_csv.copy())
                    else:
                        print(f"[WARN] Received non-dict voltage data: {voltage_data}")
                except Exception as e:
                    print(f"[ERROR] Failed to evaluate voltage data: {e}")
            else:
                print(f"[WARN] Malformed voltage string: '{voltage_str}'")
        else:
            # No voltage update received this time step; skip to next iteration
            continue

    # --- Finalize HELICS federate ---
    h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
    #print("[Voltage Consumer Federate] Finalized.")

    # --- Save collected voltage time series to CSV ---
    try:
        voltage_df = pd.DataFrame(voltage_timeseries)  # Convert list of dicts to DataFrame
        voltage_df.to_csv("voltage_timeseries.csv", index=False)  # Write DataFrame to CSV
        #print("[Voltage Data] Saved to 'voltage_timeseries.csv'")
    except Exception as e:
        print(f"[ERROR] Could not save voltage data: {e}")
