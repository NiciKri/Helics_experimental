import helics as h
import pandas as pd
import time

# Helper function for converting DSS names to CSV convention.
def dss_to_csv_name(dss_name):
    return dss_name.capitalize()

def get_values_at_time(t, df):
    if t in df['time'].values:
        row = df[df['time'] == t].iloc[0]
    else:
        row = df.iloc[-1]
    return row.drop('time').to_dict()

def run_voltage_consumer_federate(solar_data, load_data, node_names, simulation_time, time_step=1.0):
    # HELICS setup
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Voltage_Consumer_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, time_step)

    fed = h.helicsCreateValueFederate("Voltage_Consumer_Federate", fedinfo)
    pub_load = h.helicsFederateRegisterPublication(fed, "load", h.HELICS_DATA_TYPE_STRING, "")
    pub_solar = h.helicsFederateRegisterPublication(fed, "solar", h.HELICS_DATA_TYPE_STRING, "")
    sub = h.helicsFederateRegisterSubscription(fed, "OpenDSS_Federate/voltage_out", "")

    h.helicsFederateEnterExecutingMode(fed)
    time.sleep(1)  # Ensure publisher is ready

    current_time = 0
    voltage_timeseries = []

    while current_time < simulation_time:
        # Publish load and solar data
        solar_values = get_values_at_time(current_time, solar_data)
        load_values = get_values_at_time(current_time, load_data)
        h.helicsPublicationPublishString(pub_load, str(load_values))
        h.helicsPublicationPublishString(pub_solar, str(solar_values))

        # Advance time
        next_time = current_time + time_step
        granted_time = h.helicsFederateRequestTime(fed, next_time)
        current_time = granted_time

        # Wait for a voltage update (up to ~1s)
        voltage_timeout = 0
        while not h.helicsInputIsUpdated(sub) and voltage_timeout < 100:
            time.sleep(0.01)
            voltage_timeout += 1

        # Only call GetString if we actually got an update
        if h.helicsInputIsUpdated(sub):
            voltage_str = h.helicsInputGetString(sub)
            if voltage_str.strip().startswith('{'):
                try:
                    voltage_data = eval(voltage_str)
                    if isinstance(voltage_data, dict):
                        voltage_data_csv = {dss_to_csv_name(key): value for key, value in voltage_data.items()}
                        voltage_data_csv['time'] = current_time
                        voltage_timeseries.append(voltage_data_csv.copy())
                    else:
                        print(f"[WARN] Received non-dict voltage data: {voltage_data}")
                except Exception as e:
                    print(f"[ERROR] Failed to evaluate voltage data: {e}")
            else:
                print(f"[WARN] Malformed voltage string: '{voltage_str}'")
        else:
            # No update this step → skip without grabbing any string
            continue

    # Finalize federate
    h.helicsFederateFinalize(fed)
    print("[Voltage Consumer Federate] Finalized.")

    # Save voltage timeseries
    try:
        voltage_df = pd.DataFrame(voltage_timeseries)
        voltage_df.to_csv("voltage_timeseries.csv", index=False)
        print("[Voltage Data] Saved to 'voltage_timeseries.csv'")
    except Exception as e:
        print(f"[ERROR] Could not save voltage data: {e}")
