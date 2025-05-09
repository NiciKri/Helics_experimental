import helics as h
import pandas as pd
import time
import os
import config  # Assumes config.py is in the same directory or in PYTHONPATH

# List of publications to subscribe to (fully-qualified names)
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


def run_logging_federate(simulation_time=config.SIMULATION_TIME, time_step=config.TIME_STEP, save_flag=config.SAVE_LOGS):
    # --- HELICS setup ---
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Logging_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, time_step)

    fed = h.helicsCreateValueFederate("Logging_Federate", fedinfo)

    # Register subscriptions
    subscriptions = {}
    for topic in PUB_TOPICS:
        subscriptions[topic] = h.helicsFederateRegisterSubscription(fed, topic, "")

    h.helicsFederateEnterExecutingMode(fed)

    records = []
    current_time = 0.0
    while current_time < simulation_time:
        # Request next time step
        next_time = current_time + time_step
        granted_time = h.helicsFederateRequestTime(fed, next_time)
        current_time = granted_time

        # Collect published values
        entry = {"time": current_time}
        for topic, sub in subscriptions.items():
            if h.helicsInputIsUpdated(sub):
                try:
                    # Use GetString for all types; booleans will convert to string
                    val = h.helicsInputGetString(sub)
                except Exception:
                    val = None
            else:
                val = None
            entry[topic] = val
        records.append(entry)

    # Finalize
    if save_flag:
        df = pd.DataFrame(records)
        log_dir = os.path.join(config.DATA_DIR, "logs")
        os.makedirs(log_dir, exist_ok=True)
        out_path = os.path.join(log_dir, "combined_log.csv")
        df.to_csv(out_path, index=False)
        print(f"[Logging Federate] Saved log to {out_path}")

    h.helicsFederateDisconnect(fed)
    h.helicsFederateFinalize(fed)
    print("[Logging Federate] Finalized.")
