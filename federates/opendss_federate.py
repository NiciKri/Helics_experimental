import helics as h  # Import the HELICS library for co-simulation
from opendssdirect import dss  # Import OpenDSSDirect for interacting with OpenDSS
import time  # Import time module for sleeping/pause functionality
import os  # Import os module for file and directory operations (not used directly here)
import config  # Import configuration parameters (e.g., TIME_STEP, SIMULATION_TIME, BASE_DIR)

# Function to convert a node name from CSV convention to OpenDSS convention.
# OpenDSS node names typically start with 'S', so we prepend 'S' if it’s missing,
# and return the lowercase form for consistency with OpenDSSDirect API.
def csv_to_dss_name(csv_name):
    if not csv_name.startswith('S') and not csv_name.startswith('s'):
        csv_name = 'S' + csv_name
    return csv_name.lower()


def run_opendss_federate():
    """
    Create and run an OpenDSS federate that:
    - Subscribes to net load updates from the Voltage Consumer Federate.
    - Subscribes to inverter injection updates from the Inverter Federate.
    - Publishes per-bus voltage results after solving the power flow in OpenDSS.
    """

    # --- HELICS Federate setup ---
    fedinfo = h.helicsCreateFederateInfo()  # Create a FederateInfo object to configure core settings
    h.helicsFederateInfoSetCoreName(fedinfo, "OpenDSS_Federate")  # Name this federate "OpenDSS_Federate"
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")  # Use ZeroMQ for message passing
    # Set the time step for HELICS requests (TIME_STEP from config)
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, config.TIME_STEP)

    # Create a value federate with the given name and configuration
    fed = h.helicsCreateValueFederate("OpenDSS_Federate", fedinfo)

    # Register a subscription to receive net load (kW) from Voltage_Consumer_Federate
    sub = h.helicsFederateRegisterSubscription(fed, "Voltage_Consumer_Federate/load", "")
    # Register a subscription to receive inverter injections from Inverter_Federate
    inverter_sub = h.helicsFederateRegisterSubscription(fed, "Inverter_Federate/injections", "")
    # Create a publication for voltage output (per-bus voltages) as a string
    pub = h.helicsFederateRegisterPublication(fed, "voltage_out", h.HELICS_DATA_TYPE_STRING, "")

    # Enter execution mode to begin the HELICS time-advancement loop
    h.helicsFederateEnterExecutingMode(fed)

    # --- OpenDSS model initialization ---
    # Load the IEEE 37-node test case by redirecting OpenDSS to the .dss file location
    dss.Command(f"Redirect {config.BASE_DIR}/data/ieee37.dss")
    # Print out all load names loaded into OpenDSS
    print("Loads in DSS after redirect:", dss.Loads.AllNames())
    # Print out all bus names in the circuit
    print("Buses in DSS:", dss.Circuit.AllBusNames())

    # Build a dictionary of each load’s initial reactive power (kVAR). 
    # This assumes each load is defined with kW and kVAR in the .dss file.
    initial_reactive = {}
    for load_name in dss.Loads.AllNames():
        dss.Loads.Name(load_name)  # Activate this load element in OpenDSS
        try:
            reactive_val = dss.Loads.kvar()  # Read the current kVAR value
        except Exception as e:
            # If the property is missing or inaccessible, log a warning and default to 0
            print(f"[WARN] Could not retrieve kvar for load {load_name}: {e}")
            reactive_val = 0
        initial_reactive[load_name] = reactive_val  # Store initial kVAR for this load

    # Initialize simulation time
    current_time = 0

    # --- Main HELICS time-advancement loop ---
    while current_time < config.SIMULATION_TIME:
        # Request the next simulation time step
        next_time = current_time + config.TIME_STEP
        granted_time = h.helicsFederateRequestTime(fed, next_time)
        # HELICS may grant a time >= requested time (aloowing for synchronization)
        
        # --- Retrieve net load from Voltage Consumer Federate ---
        timeout_counter = 0
        # Wait (with a small sleep) until the subscription is updated or timeout
        while not h.helicsInputIsUpdated(sub):
            time.sleep(0.01)  # Sleep 10 ms to avoid busy-waiting
            timeout_counter += 1
            if timeout_counter > 100:  # After ~1 second without update, warn and break
                print(f"[WARN] No load received at t={granted_time}")
                break

        # Get the load string (expected to be a dictionary literal, e.g., "{'bus1': 100, ...}")
        load_str = h.helicsInputGetString(sub)
        load = {}
        if load_str.strip().startswith('{'):
            try:
                load = eval(load_str)  # Convert string representation to a Python dict
            except Exception as e:
                print(f"[ERROR] Failed to parse load: {e}")
        else:
            print(f"[WARN] Invalid load string: '{load_str}'")

        # --- Retrieve inverter injections from Inverter Federate ---
        inverter_timeout = 0
        # Wait briefly for inverter injection data (up to ~1 second)
        while not h.helicsInputIsUpdated(inverter_sub) and inverter_timeout < 100:
            time.sleep(0.01)
            inverter_timeout += 1

        inverter_injections_str = h.helicsInputGetString(inverter_sub)
        inverter_injections = {}
        if inverter_injections_str.strip().startswith('{'):
            try:
                inverter_injections = eval(inverter_injections_str)  # Parse dict literal
            except Exception as e:
                print(f"[ERROR] Failed to parse inverter injections: {e}")
        else:
            print(f"[WARN] Invalid inverter injection string: '{inverter_injections_str}'")

        # --- Process each load: subtract inverter injections from net load ---
        print_flag = True  # Use this to print detailed info only once per time step
        for bus, kw in load.items():
            # Convert the bus name from CSV naming convention to OpenDSS naming
            dss_bus = csv_to_dss_name(bus)
            modified_kw = kw  # Default modified kW is the original net load
            # Get the original reactive load (kVAR) or default to 0 if not found
            modified_kvar = initial_reactive.get(dss_bus, 0)

            # If inverter has injections for this bus, adjust the load accordingly
            if dss_bus in inverter_injections:
                try:
                    p_inj = float(inverter_injections[dss_bus].get('p', 0))
                    q_inj = float(inverter_injections[dss_bus].get('q', 0))
                    modified_kw = kw - p_inj  # Subtract active power injection
                    modified_kvar = modified_kvar - q_inj  # Subtract reactive injection
                    if print_flag:
                        # Print details of the first bus processed this time step
                        print(
                            f"[OpenDSS] t={granted_time} | Node {dss_bus}: "
                            f"load={kw}, inverter p_injection={p_inj}, "
                            f"modified load={modified_kw}, inverter q_injection={q_inj}, "
                            f"modified kvar load={modified_kvar}"
                        )
                        print_flag = False  # Disable further printing this time step
                except Exception as e:
                    print(f"[ERROR] Error processing inverter injection for {dss_bus}: {e}")

            # If the load bus exists in the OpenDSS model, update its kW and kVAR
            if dss_bus in dss.Loads.AllNames():
                dss.Loads.Name(dss_bus)  # Activate that load element
                dss.Loads.kW(modified_kw)  # Set the new kW value
                try:
                    dss.Loads.kvar(modified_kvar)  # Update the reactive power
                except Exception as e:
                    print(f"[WARN] Unable to update kvar for {dss_bus}: {e}")
            else:
                # If the load bus is not present in OpenDSS, log and skip
                print(f"[INFO] Load {dss_bus} not found. Skipping.")

        # --- Solve the power flow in OpenDSS ---
        dss.Solution.Solve()

        # --- Collect per-bus voltage magnitudes (p.u.) and angles ---
        voltage_dict = {}
        bus_names = dss.Circuit.AllBusNames()  # Get all bus names in the circuit
        for bus in bus_names:
            dss.Circuit.SetActiveBus(bus)  # Activate the bus in OpenDSS
            # puVmagAngle returns a list: [VMag1, VAng1, VMag2, VAng2, ...]
            voltage_data = dss.Bus.puVmagAngle()
            num_phases = dss.Bus.NumNodes()  # Number of phase nodes at this bus
            for i in range(num_phases):
                # Assign a phase label 'a', 'b', 'c', etc., based on index
                phase_label = chr(ord('a') + i)
                key = bus.lower() + phase_label  # e.g. "bus1a", "bus1b"
                voltage_dict[key] = voltage_data[2 * i]  # Use the magnitude (even indices)

        # --- Publish the voltage dictionary as a string to HELICS ---
        h.helicsPublicationPublishString(pub, str(voltage_dict))

        # Advance current_time to the granted HELICS time
        current_time = granted_time

    # After simulation loop, disconnect and free the HELICS federate
    h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
    print("[OpenDSS Federate] Finalized.")
