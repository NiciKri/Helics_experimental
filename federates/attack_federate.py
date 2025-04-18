import helics as h
import time
import numbers

def run_attack_federate(hacks, breakpoints_df, simulation_time, time_step):
    """
    hacks: list of [start, end, hack_pct, bp_override, devices]
    breakpoints_df: DataFrame with lowercase node names as columns and 5 numeric entries each
    Publishes at each time step a dict:
      { node: { "bp": [...], "hack_pct": 0.2 } }
    """

    # --- HELICS setup ---
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Attack_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, time_step)

    fed = h.helicsCreateValueFederate("Attack_Federate", fedinfo)
    pub = h.helicsFederateRegisterPublication(fed, "breakpoints_attack", h.HELICS_DATA_TYPE_STRING, "")
    h.helicsFederateEnterExecutingMode(fed)

    # default volt-var breakpoints if none provided for a node
    DEFAULT_CONTROL_SETTING = [0.98, 1.01, 1.02, 1.05, 1.07]

    current_time = 0.0
    while current_time < simulation_time:
        attack_msg = {}

        for start, end, pct, bp, devices in hacks:
            if start <= current_time < end:
                for dev in devices:
                    node = dev.replace("inverter_", "").lower()
                    bp_list = None

                    if isinstance(bp, list):
                        # explicit list override
                        bp_list = [float(x) for x in bp]

                    elif isinstance(bp, numbers.Number):
                        # float/number override: offset the original breakpoints (or default if missing)
                        try:
                            # check if dataframe has 5 non-null entries for this node
                            col = breakpoints_df.get(node, None)
                            if col is not None and col.dropna().shape[0] >= 5:
                                orig = [float(v) for v in col.tolist()]
                            else:
                                orig = list(DEFAULT_CONTROL_SETTING)
                            bp_list = [v + float(bp) for v in orig]
                        except Exception as e:
                            print(f"[Attack Federate] ERROR computing float override for '{node}': {e}")
                            orig = list(DEFAULT_CONTROL_SETTING)
                            bp_list = [v + float(bp) for v in orig]

                    # else: bp_list stays None (no override)

                    attack_msg[node] = {
                        "bp": bp_list,
                        "hack_pct": float(pct) if isinstance(pct, numbers.Number) else pct
                    }

        # debug print to confirm exactly what we’re publishing
        print(f"[Attack Federate] t={current_time:.1f} → {attack_msg}")

        # publish (as string)
        h.helicsPublicationPublishString(pub, str(attack_msg))

        # advance time
        next_time = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_time)
        
    h.helicsFederateDisconnect(fed)
    h.helicsFederateFinalize(fed)
    print("[Attack Federate] Finalized.")
