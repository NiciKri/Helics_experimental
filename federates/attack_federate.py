import helics as h
import time

def run_attack_federate(hacks, breakpoints_df, simulation_time, time_step):
    """
    hacks: list of [start, end, hack_pct, bp_override, devices]
    breakpoints_df: original volt‑var breakpoints DataFrame (columns lowercase node names)
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

    current_time = 0.0
    while current_time < simulation_time:
        attack_msg = {}
        for start, end, pct, bp, devices in hacks:
            if start <= current_time < end:
                for dev in devices:
                    node = dev.replace("inverter_", "").lower()
                    # determine bp list for this node
                    if isinstance(bp, list):
                        bp_list = bp
                    elif isinstance(bp, float):
                        orig = breakpoints_df[node + "_pv"].tolist()
                        bp_list = [v + bp for v in orig]
                    else:
                        bp_list = None  # signal “adaptive” if you want
                    attack_msg[node] = {"bp": bp_list, "hack_pct": pct}

        # publish dict as string
        h.helicsPublicationPublishString(pub, str(attack_msg))

        # advance time
        next_time = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_time)

    h.helicsFederateFinalize(fed)
    print("[Attack Federate] Finalized.")
