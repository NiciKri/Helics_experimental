import helics as h
import time
import numbers
import random

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

    # --- Preprocess hack definitions ---
    start_time = 0
    end_time = simulation_time
    duration = end_time - start_time
    pv_device_list = []
    if breakpoints_df is not None:
        pv_device_list = [col.strip().lower() for col in breakpoints_df.columns]

    for hack in hacks:
        # Fill missing start/end times
        if not hack[0] and not hack[1]:
            hack[0] = start_time + round(duration * 0.25)
            hack[1] = start_time + round(duration * 0.75)
        elif not hack[0] and hack[1]:
            hack[0] = random.randint(start_time + 10, hack[1] - 10)
        elif hack[0] and not hack[1]:
            hack[1] = random.randint(hack[0] + 10, end_time - 10)
        # Validate timeline
        if hack[0] not in range(start_time, end_time) or hack[1] not in range(start_time, end_time) or hack[0] >= hack[1]:
            raise ValueError("Hack start and end times must be within simulation start and end times and start < end")
        # Default hack percentage
        if not hack[2]:
            hack[2] = round(random.uniform(0.05, 0.40), 2)
        # Target devices
        if not hack[4]:
            hack[4] = pv_device_list.copy()
        elif isinstance(hack[4], list):
            for device in hack[4]:
                if device.lower() not in pv_device_list:
                    raise ValueError(f"Invalid inverter: {device}")
        elif isinstance(hack[4], float):
            count = round(hack[4] * len(pv_device_list))
            hack[4] = count
        if isinstance(hack[4], int):
            hack[4] = random.sample(pv_device_list, hack[4])

    DEFAULT_CONTROL_SETTING = [0.98, 1.01, 1.02, 1.05, 1.07]
    current_time = 0.0

    while current_time < simulation_time:
        # Gather active hacks per node
        node_hacks = {}
        for start, end, pct, bp, devices in hacks:
            if start <= current_time < end:
                for dev in devices:
                    node = dev.replace("inverter_", "").lower()
                    # Compute breakpoint override list
                    bp_list = None
                    if isinstance(bp, list):
                        bp_list = [float(x) for x in bp]
                    elif isinstance(bp, numbers.Number):
                        try:
                            col = breakpoints_df.get(node, None)
                            if col is not None and col.dropna().shape[0] >= 5:
                                orig = [float(v) for v in col.tolist()]
                            else:
                                orig = list(DEFAULT_CONTROL_SETTING)
                            bp_list = [v + float(bp) for v in orig]
                        except Exception:
                            orig = list(DEFAULT_CONTROL_SETTING)
                            bp_list = [v + float(bp) for v in orig]
                    node_hacks.setdefault(node, []).append((start, pct, bp_list))

        # Resolve overlaps: sequential application for N hacks
        attack_msg = {}
        for node, infos in node_hacks.items():
            # sort by start time
            infos.sort(key=lambda x: x[0])
            # choose breakpoints from earliest hack that has one
            chosen_bp = None
            for _, _, bp_list in infos:
                if bp_list is not None:
                    chosen_bp = bp_list
                    break
            # combine hack percentages sequentially on remaining capacity
            combined_pct = 0.0
            remaining = 1.0
            for _, pct, _ in infos:
                combined_pct += pct * remaining
                remaining *= (1.0 - pct)
            attack_pct = round(combined_pct, 2)
            attack_msg[node] = {"bp": chosen_bp, "hack_pct": float(attack_pct)}

        # debug print
        print(f"[Attack Federate] t={current_time:.1f} → {attack_msg}")
        h.helicsPublicationPublishString(pub, str(attack_msg))

        # Advance time
        next_time = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_time)

    h.helicsFederateDisconnect(fed)
    h.helicsFederateFinalize(fed)
    print("[Attack Federate] Finalized.")
