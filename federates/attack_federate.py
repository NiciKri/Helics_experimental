import helics as h
import time
import numbers
import random

DEFAULT_CONTROL_SETTING = [0.98, 1.01, 1.02, 1.05, 1.07]

def run_attack_federate(hacks, breakpoints_df, simulation_time, time_step):
    """
    hacks: list of [start, end, hack_pct, bp_override, devices]
    breakpoints_df: DataFrame with lowercase node names as columns and 5 numeric entries each
    Publishes at each time step a dict:
      { node: [ {"pct": <fraction>, "bp": [...]} , ... ] }
    where the first entry is always the healthy segment, followed by each hack contribution.
    """
    # --- HELICS setup ---
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Attack_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, time_step)

    fed = h.helicsCreateValueFederate("Attack_Federate", fedinfo)
    pub = h.helicsFederateRegisterPublication(fed, "breakpoints_attack", h.HELICS_DATA_TYPE_STRING, "")
    h.helicsFederateEnterExecutingMode(fed)

    # Time bounds and device list
    start_time = 0
    end_time = simulation_time
    duration = end_time - start_time
    pv_devices = []
    if breakpoints_df is not None:
        pv_devices = [col.strip().lower() for col in breakpoints_df.columns]

    # Fill defaults for hacks
    for hack in hacks:
        # start/end times
        if hack[0] is None and hack[1] is None:
            hack[0] = start_time + round(duration * 0.25)
            hack[1] = start_time + round(duration * 0.75)
        elif hack[0] is None:
            hack[0] = random.randint(start_time + 1, hack[1] - 1)
        elif hack[1] is None:
            hack[1] = random.randint(hack[0] + 1, end_time - 1)
        # hack percentage
        if hack[2] is None:
            hack[2] = round(random.uniform(0.05, 0.40), 2)
        # target devices
        if hack[4] is None:
            hack[4] = pv_devices.copy()
        elif isinstance(hack[4], (int, float)):
            count = int(hack[4] * len(pv_devices)) if isinstance(hack[4], float) else hack[4]
            hack[4] = random.sample(pv_devices, count)

    current_time = 0.0
    while current_time < simulation_time:
        # Gather active hacks per node
        node_hacks = {}
        for start, end, pct, bp, devices in hacks:
            if start <= current_time < end+1:
                for dev in devices:
                    node = dev.replace("inverter_", "").lower()
                    # Determine this hack's breakpoints list
                    if isinstance(bp, list):
                        bp_list = [float(x) for x in bp]
                    elif isinstance(bp, numbers.Number):
                        orig = (breakpoints_df[node].dropna().tolist()
                                if node in breakpoints_df else DEFAULT_CONTROL_SETTING)
                        orig = [float(v) for v in orig] if len(orig) >= 5 else list(DEFAULT_CONTROL_SETTING)
                        bp_list = [v + float(bp) for v in orig]
                    else:
                        orig = (breakpoints_df[node].dropna().tolist()
                                if node in breakpoints_df else DEFAULT_CONTROL_SETTING)
                        bp_list = [float(v) for v in orig]
                    node_hacks.setdefault(node, []).append((start, pct, bp_list))

        attack_msg = {}
        # Build message with healthy first, then hacks
        for node, infos in node_hacks.items():
            infos.sort(key=lambda x: x[0])
            orig_bp = (breakpoints_df[node].dropna().tolist()
                       if node in breakpoints_df else DEFAULT_CONTROL_SETTING)
            orig_bp = [float(v) for v in orig_bp]
            hack_segments = []
            remaining = 1.0
            for _, pct, bp_list in infos:
                seg_pct = round(pct * remaining, 4)
                hack_segments.append({"pct": seg_pct, "bp": bp_list})
                remaining *= (1.0 - pct)
            healthy_pct = round(remaining, 4)
            healthy_segment = {"pct": healthy_pct, "bp": orig_bp}
            segments = [healthy_segment] + hack_segments
            attack_msg[node] = segments

        print(f"[Attack Federate] t={current_time:.1f} → {attack_msg}")
        h.helicsPublicationPublishString(pub, str(attack_msg))

        # Advance time
        next_time = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_time)

    h.helicsFederateDisconnect(fed)
    h.helicsFederateFinalize(fed)
    print("[Attack Federate] Finalized.")
