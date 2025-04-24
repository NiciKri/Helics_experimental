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
      { node: [ {"pct": <fraction>, "bp": [...]}, ... ] }
    Segments list always has length = 1 (healthy) + len(hacks): inactive hacks have pct=0.
    """
    # --- HELICS setup ---
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Attack_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, time_step)

    fed = h.helicsCreateValueFederate("Attack_Federate", fedinfo)
    pub = h.helicsFederateRegisterPublication(fed, "breakpoints_attack", h.HELICS_DATA_TYPE_STRING, "")
    h.helicsFederateEnterExecutingMode(fed)

    # Precompute PV devices and original bps
    pv_devices = [col.strip().lower() for col in breakpoints_df.columns] if breakpoints_df is not None else []
    node_bps = {}
    if breakpoints_df is not None:
        for col in breakpoints_df.columns:
            vals = breakpoints_df[col].dropna().tolist()
            node_bps[col.strip().lower()] = [float(v) for v in vals] if len(vals) >= 5 else DEFAULT_CONTROL_SETTING

    num_hacks = len(hacks)
    current_time = 0.0

    while current_time < simulation_time:
        attack_msg = {}
        for node in pv_devices:
            orig_bp = node_bps.get(node, DEFAULT_CONTROL_SETTING)
            remaining = 1.0
            hack_segments = []
            # Build one segment per hack definition
            for start, end, pct, bp_override, devices in hacks:
                active = (start <= current_time < end + 1) and (node in [d.lower() for d in devices])
                if active:
                    seg_pct = round(pct * remaining, 4)
                    # determine breakpoint list for this hack
                    if isinstance(bp_override, list):
                        bp_list = [float(v) for v in bp_override]
                    elif isinstance(bp_override, numbers.Number):
                        bp_list = [v + float(bp_override) for v in orig_bp]
                    else:
                        bp_list = orig_bp
                    remaining *= (1.0 - pct)
                else:
                    seg_pct = 0.0
                    bp_list = orig_bp
                hack_segments.append({"pct": seg_pct, "bp": bp_list})
            # Healthy segment first
            healthy_seg = {"pct": round(remaining, 4), "bp": orig_bp}
            segments = [healthy_seg] + hack_segments
            attack_msg[node] = segments

        print(f"[Attack Federate] t={current_time:.1f} → {attack_msg}")
        h.helicsPublicationPublishString(pub, str(attack_msg))

        # Advance time
        next_time = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_time)

    h.helicsFederateDisconnect(fed)
    h.helicsFederateFinalize(fed)
    print("[Attack Federate] Finalized.")
