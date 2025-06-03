import helics as h
import time
import numbers
import random
from collections import deque

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
    attack_pub = h.helicsFederateRegisterPublication(fed, "breakpoints_attack", h.HELICS_DATA_TYPE_STRING, "")
    healthy_pub = h.helicsFederateRegisterPublication(fed, "healthy_breakpoints", h.HELICS_DATA_TYPE_STRING, "")
    #pub = h.helicsFederateRegisterPublication(fed, "breakpoints_attack_", h.HELICS_DATA_TYPE_STRING, "")
    #attack_flag_pub = h.helicsFederateRegisterPublication(fed, "attack_flag", h.HELICS_DATA_TYPE_STRING, "")
    attack_flag_pub = h.helicsFederateRegisterPublication(fed, "attack_flag", h.HELICS_DATA_TYPE_BOOLEAN, "")
    voltage_sub = h.helicsFederateRegisterSubscription(fed, "OpenDSS_Federate/voltage_out", "")
    h.helicsFederateEnterExecutingMode(fed)

    # Precompute PV devices and original bps
    pv_devices = [col.strip().lower() for col in breakpoints_df.columns]
    node_bps = {
        col.strip().lower(): (
            [float(v) for v in breakpoints_df[col].dropna().tolist()]
            if len(breakpoints_df[col].dropna()) >= 5
            else DEFAULT_CONTROL_SETTING
        )
        for col in breakpoints_df.columns
    }

    # Initialize per-node voltage history (deque maxlen=10)
    volt_hist_len = 50  # length of voltage history to consider for breakpoints
    volt_history = {node: deque(maxlen=volt_hist_len) for node in pv_devices}

    # --- Preprocessing of hack definitions ---
    start_time = 0.0
    end_time = float(simulation_time)
    duration = end_time - start_time
    pv_device_list = pv_devices.copy()

    for hack in hacks:
        # time defaults
        if hack[0] is None and hack[1] is None:
            hack[0] = start_time + round(duration * 0.25)
            hack[1] = start_time + round(duration * 0.75)
        elif hack[0] is None:
            hack[0] = random.randint(int(start_time + 10), int(hack[1] - 10))
        elif hack[1] is None:
            hack[1] = random.randint(int(hack[0] + 10), int(end_time - 10))
        if not (start_time <= hack[0] <= end_time) or not (start_time <= hack[1] <= end_time):
            raise ValueError("Hack start and end times must be within simulation start and end times")

        # percentage default
        if hack[2] is None:
            hack[2] = round(random.uniform(0.05, 0.40), 2)

        # devices default
        if hack[4] is None:
            hack[4] = pv_device_list.copy()
        elif isinstance(hack[4], float):
            hack[4] = random.sample(pv_device_list, round(hack[4] * len(pv_device_list)))
        elif isinstance(hack[4], int):
            hack[4] = random.sample(pv_device_list, hack[4])
        elif isinstance(hack[4], list):
            hack[4] = [d.lower() for d in hack[4]]
        else:
            raise ValueError("Invalid devices entry in hack definition")

    current_time = 0.0
    while current_time < simulation_time:
        # 1) Sync and collect voltage data
        voltage_data = {}
        timeout = 0
        while not h.helicsInputIsUpdated(voltage_sub) and timeout < 100:
            time.sleep(0.01)
            timeout += 1
        if h.helicsInputIsUpdated(voltage_sub):
            try:
                raw = h.helicsInputGetString(voltage_sub)
                voltage_data = eval(raw) if raw.strip().startswith('{') else {}
            except:
                voltage_data = {}
        # update history
        for node in pv_devices:
            if node in voltage_data:
                try:
                    v = float(voltage_data[node])
                    volt_history[node].append(v)
                except:
                    pass

        # 2) Build and publish attack message
        full_attack_msg = {}
        attack_msg = {}
        healthy_msg = {}
        attack_flag = False

        for node in pv_devices:
            orig_bp = node_bps[node]
            remaining = 1.0
            segments = []
            for start, end, pct, bp_override, devices in hacks:
                active = (start <= current_time < end + 1) and (node in devices)
                if active:
                    seg_pct = round(pct * remaining, 4)
                    # determine breakpoints for this segment
                    if isinstance(bp_override, list):
                        bp_list = bp_override
                    elif isinstance(bp_override, numbers.Number):
                        bp_list = [v + float(bp_override) for v in orig_bp]
                    elif bp_override is None:
                        hist = list(volt_history[node])
                        if len(hist) >= volt_hist_len:
                            avg_v = sum(hist[-volt_hist_len:]) / volt_hist_len
                        elif hist:
                            avg_v = sum(hist) / len(hist)
                        else:
                            avg_v = 1.0  # fallback nominal
                        bp_list = [
                            round(avg_v - 0.001, 4),
                            round(avg_v, 4),
                            round(avg_v + 0.001, 4),
                            round(avg_v + 0.002, 4),
                            round(avg_v + 0.003, 4)
                        ]
                    else:
                        bp_list = orig_bp
                    remaining *= (1.0 - pct)
                else:
                    seg_pct = 0.0
                    bp_list = orig_bp
                segments.append({"pct": seg_pct, "bp": bp_list})
            # healthy segment first
            healthy = {"pct": round(remaining, 4), "bp": orig_bp}
            if remaining < 1.0:
                attack_flag = True
            full_attack_msg[node] = [healthy] + segments
            healthy_msg[node] = [healthy]
            attack_msg[node] = segments
            # print messages for node s701a
            #if node == "s701a":
            #    print(f"[Time {current_time}] Node {node} segments:")
            #    print(f"  Healthy: {healthy}")
            #    print(f"  Attack: {segments}")
        

        #print(f"[Attack Federate] t={current_time:.1f} → {attack_msg}")
        h.helicsPublicationPublishString(healthy_pub, str(healthy_msg))
        h.helicsPublicationPublishString(attack_pub, str(attack_msg))
        #h.helicsPublicationPublishString(pub, str(full_attack_msg))

        # publish attack flag
        #h.helicsPublicationPublishString(attack_flag_pub, str(attack_flag))
        h.helicsPublicationPublishBoolean(attack_flag_pub, attack_flag)

        # 3) Advance time
        next_t = current_time + time_step
        current_time = h.helicsFederateRequestTime(fed, next_t)

    # teardown
    h.helicsFederateDisconnect(fed)
    h.helicsFederateFree(fed)
    print("[Attack Federate] Finalized.")
