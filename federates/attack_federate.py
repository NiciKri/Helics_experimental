import helics as h
import pandas as pd
import time
import random
import config  # your simulation settings


def run_attack_federate(hacks: list,
                         pv_device_list: list,
                         simulation_time: float,
                         time_step: float = 1.0):
    """
    A HELICS federate that publishes dynamic attack breakpoints.

    hacks: list of [start_time, end_time, percent, breakpoints, device_list]
    pv_device_list: list of all PV device names
    """
    # --- 1. Preprocess hack definitions ---
    start_time = 0.0
    end_time = simulation_time
    duration = end_time - start_time
    for hack in hacks:
        # Ensure hack times
        if not hack[0] and not hack[1]:  # neither provided
            hack[0] = start_time + round(duration * 0.25)
            hack[1] = start_time + round(duration * 0.75)
        elif not hack[0] and hack[1]:    # only end provided
            hack[0] = random.randint(int(start_time + 10), int(hack[1] - 10))
        elif hack[0] and not hack[1]:    # only start provided
            hack[1] = random.randint(int(hack[0] + 10), int(end_time - 10))
        # Validate
        if hack[0] < start_time or hack[1] > end_time:
            raise ValueError("Hack start/end must be within simulation time range")
        # Percent capacity compromised
        if not hack[2]:
            hack[2] = round(random.uniform(0.05, 0.40), 2)
        # Devices to attack
        if not hack[4]:
            hack[4] = pv_device_list.copy()
        elif isinstance(hack[4], float):
            n = round(hack[4] * len(pv_device_list))
            hack[4] = random.sample(pv_device_list, n)
        elif isinstance(hack[4], int):
            hack[4] = random.sample(pv_device_list, hack[4])
        elif isinstance(hack[4], list):
            for device in hack[4]:
                if device not in pv_device_list:
                    raise ValueError(f"Invalid device '{device}' in hack list")

    # --- 2. Create federate info and publication ---
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Attack_Federate")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(
        fedinfo,
        h.HELICS_PROPERTY_TIME_DELTA,
        time_step
    )
    fed = h.helicsCreateValueFederate("Attack_Federate", fedinfo)
    pub = h.helicsFederateRegisterPublication(
        fed,
        "Attack_Federate/breakpoints",
        h.HELICS_DATA_TYPE_STRING,
        ""
    )

    # --- 3. Enter execution ---
    h.helicsFederateEnterExecutingMode(fed)

    current_time = start_time
    while current_time < end_time:
        # Determine active hacks at this time
        payload = {}
        for hack in hacks:
            if hack[0] <= current_time < hack[1]:
                # build breakpoint mapping
                bps = hack[3]  # list of breakpoints or None for adaptive
                for dev in hack[4]:
                    payload[dev] = bps
        # Publish current breakpoints
        h.helicsPublicationPublishString(pub, str(payload))
        print(f"[Attack] t={current_time} | Active hacks: {len(payload)} devices")

        # Advance time
        next_time = current_time + time_step
        granted = h.helicsFederateRequestTime(fed, next_time)
        current_time = granted

    # --- 4. Finalize ---
    h.helicsFederateFinalize(fed)
    print("[Attack Federate] Finalized.")


if __name__ == "__main__":
    # Example usage: load hacks from CSV or config
    # CSV expected columns: start, end, percent, bp1..bpN, devices (comma-separated)
    hack_df = pd.read_csv(f"{config.DATA_DIR}/attack_hacks.csv")
    hacks = []
    for _, row in hack_df.iterrows():
        devices = []
        if pd.notna(row.get('devices')):
            devices = [d.strip() for d in row['devices'].split(',')]
        # breakpoints assumed in columns bp1, bp2, ... collect into list
        bp_cols = [c for c in hack_df.columns if c.startswith('bp')]
        bps = row[bp_cols].dropna().tolist() or None
        hacks.append([
            row.get('start', None),
            row.get('end', None),
            row.get('percent', None),
            bps,
            devices
        ])
    # All PV devices must be provided (could read from a list file)
    all_devices = pd.read_csv(f"{config.DATA_DIR}/pv_devices.csv")["device_id"].tolist()

    run_attack_federate(hacks, all_devices,
                         simulation_time=config.SIMULATION_TIME,
                         time_step=config.TIME_STEP)
