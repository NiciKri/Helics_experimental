import config
import numpy as np
from Environment import DRLControllerEnv   # replace with your module path

def main():
    sim_time = config.SIMULATION_TIME
    dt = config.TIME_STEP

    env = DRLControllerEnv(simulation_time=sim_time, time_step=dt)

    num_runs = 2
    for run_idx in range(num_runs):
        print(f"\n=== Run #{run_idx + 1} ===")

        # Sample *one* 2‐dim action
        global_action = env.action_space.sample()
        print("  Global action (shape (2,)):", global_action)

        # Step runs a full simulation; inside, it will recompute per‐node actions
        final_obs, total_reward, done, truncated, info = env.step(global_action)

        print(f"  Done flag:    {done}")
        print(f"  Total reward: {total_reward:.4f}")
        print(f"  Final_obs[:2] (shape {final_obs.shape}):")
        print(final_obs[:2, :])  # show only first 2 nodes

    env.close()

if __name__ == "__main__":
    main()
