import numpy as np
from Environment import DRLControllerEnv
import config

def main():
    # Parameters must match whatever you’d use for training.
    SIM_TIME = config.SIMULATION_TIME
    TIME_STEP = config.TIME_STEP

    # Create your env with for_eval=False so that reset() does the full HELICS startup
    env = DRLControllerEnv(simulation_time=SIM_TIME, time_step=TIME_STEP, for_eval=False)

    try:
        # 1) Reset once
        obs, info = env.reset()
        print("Initial observation shape:", obs.shape)  # should be (n_nodes, 5)
        print("Initial info:", info)

        done = False
        step_count = 0

        while not done:
            # 2) Pick a dummy action. Here we choose zeros; you could also try random:
            #    action = env.action_space.sample()
            action = np.zeros(env.action_space.shape, dtype=np.float64)

            obs, reward, terminated, truncated, info = env.step(action)

            step_count += 1
            if step_count % 10 == 0 or terminated:
                print(f"Step {step_count}: reward={reward:.3f}, terminated={terminated}, truncated={truncated}")
                # Optionally print a slice of obs to see voltage history, etc.
                print("  Obs sample (first node):", obs[0])

            done = terminated or truncated

        print("Episode finished after", step_count, "steps.")
        print("Final observation shape:", obs.shape)

    except Exception as e:
        # If anything in reset/step blows up, you’ll see the traceback here.
        print("Environment test failed:", e)

    finally:
        # 3) Always close the environment so HELICS tears down correctly
        env.close()
        print("Environment closed cleanly.")

if __name__ == "__main__":
    main()
