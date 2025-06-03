import os
from stable_baselines3 import PPO
from Environment import DRLControllerEnv
import config

# Logging + model dirs
LOG_DIR = "./logs"
MODEL_DIR = "./models"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)

# Hyperparameters
SIM_TIME = config.SIMULATION_TIME
TIME_STEP = config.TIME_STEP
EPOCHS = 50
STEPS_PER_EPISODE = int(SIM_TIME / TIME_STEP)

# Create one env (no evaluation)
env = DRLControllerEnv(simulation_time=SIM_TIME, time_step=TIME_STEP)

# Instantiate PPO with no eval callback
model = PPO(
    "MlpPolicy",
    env,
    verbose=1,
    tensorboard_log=LOG_DIR,
    # Make sure SB3 does not internally reset the timestep counter
    reset_num_timesteps=False
)

try:
    print("Training started...")
    for epoch in range(1, EPOCHS + 1):
        # Learn for exactly one episode’s worth of timesteps
        model.learn(
            total_timesteps=STEPS_PER_EPISODE,
            reset_num_timesteps=False,  # continue timestep count across calls
        )
        print(f"=== Epoch {epoch}/{EPOCHS} complete ===")

    print("Training completed.")
    model.save(os.path.join(MODEL_DIR, "ppo_drl_controller"))
    print(f"Model saved to: {MODEL_DIR}/ppo_drl_controller")
finally:
    env.close()
    print("Federates closed and threads joined.")
