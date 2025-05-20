import os
#import gym
#import numpy as np
from stable_baselines3 import PPO
from stable_baselines3.common.callbacks import EvalCallback
from Environment import DRLControllerEnv
import config

# Logging and save dirs
LOG_DIR = "./logs"
MODEL_DIR = "./models"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)

# Hyperparameters
SIM_TIME = config.SIMULATION_TIME  # Keep short for faster episodes during training
TIME_STEP = config.TIME_STEP
EPOCHS = 50 # Number of simulations to run
TOTAL_TIMESTEPS = SIM_TIME*EPOCHS # Total number of time steps

# Initialize environment
env = DRLControllerEnv(simulation_time=SIM_TIME, time_step=TIME_STEP)

# Use PPO with MLP policy
model = PPO("MlpPolicy", env, verbose=1, tensorboard_log=LOG_DIR)

# Optional: evaluation callback
eval_env = DRLControllerEnv(simulation_time=SIM_TIME, time_step=TIME_STEP)
eval_callback = EvalCallback(
    eval_env,
    best_model_save_path=MODEL_DIR,
    log_path=LOG_DIR,
    eval_freq=50,
    deterministic=True,
    render=False,
)

try:
    # Start training
    print("🚀 Training started...")
    model.learn(total_timesteps=TOTAL_TIMESTEPS, callback=eval_callback)
    print("✅ Training completed.")

    # Save model
    model.save(os.path.join(MODEL_DIR, "ppo_drl_controller"))
    print(f"📦 Model saved to: {MODEL_DIR}/ppo_drl_controller")

finally:
    # Cleanup
    env.close()
    eval_env.close()
    print("🧹 Federates closed and threads joined.")
