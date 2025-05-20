import os
from stable_baselines3 import PPO
from stable_baselines3.common.callbacks import EvalCallback, BaseCallback
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
TOTAL_TIMESTEPS = STEPS_PER_EPISODE * EPOCHS

# --- Callbacks ---
class PrintEpochCallback(BaseCallback):
    def __init__(self, total_epochs:int, verbose=0):
        super().__init__(verbose)
        self.epoch = 0
        self.total_epochs = total_epochs

    def _on_step(self) -> bool:
        # Must return True to keep training going
        return True

    def _on_rollout_end(self) -> None:
        self.epoch += 1
        print(f"=== Epoch {self.epoch}/{self.total_epochs} complete ===")

# Create envs
env = DRLControllerEnv(simulation_time=SIM_TIME, time_step=TIME_STEP)
eval_env = DRLControllerEnv(simulation_time=SIM_TIME, time_step=TIME_STEP)

# Instantiate callbacks
print_cb = PrintEpochCallback(total_epochs=EPOCHS)
eval_cb = EvalCallback(
    eval_env,
    best_model_save_path=MODEL_DIR,
    log_path=LOG_DIR,
    eval_freq=STEPS_PER_EPISODE,  # once per episode
    deterministic=True,
    render=False,
)

# Create model
model = PPO(
    "MlpPolicy",
    env,
    verbose=1,
    tensorboard_log=LOG_DIR
)

try:
    print("🚀 Training started...")
    model.learn(
        total_timesteps=TOTAL_TIMESTEPS,
        callback=[print_cb, eval_cb]
    )
    print("✅ Training completed.")
    model.save(os.path.join(MODEL_DIR, "ppo_drl_controller"))
    print(f"📦 Model saved to: {MODEL_DIR}/ppo_drl_controller")
finally:
    env.close()
    eval_env.close()
    print("🧹 Federates closed and threads joined.")
