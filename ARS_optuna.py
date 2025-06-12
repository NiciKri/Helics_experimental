import numpy as np
import gymnasium as gym
import optuna
from typing import Any, Dict

# (Paste or import your Normalizer and ARS classes here)
class Normalizer:
    """
    Tracks running mean and variance of observations for normalization.
    """
    def __init__(self, size: int):
        self.n = 0
        self.mean = np.zeros(size)
        self.var = np.ones(size)

    def observe(self, x: np.ndarray):
        self.n += 1
        if self.n == 1:
            self.mean = x.copy()
            self.var = np.ones_like(x)
        else:
            old_mean = self.mean.copy()
            self.mean += (x - old_mean) / self.n
            self.var += (x - old_mean) * (x - self.mean)

    def normalize(self, inputs: np.ndarray) -> np.ndarray:
        std = np.sqrt(self.var / max(self.n - 1, 1))
        return (inputs - self.mean) / (std + 1e-8)

class ARS:
    """
    Augmented Random Search (ARS) with observation normalization.
    """
    def __init__(
        self,
        env_name: str = 'Pendulum-v1',
        n_directions: int = 16,
        n_best: int = 8,
        noise: float = 0.03,
        learning_rate: float = 0.02,
        max_steps: int = 200
    ):
        self.env = gym.make(env_name)
        self.obs_dim = self.env.observation_space.shape[0]
        self.act_low = self.env.action_space.low
        self.act_high = self.env.action_space.high
        self.n_directions = n_directions
        self.n_best = n_best
        self.nu = noise
        self.alpha = learning_rate
        self.max_steps = max_steps
        self.theta = np.zeros(self.obs_dim + 1)  # linear policy weights + bias
        self.normalizer = Normalizer(self.obs_dim)

    def get_action(self, state: np.ndarray, theta: np.ndarray) -> np.ndarray:
        state_norm = self.normalizer.normalize(state)
        value = np.dot(theta[:-1], state_norm) + theta[-1]
        return np.clip(value, self.act_low, self.act_high)

    def rollout(self, theta: np.ndarray) -> float:
        state, _ = self.env.reset()
        total_reward = 0.0
        for _ in range(self.max_steps):
            self.normalizer.observe(state)
            action = self.get_action(state, theta)
            state, reward, terminated, truncated, _ = self.env.step(action)
            total_reward += reward
            if terminated or truncated:
                break
        return total_reward

    def train(self, iterations: int = 100):
        for it in range(1, iterations + 1):
            deltas = [np.random.randn(self.obs_dim + 1) for _ in range(self.n_directions)]
            rewards_pos, rewards_neg = [], []
            for delta in deltas:
                rewards_pos.append(self.rollout(self.theta + self.nu * delta))
                rewards_neg.append(self.rollout(self.theta - self.nu * delta))

            scores = np.array([max(rp, rn) for rp, rn in zip(rewards_pos, rewards_neg)])
            idxs = np.argsort(scores)[-self.n_best:]

            # compute update step
            step = np.zeros_like(self.theta)
            paired = []
            for idx in idxs:
                step += (rewards_pos[idx] - rewards_neg[idx]) * deltas[idx]
                paired += [rewards_pos[idx], rewards_neg[idx]]

            sigma_r = np.std(np.array(paired))
            if sigma_r > 1e-8:
                self.theta += (self.alpha / (self.n_best * sigma_r)) * step

        # return the final policy performance
        return self.rollout(self.theta)


def objective(trial: optuna.Trial) -> float:
    # 1) suggest hyperparameters
    n_directions = trial.suggest_int('n_directions', 8, 64, step=8)
    n_best       = trial.suggest_int('n_best', 2, n_directions//2, step=2)
    noise        = trial.suggest_float('noise', 1e-3, 1e-1, log=True)
    lr           = trial.suggest_float('learning_rate', 1e-4, 1e-1, log=True)
    max_steps    = trial.suggest_int('max_steps', 100, 300, step=50)

    # 2) build and run one short training session
    agent = ARS(
        env_name='Pendulum-v1',
        n_directions=n_directions,
        n_best=n_best,
        noise=noise,
        learning_rate=lr,
        max_steps=max_steps
    )
    # do a small number of ARS updates to gauge performance
    final_reward = agent.train(iterations=20)
    return final_reward  # Optuna will try to maximize this

if __name__ == '__main__':
    # create a study and run
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=50, timeout=3600)  # e.g. 50 trials or 1h limit

    print("Best hyperparameters:")
    for k, v in study.best_trial.params.items():
        print(f"  • {k}: {v}")
    print(f"Best final reward: {study.best_value:.2f}")
