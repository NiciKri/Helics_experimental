#!/usr/bin/env python3

import numpy as np
import gymnasium as gym

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
        n_directions: int = 56,
        n_best: int = 18,
        noise: float = 0.01236473939983117,
        learning_rate: float = 0.02309410454296106,
        max_steps: int = 100
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
        self.theta = np.zeros(self.obs_dim + 1)

    def get_action(self, state: np.ndarray, theta: np.ndarray) -> np.ndarray:
        value = np.dot(theta[:-1], state) + theta[-1]
        return np.clip(value, self.act_low, self.act_high)

    def rollout(self, theta: np.ndarray) -> float:
        norm = Normalizer(self.obs_dim)
        state, _ = self.env.reset()
        total_reward = 0.0
        for _ in range(self.max_steps):
            norm.observe(state)
            s_norm = norm.normalize(state)
            action = self.get_action(s_norm, theta)
            state, reward, terminated, truncated, _ = self.env.step(action)
            total_reward += reward
            if terminated or truncated:
                break
        return total_reward

    def train(self, iterations: int = 100):
        for _ in range(iterations):
            # Perform one ARS update (using train_step logic if refactored)  
            deltas      = [np.random.randn(self.obs_dim + 1) for _ in range(self.n_directions)]
            rewards_pos = []
            rewards_neg = []
            for d in deltas:
                rewards_pos.append(self.rollout(self.theta + self.nu * d))
                rewards_neg.append(self.rollout(self.theta - self.nu * d))
            scores = np.array([max(rp, rn) for rp, rn in zip(rewards_pos, rewards_neg)])
            idxs   = np.argsort(scores)[-self.n_best:]
            step = np.zeros_like(self.theta)
            paired = []
            for i in idxs:
                step   += (rewards_pos[i] - rewards_neg[i]) * deltas[i]
                paired += [rewards_pos[i], rewards_neg[i]]
            sigma_r = np.std(np.array(paired))
            if sigma_r > 1e-8:
                self.theta += (self.alpha / (self.n_best * sigma_r)) * step


def main():
    # Instantiate and optionally train the agent
    agent = ARS(env_name='Pendulum-v1')
    print("Training agent for 100 iterations...")
    agent.train(iterations=100)

    # --- Visualization in human mode ---
    print("Launching environment window (human mode)...")
    env = gym.make('Pendulum-v1', render_mode='human')
    obs, _ = env.reset()
    total_reward = 0.0
    for _ in range(agent.max_steps):
        # normalize and act
        # re-use Normalizer if desired; here we use raw observation
        action = agent.get_action(obs, agent.theta)
        obs, reward, terminated, truncated, _ = env.step(action)
        total_reward += reward
        if terminated or truncated:
            break
    print(f"Total reward: {total_reward:.2f}")
    env.close()

if __name__ == '__main__':
    main()
