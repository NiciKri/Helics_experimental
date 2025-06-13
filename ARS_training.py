# ars_train.py

import numpy as np
from dataclasses import dataclass
from Environment import DRLControllerEnv
import config

# ─── Linear policy ───────────────────────────────────────────────────────────
class LinearPolicy:
    def __init__(self, state_shape, action_shape):
        """
        a = M @ s + b
        state_shape: (n_nodes, 5) → we flatten to D = n_nodes*5
        action_shape: (n_nodes,) → P = n_nodes shifts
        """
        D = state_shape[0] * state_shape[1]
        P = action_shape[0]
        # store policy as (P x (D+1)) to include bias
        self.theta = np.zeros((P, D + 1), dtype=np.float64)

    def get_params(self):
        return self.theta

    def set_params(self, theta):
        self.theta = theta

    def act(self, obs_flat):
        # obs_flat: (D,)
        x = np.append(obs_flat, 1.0)      # bias term
        return self.theta.dot(x)          # → (P,)


# ─── Normalizer ─────────────────────────────────────────────────────────────
class Normalizer:
    def __init__(self, size):
        self.n = 0
        self.mean = np.zeros(size, float)
        self.S = np.zeros(size, float)  # sum of squares of diff

    def update(self, x):
        self.n += 1
        if self.n == 1:
            self.mean[:] = x
        else:
            old_mean = self.mean.copy()
            self.mean += (x - old_mean) / self.n
            self.S += (x - old_mean) * (x - self.mean)

    def normalize(self, x):
        self.update(x)
        var = (self.S / max(self.n,1)).clip(min=1e-2)
        return (x - self.mean) / np.sqrt(var)


# ─── ARS hyperparameters ─────────────────────────────────────────────────────
@dataclass
class ARSParams:
    rand_directions: int = 16    # N
    best_directions: int = 8     # b <= N
    learning_rate: float = 0.02  # α
    noise_std: float = 0.03      # ν
    num_iterations: int = 200    # number of policy updates
    rollout_length: int = 1      # each rollout runs one full sim (env.step returns full trajectory reward)


# ─── ARS agent ───────────────────────────────────────────────────────────────
class ARSAgent:
    def __init__(self, env, params: ARSParams):
        self.env = env
        self.p = params
        # flatten state_dim = n_nodes*5
        n_nodes, obs_dim = env.observation_space.shape
        self.state_dim = n_nodes * obs_dim
        # actions = n_nodes shifts
        self.action_dim = env.action_space.shape[0]
        # policy
        self.policy = LinearPolicy((n_nodes, obs_dim), (n_nodes,))
        self.normalizer = Normalizer(self.state_dim)

    def _rollout(self, theta):
        """Run one full simulation with a fixed policy theta, return cumulative reward."""
        self.policy.set_params(theta)
        obs, _ = self.env.reset()
        total_reward = 0.0
        done = False
        while not done:
            obs_flat = obs.flatten()
            obs_norm = self.normalizer.normalize(obs_flat)
            action = self.policy.act(obs_norm)
            obs, reward, done, truncated, info = self.env.step(action)
            total_reward += reward
        return total_reward

    def train(self):
        theta = self.policy.get_params().copy()
        for it in range(self.p.num_iterations):
            # 1) sample directions
            deltas = [np.random.randn(*theta.shape) for _ in range(self.p.rand_directions)]

            # 2) evaluate positive/negative rollouts
            rewards_pos = []
            rewards_neg = []
            for d in deltas:
                rewards_pos.append(self._rollout(theta + self.p.noise_std * d))
                rewards_neg.append(self._rollout(theta - self.p.noise_std * d))

            # 3) select best directions
            scores = np.maximum(rewards_pos, rewards_neg)
            idxs = np.argsort(scores)[-self.p.best_directions:]

            # 4) compute step
            step = np.zeros_like(theta)
            sigma_r = np.std(np.array(rewards_pos + rewards_neg))
            for idx in idxs:
                step += (rewards_pos[idx] - rewards_neg[idx]) * deltas[idx]
            step *= (self.p.learning_rate / (self.p.best_directions * (sigma_r + 1e-8)))

            # 5) update policy
            theta += step
            # log progress
            rollout_reward = self._rollout(theta)
            print(f"Iter {it+1:03d} | Rollout reward {rollout_reward:.2f}")

        # final policy
        self.policy.set_params(theta)
        return theta


# ─── MAIN ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # instantiate your HELICS‐based env
    sim_time = config.SIMULATION_TIME
    dt = config.TIME_STEP
    env = DRLControllerEnv(sim_time, dt)

    # configure ARS
    params = ARSParams(
        rand_directions=2,
        best_directions=1,
        learning_rate=0.001,
        noise_std=0.01,
        num_iterations=30
    )
    agent = ARSAgent(env, params)

    # train!
    best_theta = agent.train()

    # save best_theta if you like:
    np.save("best_ars_theta.npy", best_theta)
    print("Finished training. Policy saved to best_ars_theta.npy")
