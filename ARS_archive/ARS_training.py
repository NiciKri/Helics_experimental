import sys
import traceback

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
        self.M = np.zeros((P, D))
        self.b = np.zeros(P)

    def get_params(self):
        return np.concatenate([self.M.flatten(), self.b])

    def set_params(self, theta):
        P, D = self.M.shape, self.M.shape[1]
        total = P[0] * P[1]
        M_flat = theta[:total]
        b_flat = theta[total:]
        self.M = M_flat.reshape(P)
        self.b = b_flat

    def act(self, s):
        return self.M @ s + self.b


# ─── Normalizer ───────────────────────────────────────────────────────────────
class Normalizer:
    def __init__(self, size):
        self.n = 0
        self.mean = np.zeros(size)
        self.S = np.zeros(size)

    def update(self, x):
        self.n += 1
        if self.n == 1:
            self.mean = x.copy()
        else:
            old_mean = self.mean.copy()
            self.mean += (x - self.mean) / self.n
            self.S += (x - old_mean) * (x - self.mean)

    def normalize(self, x):
        self.update(x)
        var = (self.S / max(self.n, 1)).clip(min=1e-2)
        return (x - self.mean) / np.sqrt(var)


# ─── ARS hyperparameters ─────────────────────────────────────────────────────
@dataclass
class ARSParams:
    rand_directions: int = 16    # N
    best_directions: int = 8     # b ≤ N
    learning_rate: float = 0.02  # α
    noise_std: float = 0.03      # ν
    num_iterations: int = 200    # number of policy updates


# ─── ARS Agent ────────────────────────────────────────────────────────────────
class ARSAgent:
    def __init__(self, env: DRLControllerEnv, params: ARSParams):
        self.env = env
        self.p = params

        n_nodes, obs_dim = env.observation_space.shape
        self.state_dim = n_nodes * obs_dim
        self.action_dim = env.action_space.shape[0]
        self.policy = LinearPolicy((n_nodes, obs_dim), (n_nodes,))
        self.normalizer = Normalizer(self.state_dim)

    def _rollout(self, theta):
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
        print(f"Starting ARS training for {self.p.num_iterations} iterations")
        theta = self.policy.get_params().copy()

        for it in range(self.p.num_iterations):
            print(f"\n→ Beginning iteration {it+1}/{self.p.num_iterations}")
            try:
                # 1) sample random directions
                deltas = [np.random.randn(*theta.shape) for _ in range(self.p.rand_directions)]

                # 2) evaluate positive and negative rollouts
                rewards_pos = []
                rewards_neg = []
                for idx, d in enumerate(deltas):
                    print(f"  • Rollout +δ #{idx+1}")
                    rewards_pos.append(self._rollout(theta + self.p.noise_std * d))
                    print(f"  • Rollout –δ #{idx+1}")
                    rewards_neg.append(self._rollout(theta - self.p.noise_std * d))

                # 3) select best directions
                scores = np.maximum(rewards_pos, rewards_neg)
                idxs = np.argsort(scores)[-self.p.best_directions:]

                # 4) compute update step
                sigma_r = np.std(rewards_pos + rewards_neg) + 1e-8
                step = sum((rewards_pos[i] - rewards_neg[i]) * deltas[i] for i in idxs)
                step *= (self.p.learning_rate / (self.p.best_directions * sigma_r))

                # 5) update policy parameters
                theta += step

                # 6) log progress
                rollout_reward = self._rollout(theta)
                print(f"Iter {it+1:03d} | Rollout reward {rollout_reward:.2f}")

            except Exception:
                print(f"\n!!! Exception on iteration {it+1} !!!")
                traceback.print_exc()
                print("Aborting training early.")
                break

        print("Setting final policy parameters (theta) and returning")
        self.policy.set_params(theta)
        return theta


# ─── MAIN ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sim_time = config.SIMULATION_TIME
    dt = config.TIME_STEP
    env = DRLControllerEnv(sim_time, dt)

    params = ARSParams(
        rand_directions=2,
        best_directions=1,
        learning_rate=0.001,
        noise_std=0.03,
        num_iterations=200
    )

    agent = ARSAgent(env, params)

    env.model = lambda obs: agent.policy.act(agent.normalizer.normalize(obs.flatten()))

    try:
        best_theta = agent.train()
    except Exception:
        print("Fatal error during training:")
        traceback.print_exc()
        sys.exit(1)

    np.save("best_ars_theta.npy", best_theta)
    print("Finished training. Policy saved to best_ars_theta.npy")
