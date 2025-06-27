import sys
import traceback

import numpy as np
import torch
import torch.nn as nn
from dataclasses import dataclass

from Environment import DRLControllerEnv
from utils import Normalizer, NeuralPolicy
import config

# ─── ARS hyperparameters ─────────────────────────────────────────────────────
@dataclass
class ARSParams:
    rand_directions: int = 16        # N
    best_directions: int = 8         # b ≤ N
    learning_rate: float = 0.02      # initial α
    noise_std: float = 0.03          # ν₀
    num_iterations: int = 200        # number of policy updates
    lr_decay_factor: float = 0.5     # multiply α by this every lr_decay_every steps
    lr_decay_every: int = 10         # iterations between LR reductions
    noise_decay_factor: float = 0.5  # multiply ν by this every noise_decay_every steps
    noise_decay_every: int = 10      # iterations between noise reductions

# ─── ARS Agent ────────────────────────────────────────────────────────────────
class ARSAgent:
    def __init__(self, env: DRLControllerEnv, params: ARSParams):
        self.env = env
        self.p = params

        # Hardcode for 3x3 input and 3 output (a, b, c)
        self.state_dim = 9
        self.action_dim = 3

        self.policy = NeuralPolicy(self.state_dim, self.action_dim, hidden_sizes=(9, 9))
        self.normalizer = Normalizer(self.state_dim)

    def _rollout(self, theta: np.ndarray) -> float:
        """
        Run one episode with the given parameters theta, returning total reward.
        Clips actions to [-0.5, 0.5] after network output.
        """
        self.policy.set_params(theta)
        obs, _ = self.env.reset()
        total_reward = 0.0
        done = False
        step_count = 0
        while not done:
            # Extract only the agent's 3x3 observation (for node_base phases)
            agent_obs_nodes = [f"{config.TARGET_NODE}a", f"{config.TARGET_NODE}b", f"{config.TARGET_NODE}c"]
            obs_agent = np.array([obs[self.env.node_names.index(n)] for n in agent_obs_nodes])
            obs_flat = obs_agent.flatten()
            obs_norm = self.normalizer.normalize(obs_flat)
            action = self.policy.act(obs_norm)
            # clamp actions to [-0.5, 0.5]
            action = np.clip(action, -0.5, 0.5)
            #print(f"Step {step_count}: obs_agent = {obs_agent}, action = {action}")
            obs, reward, done, truncated, info = self.env.step(action)
            #print(f"Step {step_count}: reward = {reward}, done = {done}, info = {info}")
            total_reward += reward
            step_count += 1
        #print(f"Total reward for rollout: {total_reward}")
        return total_reward

    def train(self) -> np.ndarray:
        print(f"Starting ARS training for {self.p.num_iterations} iterations")
        theta = self.policy.get_params().copy()

        # initialize schedulers
        lr = self.p.learning_rate
        nu = self.p.noise_std

        for it in range(self.p.num_iterations):
            print(f"\n→ Iteration {it+1}/{self.p.num_iterations} | LR={lr:.5f} | noise={nu:.5f}")
            try:
                # 1) sample random directions
                deltas = [np.random.randn(*theta.shape) for _ in range(self.p.rand_directions)]

                # 2) evaluate positive and negative rollouts using current noise nu
                rewards_pos, rewards_neg = [], []
                for idx, d in enumerate(deltas):
                    rewards_pos.append(self._rollout(theta + nu * d))
                    rewards_neg.append(self._rollout(theta - nu * d))

                # 3) select best directions
                scores = np.maximum(rewards_pos, rewards_neg)
                top_idxs = np.argsort(scores)[-self.p.best_directions:]

                # 4) compute the update step
                sigma_r = np.std(rewards_pos + rewards_neg) + 1e-8
                step = np.zeros_like(theta)
                for i in top_idxs:
                    step += (rewards_pos[i] - rewards_neg[i]) * deltas[i]
                # apply scheduled learning rate
                step *= (lr / (self.p.best_directions * sigma_r))

                # 5) update policy parameters
                theta += step

                # 6) log progress via unperturbed rollout
                rollout_reward = self._rollout(theta)
                print(f"Iter {it+1:03d} | Rollout reward {rollout_reward:.2f}")

                # 7) adjust schedules
                if (it + 1) % self.p.lr_decay_every == 0:
                    lr *= self.p.lr_decay_factor
                    print(f"↘ Decayed LR → {lr:.5f}")
                if (it + 1) % self.p.noise_decay_every == 0:
                    nu *= self.p.noise_decay_factor
                    print(f"↘ Decayed noise_std → {nu:.5f}")

            except Exception:
                print(f"\n!!! Exception on iteration {it+1} !!!")
                traceback.print_exc()
                print("Aborting training early.")
                break

        print("Training complete—setting final parameters.")
        self.policy.set_params(theta)
        return theta


# ─── MAIN ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sim_time = config.SIMULATION_TIME
    dt = config.TIME_STEP
    env = DRLControllerEnv(sim_time, dt)

    params = ARSParams(
        rand_directions=32,
        best_directions=16,
        learning_rate=0.005,
        noise_std=0.02,
        num_iterations=100,
        lr_decay_factor=0.5,
        lr_decay_every=10,
        noise_decay_factor=0.5,
        noise_decay_every=20,
    )

    agent = ARSAgent(env, params)

    # override env.model for built-in controllers
    env.model = lambda obs: agent.policy.act(agent.normalizer.normalize(obs.flatten()))

    try:
        best_theta = agent.train()
    except Exception:
        print("Fatal error during training:")
        traceback.print_exc()
        sys.exit(1)

    np.save("best_ars_theta.npy", best_theta)
    print("Finished training. Policy parameters saved to best_ars_theta.npy")
