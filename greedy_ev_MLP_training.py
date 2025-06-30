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

# ─── save_normalizer_state function ───────────────────────────────────────────
def save_normalizer_state(normalizer, path):
    np.savez(path, mean=normalizer.mean, S=normalizer.S, n=normalizer.n)

# ─── ARS Agent ────────────────────────────────────────────────────────────────
class ARSAgent:
    def __init__(self, env: DRLControllerEnv, params: ARSParams):
        self.env = env
        self.p = params

        # Hardcode for 3x3 input and 3 output (a, b, c)
        # 3 nodes × 6 features = 18
        self.state_dim = 18
        self.action_dim = 3

        self.policy = NeuralPolicy(self.state_dim, self.action_dim, hidden_sizes=(32, 32))
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
            # Extract only the agent's 3x6 observation (for node_base phases)
            agent_obs_nodes = [f"{config.TARGET_NODE}a", f"{config.TARGET_NODE}b", f"{config.TARGET_NODE}c"]
            obs_agent = np.array([obs[self.env.node_names.index(n)] for n in agent_obs_nodes])  # shape (3,6)
            obs_flat = obs_agent.flatten()  # shape (18,)
            obs_norm = self.normalizer.normalize(obs_flat)
            action = self.policy.act(obs_norm)
            # clamp actions to [-0.5, 0.5]
            action = np.clip(action, -0.5, 0.5)
            obs, reward, done, truncated, info = self.env.step(action)
            total_reward += reward
            step_count += 1
        return total_reward

    def train(self) -> np.ndarray:
        print(f"Starting greedy evolutionary training for {self.p.num_iterations} iterations")
        theta = self.policy.get_params().copy()
        best_reward = self._rollout(theta)
        print(f"Initial theta reward: {best_reward:.2f}")
        checkpoint_dir = "ars_checkpoints"
        for it in range(self.p.num_iterations):
            print(f"\n→ Iteration {it+1}/{self.p.num_iterations}")
            try:
                # 1) sample random directions
                deltas = [np.random.randn(*theta.shape) for _ in range(self.p.rand_directions)]
                candidates = [theta]
                candidate_rewards = [best_reward]

                # 2) evaluate positive and negative rollouts using current noise nu
                for idx, d in enumerate(deltas):
                    theta_pos = theta + self.p.noise_std * d
                    theta_neg = theta - self.p.noise_std * d
                    r_pos = self._rollout(theta_pos)
                    r_neg = self._rollout(theta_neg)
                    candidates.extend([theta_pos, theta_neg])
                    candidate_rewards.extend([r_pos, r_neg])
                    print(f"  Direction {idx+1:02d}: reward_pos = {r_pos:.2f}, reward_neg = {r_neg:.2f}")

                # 3) select the best candidate
                best_idx = int(np.argmax(candidate_rewards))
                best_candidate = candidates[best_idx]
                best_candidate_reward = candidate_rewards[best_idx]

                # 4) update theta if improved
                if best_candidate_reward > best_reward:
                    print(f"  New best found! Reward improved from {best_reward:.2f} to {best_candidate_reward:.2f}")
                    theta = best_candidate.copy()
                    best_reward = best_candidate_reward
                else:
                    print(f"  No improvement. Keeping previous theta (reward {best_reward:.2f})")

                # Save intermediate model every 5 epochs
                checkpoint_interval = 1
                if (it + 1) % checkpoint_interval == 0:
                    checkpoint_path = f"{checkpoint_dir}/theta_iter_{it+1:03d}.npy"
                    np.save(checkpoint_path, theta)
                    print(f"Checkpoint saved: {checkpoint_path}")
                    # Save normalizer state
                    normalizer_path = f"{checkpoint_dir}/normalizer_iter_{it+1:03d}.npz"
                    save_normalizer_state(self.normalizer, normalizer_path)
                    print(f"Normalizer state saved: {normalizer_path}")

            except Exception:
                print(f"\n!!! Exception on iteration {it+1} !!!")
                traceback.print_exc()
                print("Aborting training early.")
                break

        print("Training complete—setting final parameters.")
        self.policy.set_params(theta)
        # Save final normalizer state
        final_normalizer_path = f"{checkpoint_dir}/normalizer_final.npz"
        save_normalizer_state(self.normalizer, final_normalizer_path)
        print(f"Final normalizer state saved: {final_normalizer_path}")
        return theta


# ─── MAIN ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sim_time = config.SIMULATION_TIME
    dt = config.TIME_STEP
    env = DRLControllerEnv(sim_time, dt)

    params = ARSParams(
        rand_directions=16,
        best_directions=2,
        learning_rate=0.005,
        noise_std=0.02,
        num_iterations=30,
        lr_decay_factor=0.5,
        lr_decay_every=10,
        noise_decay_factor=0.5,
        noise_decay_every=10,
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
