import numpy as np
import torch
import torch.nn as nn
from torch.nn.utils import parameters_to_vector, vector_to_parameters
from dataclasses import dataclass
from Environment import DRLControllerEnv
import config

# ─── Torch-based two-layer MLP policy per node ───────────────────────────────
class TorchMLPPolicy(nn.Module):
    def __init__(self, feature_dim: int, hidden_size: int):
        super().__init__()
        self.fc1 = nn.Linear(feature_dim, hidden_size)
        self.fc2 = nn.Linear(hidden_size, 1)
        nn.init.xavier_uniform_(self.fc1.weight)
        nn.init.zeros_(self.fc1.bias)
        nn.init.xavier_uniform_(self.fc2.weight)
        nn.init.zeros_(self.fc2.bias)

    def forward(self, obs: torch.Tensor) -> torch.Tensor:
        h = torch.tanh(self.fc1(obs))
        out = self.fc2(h)
        return out.view(-1)

    def get_params(self) -> np.ndarray:
        vec = parameters_to_vector(self.parameters()).detach().cpu()
        return vec.numpy().astype(np.float64)

    def set_params(self, theta: np.ndarray):
        vec = torch.from_numpy(theta.astype(np.float32))
        vector_to_parameters(vec, self.parameters())

    def act(self, obs_np: np.ndarray) -> np.ndarray:
        obs = torch.from_numpy(obs_np.astype(np.float32))
        with torch.no_grad():
            out = self.forward(obs)
        return out.cpu().numpy()

# ─── Per-feature normalizer ─────────────────────────────────────────────────
class Normalizer:
    def __init__(self, feature_dim: int):
        self.n = 0
        self.mean = np.zeros(feature_dim, dtype=np.float64)
        self.S = np.zeros(feature_dim, dtype=np.float64)

    def update(self, x: np.ndarray):
        self.n += 1
        if self.n == 1:
            self.mean[:] = x
        else:
            delta = x - self.mean
            self.mean += delta / self.n
            self.S += delta * (x - self.mean)

    def normalize(self, obs: np.ndarray) -> np.ndarray:
        for row in obs:
            self.update(row)
        var = (self.S / max(self.n, 1)).clip(min=1e-2)
        return (obs - self.mean) / np.sqrt(var)

# ─── ARS hyperparameters ────────────────────────────────────────────────────
@dataclass
class ARSParams:
    rand_directions: int = 8
    best_directions: int = 2
    learning_rate: float = 0.001
    noise_std: float = 0.01
    num_iterations: int = 200
    hidden_size: int = 32

# ─── ARS agent using TorchMLPPolicy ─────────────────────────────────────────
class ARSAgent:
    def __init__(self, env: DRLControllerEnv, params: ARSParams):
        self.env = env
        self.p = params
        _, obs_dim = env.observation_space.shape

        self.policy = TorchMLPPolicy(obs_dim, self.p.hidden_size)
        self.normalizer = Normalizer(obs_dim)

    def _rollout(self, theta: np.ndarray) -> float:
        self.policy.set_params(theta)
        obs, _ = self.env.reset()
        total_reward = 0.0
        done = False
        while not done:
            obs_norm = self.normalizer.normalize(obs)
            actions = self.policy.act(obs_norm)
            obs, reward, done, truncated, info = self.env.step(actions)
            total_reward += reward
        return total_reward

    def train(self) -> np.ndarray:
        theta = self.policy.get_params().copy()
        learning_rate = self.p.learning_rate

        for it in range(1, self.p.num_iterations + 1):
            deltas = [np.random.randn(theta.size) for _ in range(self.p.rand_directions)]
            rewards_pos, rewards_neg = [], []

            for d in deltas:
                rewards_pos.append(self._rollout(theta + self.p.noise_std * d))
                rewards_neg.append(self._rollout(theta - self.p.noise_std * d))

            # *** print raw rewards ***
            print(f"Iter {it:03d} — rewards_pos: {rewards_pos}")
            print(f"Iter {it:03d} — rewards_neg: {rewards_neg}")

            # normalize rewards
            all_r = np.array(rewards_pos + rewards_neg)
            mu, sigma = all_r.mean(), all_r.std() + 1e-8
            rp = [(r - mu) / sigma for r in rewards_pos]
            rn = [(r - mu) / sigma for r in rewards_neg]

            # pick best directions
            scores = [max(a, b) for a, b in zip(rp, rn)]
            best_ids = np.argsort(scores)[-self.p.best_directions:]

            # update theta
            step = np.zeros_like(theta)
            for idx in best_ids:
                step += (rp[idx] - rn[idx]) * deltas[idx]
            learning_rate *= 0.8
            theta += (learning_rate / self.p.best_directions) * step

            # log updated policy performance
            r = self._rollout(theta)
            print(f"Iter {it:03d} — rollout reward: {r:.2f}")

        self.policy.set_params(theta)
        return theta

# ─── MAIN ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sim_time = config.SIMULATION_TIME
    dt = config.TIME_STEP
    env = DRLControllerEnv(sim_time, dt)

    params = ARSParams(
        rand_directions=2,
        best_directions=1,
        learning_rate=0.001,
        noise_std=0.01,
        num_iterations=100,
        hidden_size=32
    )

    agent = ARSAgent(env, params)
    best_theta = agent.train()
    np.save("best_torch_mlp_theta.npy", best_theta)
    print("Training complete — Torch MLP policy saved to best_torch_mlp_theta.npy")
    print("Best parameters:", best_theta)
