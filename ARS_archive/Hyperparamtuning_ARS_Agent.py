import os
import numpy as np
import optuna
from Environment import DRLControllerEnv
import config

# ===== Configuration (set everything here) =====
POLICY = "mlp"          # fixed to MLP only
HIDDEN_DIM = 32           # fixed hidden dimension for the MLP
N_TRIALS = 10             # number of Optuna trials
TIMEOUT = 20*60*60            # seconds to limit tuning (e.g., 30 minutes)
RAND_DIRS = 8             # number of random directions
BEST_DIRS = RAND_DIRS // 2 # number of best directions
WARMUP_STEPS = 10         # warm-up steps before training

class Normalizer:
    """
    Welford's algorithm to compute running mean and variance for normalization.
    """
    def __init__(self, size, eps=1e-8):
        self.mean = np.zeros(size)
        self.var = np.ones(size)
        self.count = eps

    def normalize(self, x):
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.var += delta * delta2
        std = np.sqrt(self.var / self.count)
        return (x - self.mean) / (std + 1e-8)


class MLPPolicy:
    """
    A tiny MLP policy: state -> tanh(W1 x + b1) -> W2 h + b2 -> action
    """
    def __init__(self, state_dim, action_dim):
        self.shapes = [
            (HIDDEN_DIM, state_dim),  # W1
            (HIDDEN_DIM,),            # b1
            (action_dim, HIDDEN_DIM), # W2
            (action_dim,),            # b2
        ]
        self.sizes = [np.prod(s) for s in self.shapes]
        self.total_size = sum(self.sizes)
        self.theta = np.zeros(self.total_size, dtype=float)

    def _unpack(self, theta):
        params = []
        idx = 0
        for shape, size in zip(self.shapes, self.sizes):
            flat = theta[idx:idx + size]
            params.append(flat.reshape(shape))
            idx += size
        return params

    def get_policy(self):
        return self.theta.copy()

    def set_policy(self, theta):
        self.theta[:] = theta

    def compute_action(self, state, theta=None):
        if theta is None:
            theta = self.theta
        W1, b1, W2, b2 = self._unpack(theta)
        x = state.flatten()
        h = np.tanh(W1.dot(x) + b1)
        return W2.dot(h) + b2


class ARSAgent:
    """
    Augmented Random Search with fixed MLP policy.
    """
    def __init__(
        self,
        env: DRLControllerEnv,
        rand_dirs: int = RAND_DIRS,
        best_dirs: int = BEST_DIRS,
        lr: float = 1e-4,
        nu: float = 0.02,
        n_iters: int = 5,
        smoothing: float = 0.9,
        warmup_steps: int = WARMUP_STEPS
    ):
        self.env = env
        obs_shape = env.observation_space.shape
        self.state_dim = int(np.prod(obs_shape))
        self.action_dim = env.action_space.shape[0]

        assert best_dirs <= rand_dirs, "best_dirs must be <= rand_dirs"
        self.rand_dirs = rand_dirs
        self.best_dirs = best_dirs
        self.lr = lr
        self.nu = nu
        self.n_iters = n_iters
        self.smoothing = smoothing
        self.warmup_steps = warmup_steps

        # Fixed MLP policy with HIDDEN_DIM
        self.policy = MLPPolicy(self.state_dim, self.action_dim)

        self.normalizer = Normalizer(self.state_dim)
        self._warmup_normalizer(self.warmup_steps)

    def _warmup_normalizer(self, total_steps: int):
        seen = 0
        while seen < total_steps:
            state, _ = self.env.reset()
            done = False
            while not done and seen < total_steps:
                flat = state.flatten()
                self.normalizer.normalize(flat)
                action = self.env.action_space.sample()
                state, _, done, truncated, _ = self.env.step(action)
                seen += 1

    def rollout(self, theta):
        state, _ = self.env.reset()
        x = self.normalizer.normalize(state.flatten())
        raw = self.policy.compute_action(x, theta)
        _, reward, done, truncated, _ = self.env.step(raw)
        return reward

    def train(self):
        theta = self.policy.get_policy()
        rewards_log, best_scores_log, smooth_log = [], [], []
        smoothed = None

        for it in range(self.n_iters):
            deltas = [np.random.randn(self.policy.total_size) for _ in range(self.rand_dirs)]
            r_pos = np.zeros(self.rand_dirs)
            r_neg = np.zeros(self.rand_dirs)

            for k, d in enumerate(deltas):
                r_pos[k] = self.rollout(theta + self.nu * d)
                r_neg[k] = self.rollout(theta - self.nu * d)

            scores = np.maximum(r_pos, r_neg)
            best_scores_log.append(scores.max())
            idxs = np.argsort(scores)[-self.best_dirs:]

            sigma = np.std(np.concatenate([r_pos[idxs], r_neg[idxs]])) + 1e-8
            step = np.zeros_like(theta)
            for idx in idxs:
                step += (r_pos[idx] - r_neg[idx]) * deltas[idx]
            theta += (self.lr / (self.best_dirs * sigma)) * step
            self.policy.set_policy(theta)

            r_eval = self.rollout(theta)
            rewards_log.append(r_eval)
            smoothed = r_eval if smoothed is None else self.smoothing * smoothed + (1 - self.smoothing) * r_eval
            smooth_log.append(smoothed)

            print(f"Iter {it+1}/{self.n_iters} | Eval {r_eval:.1f} | Best {best_scores_log[-1]:.1f} | Smooth {smoothed:.1f}")

        return theta, rewards_log, best_scores_log, smooth_log


def objective(trial):
    # sample only lr and nu
    lr = trial.suggest_loguniform("lr", 1e-6, 1e-2)
    nu = trial.suggest_float("nu", 1e-3, 0.1)

    env = DRLControllerEnv(
        simulation_time=config.SIMULATION_TIME,
        time_step=config.TIME_STEP
    )
    agent = ARSAgent(
        env,
        rand_dirs=RAND_DIRS,
        best_dirs=BEST_DIRS,
        lr=lr,
        nu=nu,
        n_iters=10,
        smoothing=0.9,
        warmup_steps=WARMUP_STEPS
    )

    _, _, _, smooth_log = agent.train()
    return smooth_log[-1]

if __name__ == "__main__":
    study = optuna.create_study(
        direction="maximize",
        pruner=optuna.pruners.MedianPruner(
            n_startup_trials=3,
            n_warmup_steps=3
        )
    )
    study.optimize(
        objective,
        n_trials=N_TRIALS,
        timeout=TIMEOUT
    )

    # Print and save best parameters
    output_txt = "best_params.txt"
    with open(output_txt, "w") as f:
        f.write(f"Best hyperparameters for '{POLICY}' policy:\n")
        f.write(f"lr: {study.best_params['lr']}\n")
        f.write(f"nu: {study.best_params['nu']}\n")
        f.write(f"Achieved smoothed reward: {study.best_value:.2f}\n")
    print(f"\nBest hyperparameters for '{POLICY}' policy saved to '{output_txt}'")
