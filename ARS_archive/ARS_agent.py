import os
import numpy as np
from Environment import DRLControllerEnv
import config

# === Configurable Hyperparameters ===
POLICY_TYPE   = 'mlp'      # 'mlp' or 'linear'
RAND_DIRS     = 2         # number of random perturbation directions
BEST_DIRS     = 1          # number of top directions to use for update
LR            = 1e-4       # learning rate
NU            = 0.001      # perturbation scale
N_ITERS       = 100        # number of training iterations
SMOOTHING     = 0.9        # reward smoothing factor (0-1)
HIDDEN_DIM    = 64         # hidden layer size for MLP policy
WARMUP_STEPS  = None       # number of env steps for normalizer warm-up (None = one episode)
RESULTS_DIR   = 'results'  # folder to save outputs


class Normalizer:
    """
    Welford's algorithm to compute running mean and variance for normalization.
    """
    def __init__(self, size, eps=1e-8):
        self.mean = np.zeros(size)
        self.var  = np.ones(size)
        self.count = eps

    def normalize(self, x):
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.var += delta * delta2
        std = np.sqrt(self.var / self.count)
        return (x - self.mean) / (std + 1e-8)


class LinearPolicy:
    """
    Linear policy: action = W x + b
    """
    def __init__(self, state_dim, action_dim):
        self.shapes = [(action_dim, state_dim), (action_dim,)]
        self.sizes = [np.prod(s) for s in self.shapes]
        self.total_size = sum(self.sizes)
        self.theta = np.zeros(self.total_size, dtype=float)

    def _unpack(self, theta):
        params, idx = [], 0
        for shape, size in zip(self.shapes, self.sizes):
            part = theta[idx:idx+size]
            params.append(part.reshape(shape))
            idx += size
        return params

    def get_policy(self):
        return self.theta.copy()

    def set_policy(self, theta):
        self.theta[:] = theta

    def compute_action(self, state, theta=None):
        if theta is None:
            theta = self.theta
        W, b = self._unpack(theta)
        return W.dot(state) + b


class MLPPolicy:
    """
    MLP policy: state -> tanh(W1 x + b1) -> W2 h + b2 -> action
    """
    def __init__(self, state_dim, action_dim, hidden_dim):
        self.shapes = [
            (hidden_dim, state_dim),
            (hidden_dim,),
            (action_dim, hidden_dim),
            (action_dim,),
        ]
        self.sizes = [np.prod(s) for s in self.shapes]
        self.total_size = sum(self.sizes)
        self.theta = np.zeros(self.total_size, dtype=float)

    def _unpack(self, theta):
        params, idx = [], 0
        for shape, size in zip(self.shapes, self.sizes):
            part = theta[idx:idx+size]
            params.append(part.reshape(shape))
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
        h = np.tanh(W1.dot(state) + b1)
        return W2.dot(h) + b2


class ARSAgent:
    """
    Augmented Random Search with switchable policy.
    """
    def __init__(self, env):
        obs_shape    = env.observation_space.shape
        state_dim    = int(np.prod(obs_shape))
        action_dim   = env.action_space.shape[0]

        # choose policy
        if POLICY_TYPE == 'mlp':
            self.policy = MLPPolicy(state_dim, action_dim, HIDDEN_DIM)
        elif POLICY_TYPE == 'linear':
            self.policy = LinearPolicy(state_dim, action_dim)
        else:
            raise ValueError(f"Unknown POLICY_TYPE: {POLICY_TYPE}")

        # hyperparams
        self.rand_dirs = RAND_DIRS
        self.best_dirs = BEST_DIRS
        self.lr        = LR
        self.nu        = NU
        self.n_iters   = N_ITERS
        self.smooth    = SMOOTHING

        # normalizer warm-up
        sim_time = config.SIMULATION_TIME
        dt       = config.TIME_STEP
        steps    = int(sim_time / dt) if WARMUP_STEPS is None else WARMUP_STEPS

        self.normalizer = Normalizer(state_dim)
        self._warmup(env, steps)
        self.env = env

    def _warmup(self, env, steps):
        seen = 0
        while seen < steps:
            state, _ = env.reset()
            done = False
            while not done and seen < steps:
                flat = state.flatten()
                self.normalizer.normalize(flat)
                action = env.action_space.sample()
                state, _, done, _, _ = env.step(action)
                seen += 1

    def _step(self, theta):
        state, _ = self.env.reset()
        x = self.normalizer.normalize(state.flatten())
        action = self.policy.compute_action(x, theta)
        _, reward, done, _, _ = self.env.step(action)
        return reward

    def train(self):
        theta = self.policy.get_policy()
        rewards, bests, smooths = [], [], []
        sm = None

        for it in range(self.n_iters):
            deltas = [np.random.randn(self.policy.total_size) for _ in range(self.rand_dirs)]
            r_pos = np.zeros(self.rand_dirs)
            r_neg = np.zeros(self.rand_dirs)
            for k, d in enumerate(deltas):
                r_pos[k] = self._step(theta + self.nu * d)
                r_neg[k] = self._step(theta - self.nu * d)

            scores = np.maximum(r_pos, r_neg)
            bests.append(scores.max())
            idxs = np.argsort(scores)[-self.best_dirs:]

            sigma = np.std(np.concatenate([r_pos[idxs], r_neg[idxs]])) + 1e-8
            step = sum((r_pos[i] - r_neg[i]) * deltas[i] for i in idxs)
            theta += (self.lr / (self.best_dirs * sigma)) * step
            self.policy.set_policy(theta)

            r_eval = self._step(theta)
            rewards.append(r_eval)
            sm = r_eval if sm is None else self.smooth * sm + (1 - self.smooth) * r_eval
            smooths.append(sm)
            print(f"Iter {it+1}/{self.n_iters} | Eval {r_eval:.1f} | Best {bests[-1]:.1f} | Smooth {sm:.1f}")

        return theta, rewards, bests, smooths


if __name__ == '__main__':
    # prepare environment and agent
    env = DRLControllerEnv(simulation_time=config.SIMULATION_TIME,
                            time_step=config.TIME_STEP)
    agent = ARSAgent(env)

    # train and save results
    theta, rewards, bests, smooths = agent.train()
    out_dir = os.path.join(RESULTS_DIR, POLICY_TYPE)
    os.makedirs(out_dir, exist_ok=True)
    np.save(os.path.join(out_dir, 'final_theta.npy'), theta)
    np.save(os.path.join(out_dir, 'rewards.npy'), rewards)
    np.save(os.path.join(out_dir, 'best_scores.npy'), bests)
    np.save(os.path.join(out_dir, 'smooth_rewards.npy'), smooths)
    print(f"Training complete with {POLICY_TYPE.upper()} policy. Final eval: {rewards[-1]}")
