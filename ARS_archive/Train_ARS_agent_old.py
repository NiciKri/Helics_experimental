import numpy as np
from Environment import DRLControllerEnv
import config

class Normalizer:
    def __init__(self, dim):
        self.mean = np.zeros(dim)
        self.n = np.zeros(dim)
        self.sos_diff = np.zeros(dim)
        self.var = np.zeros(dim)

    def update(self, x):
        self.n += 1
        last_mean = self.mean.copy()
        self.mean += (x - self.mean) / self.n
        self.sos_diff += (x - last_mean) * (x - self.mean)
        self.var = np.clip(self.sos_diff / self.n, 1e-2, None)

    def normalize(self, x):
        self.update(x)
        return (x - self.mean) / np.sqrt(self.var)

class ARSAgent:
    def __init__(
        self,
        env: DRLControllerEnv,
        rand_directions: int = config.RAND_DIRECTIONS if hasattr(config, 'RAND_DIRECTIONS') else 10,
        best_directions: int = config.BEST_DIRECTIONS if hasattr(config, 'BEST_DIRECTIONS') else 2,
        learning_rate: float = config.LEARNING_RATE if hasattr(config, 'LEARNING_RATE') else 0.0001,
        exploration_noise: float = config.EXPLORATION_NOISE if hasattr(config, 'EXPLORATION_NOISE') else 0.001,
        n_simulations: int = config.N_SIMULATIONS if hasattr(config, 'N_SIMULATIONS') else 5,
        rollout_length: int = config.ROLLOUT_LENGTH if hasattr(config, 'ROLLOUT_LENGTH') else 300,
        n_rollouts: int = config.N_ROLLOUTS if hasattr(config, 'N_ROLLOUTS') else 5
    ):
        assert best_directions <= rand_directions, "best_directions must be <= rand_directions"
        self.env = env
        # hyperparameters
        self.rand_dir = rand_directions
        self.best_dir = best_directions
        self.alpha = learning_rate
        self.nu = exploration_noise
        self.n_sim = n_simulations
        self.rollout_length = rollout_length
        self.n_rollouts = n_rollouts

        # dimensions
        obs_space = env.observation_space.shape
        self.obs_flat_dim = int(np.prod(obs_space))
        # include bias term
        self.param_dim = self.obs_flat_dim + 1

        # policy parameters
        self.theta = np.zeros(self.param_dim)

        # normalizer
        self.normalizer = Normalizer(self.obs_flat_dim)

    def _compute_action(self, obs, theta):
        obs_flat = obs.flatten()
        norm_obs = self.normalizer.normalize(obs_flat)
        a = np.dot(theta[:-1], norm_obs) + theta[-1]
        return np.clip(a, self.env.action_space.low, self.env.action_space.high)

    def _rollout(self, theta):
        obs, _ = self.env.reset()
        total_r = 0.0
        for step in range(self.rollout_length):
            action = self._compute_action(obs, theta)
            obs, reward, terminated, truncated, _ = self.env.step(np.array([action]))
            total_r += reward
            if terminated or truncated:
                break
        return total_r

    def train(self):
        eps = 1e-8
        for sim in range(self.n_sim):
            # sample perturbations
            deltas = [np.random.randn(self.param_dim) for _ in range(self.rand_dir)]
            pos_r = []
            neg_r = []
            # evaluate each direction with multiple rollouts
            for d in deltas:
                rp_list = [self._rollout(self.theta + self.nu * d) for _ in range(self.n_rollouts)]
                rn_list = [self._rollout(self.theta - self.nu * d) for _ in range(self.n_rollouts)]
                pos_r.append(np.mean(rp_list))
                neg_r.append(np.mean(rn_list))

            # select best directions
            scores = np.array([max(p, n) for p, n in zip(pos_r, neg_r)])
            idxs = np.argsort(scores)[-self.best_dir:]

            # compute sigma over selected directions
            selected = []
            for i in idxs:
                selected.extend([pos_r[i], neg_r[i]])
            sigma_r = np.std(selected) + eps

            # compute update
            update = np.zeros_like(self.theta)
            for i in idxs:
                update += (pos_r[i] - neg_r[i]) * deltas[i]

            # apply update
            self.theta += (self.alpha / (self.best_dir * sigma_r)) * update

            # log progress
            avg_r = self._rollout(self.theta)
            print(f"Simulation {sim+1}/{self.n_sim}: AvgReward={avg_r:.2f}")
        return self.theta

if __name__ == '__main__':
    env = DRLControllerEnv(config.SIMULATION_TIME, config.TIME_STEP)
    agent = ARSAgent(env)
    optimal_theta = agent.train()
