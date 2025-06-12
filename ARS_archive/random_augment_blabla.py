import numpy as np
from Environment import DRLControllerEnv
import gymnasium as gym
import config

def evaluate(env, w_scalar, seed=None):
    """
    Roll out one full simulation with global_action = [w_scalar].
    Returns the total_reward from env.step().
    """
    obs, total_reward, done, truncated, info = env.step(
        np.array([w_scalar], dtype=np.float64)
    )
    return total_reward

def ars(env_fn,
        num_directions=16,
        top_directions=8,
        noise_std=0.02,
        step_size=0.03,
        num_iterations=5):
    """
    Augmented Random Search.
    env_fn: a zero-arg callable returning a fresh DRLControllerEnv.
    Returns:
        w: learned policy parameter
        reward_history: list of best rewards per iteration
    """
    # policy parameter (1D for generality)
    w = np.zeros(1, dtype=np.float64)
    reward_history = []

    for iteration in range(num_iterations):
        # evaluate current (unperturbed) policy
        env = env_fn()
        base_reward = evaluate(env, w[0])

        # sample perturbations
        deltas = [np.random.randn(*w.shape) for _ in range(num_directions)]
        rewards_pos = []
        rewards_neg = []

        # evaluate in both directions for each delta
        for delta in deltas:
            env = env_fn()
            r_pos = evaluate(env, w[0] + noise_std * delta[0])
            rewards_pos.append(r_pos)

            env = env_fn()
            r_neg = evaluate(env, w[0] - noise_std * delta[0])
            rewards_neg.append(r_neg)

        # rank and select top-performing directions
        scores = np.array([max(rp, rn) for rp, rn in zip(rewards_pos, rewards_neg)])
        idxs_top = np.argsort(scores)[-top_directions:]

        # compute parameter update
        diff_std = np.std([rewards_pos[i] - rewards_neg[i] for i in idxs_top]) + 1e-8
        step = np.zeros_like(w)
        for i in idxs_top:
            step += (rewards_pos[i] - rewards_neg[i]) * deltas[i]
        w += (step_size / (top_directions * noise_std * diff_std)) * step

        # record and print progress
        best_reward = scores[idxs_top[-1]]
        reward_history.append(best_reward)
        print(
            f"Iter {iteration+1:3d} | base R {base_reward:.2f} | "
            f"best R {best_reward:.2f} | w = {w[0]:.4f}"
        )

    return w, reward_history

if __name__ == "__main__":
    # simulation hyperparameters
    sim_time  = config.SIMULATION_TIME
    time_step = config.TIME_STEP

    # environment factory
    def make_env():
        return DRLControllerEnv(
            simulation_time=sim_time,
            time_step=time_step,
            for_eval=False
        )

    # run ARS
    best_w, reward_history = ars(
        make_env,
        num_directions=32,
        top_directions=16,
        noise_std=0.05,
        step_size=0.02,
        num_iterations=5
    )

    print("=== TRAINED GLOBAL SHIFT ===", best_w)

    # save the learned policy parameter
    np.save("best_policy_w.npy", best_w)
    print("Saved best policy to best_policy_w.npy")

    # save reward history for later plotting
    np.save("reward_history.npy", np.array(reward_history))
    print("Saved reward history to reward_history.npy")
