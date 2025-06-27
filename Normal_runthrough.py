#!/usr/bin/env python3
import numpy as np
import torch
import torch.nn as nn

from Environment import DRLControllerEnv
from utils import Normalizer, NeuralPolicy
import config

def run_one_episode(theta_path="best_ars_theta.npy"):
    # 1) Setup environment
    sim_time = config.SIMULATION_TIME
    dt = config.TIME_STEP
    env = DRLControllerEnv(sim_time, dt)

    # 2) Build policy & normalizer
    n_nodes, obs_dim = env.observation_space.shape
    state_dim = n_nodes * obs_dim
    action_dim = env.action_space.shape[0]

    policy = NeuralPolicy(state_dim, action_dim, hidden_sizes=(64, 64))
    normalizer = Normalizer(state_dim)

    # 3) Load learned parameters
    theta = np.load(theta_path)
    policy.set_params(theta)

    # — OVERRIDE env.model so step() can call your policy —
    env.model = lambda obs: policy.act(normalizer.normalize(obs.flatten()))

    # 4) Run one rollout
    obs, _ = env.reset()
    total_reward = 0.0
    done = False

    while not done:
        obs_flat = obs.flatten()
        obs_norm = normalizer.normalize(obs_flat)
        action = policy.act(obs_norm)
        obs, reward, done, truncated, info = env.step(action)
        total_reward += reward

    print(f"▶ Total reward this run: {total_reward:.2f}")

if __name__ == "__main__":
    run_one_episode()
