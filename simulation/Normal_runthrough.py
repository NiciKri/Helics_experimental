#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import numpy as np
import torch
import torch.nn as nn

from Environment import DRLControllerEnv
from utils import Normalizer, NeuralPolicy
import config

def load_normalizer_state(normalizer, path):
    d = np.load(path)
    normalizer.mean = d['mean']
    normalizer.S = d['S']
    normalizer.n = int(d['n'])

def run_one_episode(theta_path="greedy_ev_checkpoints/best_theta.npy"):
    # 1) Setup environment
    sim_time = config.SIMULATION_TIME
    dt = config.TIME_STEP
    env = DRLControllerEnv(sim_time, dt)

    # 2) Build policy & normalizer for 3 nodes × 6 features = 18
    agent_obs_nodes = [f"{config.TARGET_NODE}a", f"{config.TARGET_NODE}b", f"{config.TARGET_NODE}c"]
    state_dim = len(agent_obs_nodes) * 6  # 3*6=18
    action_dim = env.action_space.shape[0]

    policy = NeuralPolicy(state_dim, action_dim, hidden_sizes=(32, 32))
    normalizer = Normalizer(state_dim)

    # 3) Load learned parameters
    theta = np.load(theta_path)
    policy.set_params(theta)

    # 3b) Load normalizer state if available
    normalizer_path = theta_path.replace('theta', 'normalizer').replace('.npy', '.npz')
    try:
        load_normalizer_state(normalizer, normalizer_path)
        print(f"Loaded normalizer state from: {normalizer_path}")
    except Exception as e:
        print(f"[Warning] Could not load normalizer state from {normalizer_path}: {e}")

    # — OVERRIDE env.model so step() can call your policy —
    env.model = lambda obs: policy.act(normalizer.normalize(obs.flatten()))

    # 4) Run one rollout
    obs, _ = env.reset()
    total_reward = 0.0
    done = False

    while not done:
        # Extract only the agent's 3x6 observation (for node_base phases)
        obs_agent = np.array([obs[env.node_names.index(n)] for n in agent_obs_nodes])  # shape (3,6)
        obs_flat = obs_agent.flatten()  # shape (18,)
        obs_norm = normalizer.normalize(obs_flat)
        action = policy.act(obs_norm)
        obs, reward, done, truncated, info = env.step(action)
        total_reward += reward

    print(f"▶ Total reward this run: {total_reward:.2f}")

if __name__ == "__main__":
    run_one_episode()
