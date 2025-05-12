import gym
from gym import spaces
import numpy as np
from gym.envs.registration import register 

class DRLControllerEnv(gym.Env):
    """Gym wrapper around your HELICS federate loop."""
    metadata = {'render.modes': []}

    def __init__(self, simulation_time, time_step):
        super().__init__()
        # OBSERVATION: [vk, vkm1, psi_k–1, epsilonk, yk]
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(5,), dtype=np.float32
        )
        # ACTION: [Δp, Δq], small continuous tweaks around zero
        max_delta = 1.0  # tune to your expected breakpoint shifts
        self.action_space = spaces.Box(
            low=np.array([-max_delta, -max_delta], dtype=np.float32),
            high=np.array([+max_delta, +max_delta], dtype=np.float32),
            dtype=np.float32
        )

        self.sim_time = simulation_time
        self.dt = time_step
        # ── here you could initialize your HELICS federate, state buffers, etc. ──

    def reset(self):
        # reset your federate (or re-enter executing mode), clear histories…
        # return initial observation vector
        return np.zeros(5, dtype=np.float32)

    def step(self, action):
        # 1) take your Δp,Δq action → build & publish new_breakpoints
        # 2) step HELICS to next time: h.helicsFederateRequestTime(…)
        # 3) read new voltage, compute psi, epsilon, y → new_obs
        # 4) compute a scalar reward (you decide the reward-shaping)
        # 5) decide done = (federate_time >= sim_time)
        # 6) return new_obs, reward, done, {}
        obs = np.zeros(5, dtype=np.float32)
        reward = 0.0
        done = False
        info = {}
        return obs, reward, done, info

    def close(self):
        # finalize your federate here
        pass