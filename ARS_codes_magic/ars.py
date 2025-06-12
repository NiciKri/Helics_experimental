import numpy as np
from dataclasses import dataclass
# import matplotlib.pyplot as plt

from magic.envs.opendssenv import OpenDSSOscillationEnv
from magic.policies.linear_policy import LinearPolicy
from magic.agents.base import AgentParams, Agent
    
class Normalizer():
    #Welford's online algorithm - used to calculate mean and st deviation at the same time (with only one pass of the data instead of two)
    #implementation from: iamsuvhro
    def __init__(self, obs_length):
        self.mean = np.zeros(obs_length)
        self.n = np.zeros(obs_length)
        self.sos_diff = np.zeros(obs_length) # sos = sum of sqaures
        self.var = np.zeros(obs_length)

    def update_statistics(self, obs):
        self.n += 1
        #update mean 
        last_mean = self.mean.copy()
        self.mean += (obs - self.mean)/self.n
        #update sum of squares differences
        self.sos_diff += (obs-last_mean)*(obs-self.mean)
        self.var = (self.sos_diff/self.n).clip(min=1e-2)

    def normalize(self, obs):
        self.update_statistics(obs)
        obs_no_mean = obs - self.mean
        obs_std = np.sqrt(self.var)
        return obs_no_mean/obs_std
    
@dataclass
class ARSParams(AgentParams):
    '''
    Attributes
    ----------
    rand_directions : int
        number of random directions to explore in a single iteration of random search
    best_directions : int
        number of directions to track for the policy update step
    learning_rate : float
        step size for update step
    exploration_noise : float
        step size in each direction explored
    '''
    rand_directions: int
    best_directions: int
    learning_rate: float
    exploration_noise: float

class ARSAgent(Agent):
    def __init__(self, env:OpenDSSOscillationEnv, params: ARSParams):
        
        #initialize a basic agent
        super().__init__(params)
        
        #training parameters
        self.agent_params = params
        self.max_iterations = self.agent_params.rollouts #maximum number of rollouts to complete
        self.max_episodes = self.agent_params.episodes #number of episodes within a single rollout (steps taken in the env)
        self.n_simulations = self.agent_params.simulations
                
        #ARS hyperparameters
        self.rand_dir = self.agent_params.rand_directions
        self.best_dir = self.agent_params.best_directions
        assert self.best_dir <= self.rand_dir
        self.alpha = self.agent_params.learning_rate #learning rate
        self.nu = self.agent_params.exploration_noise #standard deviation of the exploration noise
        
        #environment initialization
        self.env = env
        
        #collect shape of input observations/state and shape of action output
        self.state_shape = self.env.state_space
        self.action_shape = self.env.action_space
        
        #initalize normalizer
        self.normalizer = Normalizer(self.state_shape)
        
        #initialize the appropriate policy
        mapping = {'linear': LinearPolicy, 'NN': None}
        policy_type = mapping.get(self.agent_params.policy.lower(), None) #default value is none
        self.policy = policy_type(self.state_shape, self.action_shape) #initialize policy
        self.policy_params = self.policy.get_policy() #array of policy parameters
        
    def random_search(self):
        #select random directions
        rand_dir = [np.random.randn(*self.policy_params.shape) for _ in range(self.rand_dir)]
        
        #create positive & negative policies for each direction
        rand_policies_positive = [self.policy_params + self.nu*_ for _ in rand_dir]
        rand_policies_negative = [self.policy_params - self.nu*_ for _ in rand_dir]

        #collect the rewards for each set of rollouts
        rewards_positive = [self.rollout(rand_policy_positive) for rand_policy_positive in rand_policies_positive]
        rewards_negative = [self.rollout(rand_policy_negative) for rand_policy_negative in rand_policies_negative]

        #sort the policies by the size of the reward
        best_scores = [max(r_pos, r_neg) for k,(r_pos,r_neg) in enumerate(zip(rewards_positive, rewards_negative))]
        idxs = np.asarray(best_scores).argsort()[-self.best_dir:]

        #update step
        best_pos_rewards = np.asarray([rewards_positive[idx] for idx in idxs]) #create array of best awards
        best_neg_rewards = np.asarray([rewards_negative[idx] for idx in idxs])
        reward_st_dev = np.append(best_pos_rewards, best_neg_rewards).std() #get standard deviation of the best rewards
        
        #calculate the update to be made
        reward_difference = np.zeros(self.policy_params.shape) 
        for i in range(0, self.best_dir):
            r_diff = (best_pos_rewards[i] - best_neg_rewards[i])*rand_dir[idxs[i]]
            reward_difference += r_diff

        self.policy_params = self.policy_params + (self.alpha/self.best_dir/reward_st_dev)*reward_difference
        self.policy.update_policy(self.policy_params)
        cumulative_rewards = self.rollout(self.policy_params)
        return self.policy_params, cumulative_rewards
        # return self.policy_params, cumulative_rewards, reward_st_dev
        
    def get_params(self):
        '''
        Returns training parameters and ARS hyperparameters
        '''
        return self.agent_params
    
    def rollout(self, policy):
        #reset env and get iniital observation
        state = self.env.reset()
        k = 0
        sum_rewards = 0
        
        # while not terminated and not truncated and j<self.max_episodes:
        while k < self.max_iterations:  
            # #normalize state
            state = self.normalizer.normalize(state) #normalize state
            action = self.policy.compute_action(state, policy) #get next action
            V_sim, reward, state = self.env.step(self.max_episodes, action)
            sum_rewards += reward
            k+=1
        return sum_rewards
        
    def train(self):
        k=0
        thetas = []
        rewards=[]
        # st_devs = []
        while k < self.n_simulations:
            theta, reward = self.random_search()
            # theta, reward, st_dev = self.random_search()
            thetas.append(theta)
            rewards.append(reward)
            # st_devs.append(st_dev)
            k+=1
        # for i in range(0, len(thetas)):
        #     print("Rollout", i, "theta:", thetas[i])
        # for i in range(0, len(rewards)):
        #     print("Rollout", i, "reward:", rewards[i])
        
        # Plot the rewards
        # plt.plot(rewards)  # Line plot with markers
        # title = "Rewards, alpha=" + str(round(self.alpha, 5)) + ", nu=" + str(round(self.nu, 5))
        # plt.title(title)
        # plt.xlabel("Rollout")
        # plt.ylabel("Reward")
        # plt.grid(True)  # Optional: Adds grid lines
        # plt.show()  # Displays the plot
        
        # plt.plot(st_devs)  # Line plot with markers
        # title = "Standard Deviation of rewards, alpha=" + str(round(self.alpha, 3)) + ", nu=" + str(round(self.nu, 3))
        # plt.title(title)
        # plt.xlabel("Rollout")
        # plt.ylabel("standard deviation of rewards")
        # plt.grid(True)  # Optional: Adds grid lines
        # plt.show()  # Displays the plot
        
        return thetas, rewards
        # return thetas, rewards, st_devs
