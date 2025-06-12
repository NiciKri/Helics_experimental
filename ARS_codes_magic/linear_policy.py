import numpy as np

class LinearPolicy:
    def __init__(self, state_shape, action_shape):
        '''
        Initialize linear policy of the form a = Ms + b where a is the action and s is the environment state

        Parameters
        ----------
        state_shape : tuple
            dimensions of the state matrix output by the environment
        action_shape : tuple
            dimensions of the action matrix passed to the environment
        '''
        self.state_shape = state_shape
        self.action_shape = action_shape
        self.policy = np.zeros((self.action_shape[0], (self.state_shape[0]+self.state_shape[1]))) #concatenated policy matrix
    
    def update_policy(self, new_policy):
        self.policy = new_policy
    
    def compute_action(self, state, policy):
        iden = np.eye(self.action_shape[1])
        state = np.vstack((state, iden))
        action = np.dot(policy, state)[0]
        return action
    
    def resolve_policy(self):
        M = self.policy[:, 0:self.state_shape[0]] #extracts M from concatenated policy matrix
        b = self.policy[:, -self.action_shape[1]:] #extracts b from concatenated policy matrix
        return M, b
    
    def get_policy(self):
        return self.policy
    
# a = Ms + b
# [p x n] = [p x m][m x n] + [p x n]
# action_shape = (p, n)
# state_shape = (m, n)

# p = action_shape[0]
# n = action_shape[1] = state_shape[1]
# m = state_shape [0]