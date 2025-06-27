import numpy as np
import torch
import torch.nn as nn

# ─── Torch MLP Policy with bounded outputs ────────────────────────────────────
class NeuralPolicy(nn.Module):
    def __init__(self, state_dim: int, action_dim: int, hidden_sizes=(64, 64)):
        super().__init__()
        layers = []
        last_size = state_dim
        for h in hidden_sizes:
            layers += [nn.Linear(last_size, h), nn.ReLU()]
            last_size = h
        # final linear
        layers.append(nn.Linear(last_size, action_dim))
        # squash to [-1,1], then we'll scale to [-0.5,0.5]
        layers.append(nn.Tanh())

        self.net = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # net(x) ∈ [-1,1], so scale to [-0.5,0.5]
        return 0.5 * self.net(x)

    def get_params(self) -> np.ndarray:
        with torch.no_grad():
            vec = nn.utils.parameters_to_vector(self.parameters())
        return vec.cpu().numpy()

    def set_params(self, theta: np.ndarray):
        theta_t = torch.from_numpy(theta.astype(np.float32))
        with torch.no_grad():
            nn.utils.vector_to_parameters(theta_t, self.parameters())

    def act(self, s: np.ndarray) -> np.ndarray:
        x = torch.from_numpy(s.astype(np.float32))
        with torch.no_grad():
            a = self.forward(x)
        return a.cpu().numpy()
