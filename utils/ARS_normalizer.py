import numpy as np

# ─── Normalizer ───────────────────────────────────────────────────────────────
class Normalizer:
    def __init__(self, size):
        self.n = 0
        self.mean = np.zeros(size, dtype=np.float32)
        self.S = np.zeros(size, dtype=np.float32)

    def update(self, x: np.ndarray):
        self.n += 1
        if self.n == 1:
            self.mean = x.copy()
        else:
            old_mean = self.mean.copy()
            self.mean += (x - self.mean) / self.n
            self.S += (x - old_mean) * (x - self.mean)

    def normalize(self, x: np.ndarray) -> np.ndarray:
        self.update(x)
        var = (self.S / max(self.n, 1)).clip(min=1e-2)
        return (x - self.mean) / np.sqrt(var)