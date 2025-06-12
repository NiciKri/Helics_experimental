import pandas as pd
import matplotlib.pyplot as plt
import os

log_path = os.path.join('ars_logs', 'rewards.csv')
if not os.path.exists(log_path):
    raise FileNotFoundError(f"Couldn't find log file at {log_path}")

# 1) Load the data
df = pd.read_csv(log_path)

# 2) (Optional) Preview
print(df.head())

# 3) Plot
plt.figure()
plt.plot(df['iteration'], df['reward'])
plt.xlabel('Iteration')
plt.ylabel('Reward')
plt.title('ARS Reward Over Iterations')
plt.grid(True)
plt.tight_layout()
plt.show()