import pandas as pd
import matplotlib.pyplot as plt

# Load the voltage timeseries data
df = pd.read_csv("voltage_timeseries.csv")

# --- Choose the nodes you want to plot ---
nodes_to_plot = ["701a", "701b", "701c"]  # Change these to your desired nodes

# --- Check if the columns exist ---
missing = [n for n in nodes_to_plot if n not in df.columns]
if missing:
    print(f"[ERROR] The following nodes are missing in the CSV: {missing}")
    print("Available nodes:", list(df.columns))
    exit()

# --- Plotting ---
plt.figure(figsize=(10, 6))
for node in nodes_to_plot:
    plt.plot(df['time'], df[node], label=node)

plt.xlabel("Time [s]")
plt.ylabel("Voltage Magnitude [pu]")
plt.title("Voltage Magnitude Over Time")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()
