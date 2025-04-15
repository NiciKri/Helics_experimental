import pandas as pd
import matplotlib.pyplot as plt

# Load the voltage timeseries data
df = pd.read_csv("voltage_timeseries.csv")

# --- Choose Your Plotting Option ---

# None for all nodes, array of strings for specific nodes
nodes_to_plot = ["701a", "701b", "701c", "727a", "727b", "727c"]
#nodes_to_plot = None

# --- Determine Which Nodes to Plot ---
if nodes_to_plot is None or len(nodes_to_plot) == 0:
    # Automatically choose all columns except the "time" column.
    nodes_to_plot = [col for col in df.columns if col != "time"]
else:
    # Check that each specified node exists in the DataFrame.
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
