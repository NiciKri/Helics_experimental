import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

# --- Define and apply global font sizes & spacings ---
plt.rcParams.update({
    'font.size':        20,
    'axes.titlesize':   22,
    'axes.labelsize':   16,
    'xtick.labelsize':  16,
    'ytick.labelsize':  16,
    'legend.fontsize':  12,
    'figure.titlesize': 22,
    'axes.titlepad':    20,
    'axes.labelpad':    12
})

# --- File Paths ---
file1 = os.path.join(os.getcwd(), '..', 'data', 'load_data.csv')
file2 = os.path.join(os.getcwd(), '..', 'data', 'solar_data.csv')

# --- Figure sizing ---
width_scale  = 1.5
base_figsize = (10, 6)
fig_width    = base_figsize[0] * width_scale
fig_height   = base_figsize[1]

# --- Load the CSVs ---
df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# --- Extract the whole first column as y-values ---
y1 = df1.iloc[:, 0]
y2 = df2.iloc[:, 0]

# --- Create x-axis in seconds ---
x = np.arange(len(y1))

# --- Determine hour ticks ---
max_seconds = x[-1]
max_hours   = int(np.floor(max_seconds / 3600))
hour_ticks  = np.arange(0, max_hours + 1) * 3600
hour_labels = np.arange(0, max_hours + 1)

# --- Plot ---
plt.figure(figsize=(fig_width, fig_height))
plt.plot(x, y1, label="Load")
plt.plot(x, y2, label="PV Production")

plt.title("Load and PV Values Over Time (Node 701a)")
plt.xlabel("Time [h]")
plt.ylabel("Load and PV Values (kW)")

plt.xticks(hour_ticks, hour_labels)    # label ticks in hours
plt.xlim(0, max_seconds)

plt.grid(True)
plt.legend(loc='upper right')
plt.tight_layout()

#save the figure
# save_dir = r"C:\Users\nicol\MT_docs_graphs\profile"
save_dir = os.getcwd() + r"\figures\profile"
os.makedirs(save_dir, exist_ok=True)
save_name = "load_profile.png"
save_path = os.path.join(save_dir, save_name)
plt.savefig(save_path, dpi=300, bbox_inches='tight')
plt.show()
