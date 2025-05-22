import os
import pandas as pd
import matplotlib.pyplot as plt

# --- Define and apply global font sizes & spacings ---
plt.rcParams.update({
    'font.size':        20,
    'axes.titlesize':   22,
    'axes.labelsize':   16,
    'xtick.labelsize':  16,
    'ytick.labelsize':  16,
    'legend.fontsize':  12,
    'figure.titlesize': 22,
    'axes.titlepad':    20,   # space between title and plot
    'axes.labelpad':    12    # space between labels and ticks
})

# --- User‐configurable settings ---
save_dir      = r"C:\Users\nicol\MT_docs_graphs\Midterm"
save_name     = "adaptive.png"
save_path     = os.path.join(save_dir, save_name)
plot_flag     = True
save_flag     = False

# --- New: set start time for plotting (in seconds) ---
#   e.g. 50 will skip everything before time=50s. Use None to plot from time=0.
start_time    = 0

# --- New: voltage axis limits ---
# Set to numeric values for fixed limits, or None for automatic scaling.
#vmin, vmax = 0.9800, 1.0050
vmin, vmax = None, None

# --- New: width scaling factor ---
# 1.0 = base width, >1 for wider, <1 for narrower
width_scale   = 1.5

# --- Base figure size (width, height) in inches ---
base_figsize  = (10, 6)

# --- Compute scaled figure size ---
fig_width     = base_figsize[0] * width_scale
fig_height    = base_figsize[1]

# ensure save directory exists
os.makedirs(save_dir, exist_ok=True)

# Load the voltage timeseries data
df = pd.read_csv("voltage_timeseries.csv")

# --- Apply start_time filter if set ---
if start_time is not None:
    df = df[df['time'] >= start_time]

# --- Choose Your Plotting Option ---
nodes_to_plot = ["701a", "701b", "701c", "727a", "727b", "727c"]
# nodes_to_plot = None

# --- Determine Which Nodes to Plot ---
if not nodes_to_plot:
    nodes_to_plot = [col for col in df.columns if col != "time"]
else:
    missing = [n for n in nodes_to_plot if n not in df.columns]
    if missing:
        raise ValueError(f"[ERROR] The following nodes are missing in the CSV: {missing}")

# --- Plotting ---
plt.figure(figsize=(fig_width, fig_height))
for node in nodes_to_plot:
    plt.plot(df['time'], df[node], label=node)

plt.title("Voltage Magnitude Over Time")
plt.xlabel("Time [s]")
plt.ylabel("Voltage Magnitude [pu]")

# apply fixed or automatic y‐limits
if vmin is not None or vmax is not None:
    lo, hi = plt.ylim()
    new_lo = vmin if vmin is not None else lo
    new_hi = vmax if vmax is not None else hi
    plt.ylim(new_lo, new_hi)

plt.grid(True)
plt.legend(loc='lower right')
plt.tight_layout()

# --- Save to disk ---
if save_flag:
    plt.savefig(save_path, dpi=300)
    print(f"Plot saved to: {save_path}")

    # Also save the filtered data with the same base name, but .csv
    base, _ = os.path.splitext(save_name)
    data_save_name = base + ".csv"
    data_save_path = os.path.join(save_dir, data_save_name)
    df.to_csv(data_save_path, index=False)
    print(f"Filtered data saved to: {data_save_path}")

# --- Show interactively ---
if plot_flag:
    plt.show()
