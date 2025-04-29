import numpy as np
import matplotlib.pyplot as plt
import os

output_dir = "C:/Users/nicol/Helics_experimental/outputs"  # adjust if needed
node = "s701a"  # change to your node of interest

y_vals = np.load(os.path.join(output_dir, f"y_values_{node}.npy"))

#print(y_vals[:])

plt.figure(figsize=(8,4))
plt.plot(y_vals, label=f"y signal ({node})")
#plt.axhline(0.25, color='r', linestyle='--', label="Threshold")
plt.xlabel("Time step")
plt.ylabel("y")
plt.title(f"Adaptive Controller 'y' evolution for {node}")
plt.legend()
plt.grid(True)
plt.show()
