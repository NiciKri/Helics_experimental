import numpy as np
import matplotlib.pyplot as plt
import os

# output_dir = "C:/Users/nicol/Helics_experimental/outputs"  # adjust if needed
output_dir= os.path.join(os.getcwd(), '..', 'outputs')
node = "s701a"  # change to your node of interest


epsion_vals = np.load(os.path.join(output_dir, f"epsilon_values_{node}.npy"))
y_vals = np.load(os.path.join(output_dir, f"y_values_{node}.npy"))
up_vals = np.load(os.path.join(output_dir, f"up_values_{node}.npy"))
uq_vals = np.load(os.path.join(output_dir, f"uq_values_{node}.npy"))

plot_y_flag = True
plot_epsilon_flag = True
plot_up_flag = True
plot_uq_flag = False


def plot_y():
    plt.figure(figsize=(8,4))
    plt.plot(y_vals, label=f"y signal ({node})")
    #plt.axhline(0.25, color='r', linestyle='--', label="Threshold")
    plt.xlabel("Time step")
    plt.ylabel("y")
    plt.title(f"Adaptive Controller 'y' evolution for {node}")
    plt.legend()
    plt.grid(True)
    plt.show()

def plot_epsilon():
    plt.figure(figsize=(8,4))
    plt.plot(epsion_vals, label=f"epsilon signal ({node})")
    #plt.axhline(0.25, color='r', linestyle='--', label="Threshold")
    plt.xlabel("Time step")
    plt.ylabel("epsilon")
    plt.title(f"Adaptive Controller 'epsilon' evolution for {node}")
    plt.legend()
    plt.grid(True)
    plt.show()

def plot_up():
    plt.figure(figsize=(8,4))
    plt.plot(up_vals, label=f"up signal ({node})")
    #plt.axhline(0.25, color='r', linestyle='--', label="Threshold")
    plt.xlabel("Time step")
    plt.ylabel("up")
    plt.title(f"Adaptive Controller bp shift evolution for {node}")
    plt.legend()
    plt.grid(True)
    plt.show()

def plot_uq():
    plt.figure(figsize=(8,4))
    plt.plot(uq_vals, label=f"uq signal ({node})")
    #plt.axhline(0.25, color='r', linestyle='--', label="Threshold")
    plt.xlabel("Time step")
    plt.ylabel("uq")
    plt.title(f"Adaptive Controller 'uq' evolution for {node}")
    plt.legend()
    plt.grid(True)
    plt.show()

if plot_y_flag:
    plot_y()
if plot_epsilon_flag:
    plot_epsilon()
if plot_up_flag:
    plot_up()
if plot_uq_flag:
    plot_uq()