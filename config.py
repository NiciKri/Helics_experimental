# config.py

# Base directory for your simulation
BASE_DIR = r"C:/Users/nicol/Helics_experimental"

# Data directory (you can adjust this if needed)
DATA_DIR = BASE_DIR + r"/data"

# Simulation parameters
SIMULATION_TIME = 300  # Total simulation time in seconds
TIME_STEP = 1.0        # Time step in seconds

# Scaling factors for the simulation
Sbar_scaling = 1.1

hack_nodes2 = ["s701a", "s701b", "s701c"]
hack_nodes = ["s701a", "s701b"]
#hack_nodes = node_names  # Uncomment to attack all nodes
bp_override = [0.994, 0.995, 0.995, 0.996, 0.997] # Example breakpoint override
bp_701a = [0.974351955, 1.004351955, 1.004351955, 1.034351955, 1.064351955]

hacks_list = [
    #[100, 200, 0.4, bp_override, hack_nodes],
    #[100, 200, 1.0, bp_override, 1.0],
    [150, 250, 0.5, None, hack_nodes2],
    #[100, 200, 0.5, bp_override, ["s701a"]],
    #[100, 180, 0.5, bp_701a, ["s701a"]],
]
