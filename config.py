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
bp_override = [0.95, 0.95, 0.95, 0.95, 0.95]  # Example breakpoint override
#bp_override = 0.5  # Example breakpoint override

hacks_list = [
    #[100, 200, 0.8, bp_override, hack_nodes],
    [100, 200, 0.3, bp_override, hack_nodes],
    [120, 260, 0.5, bp_override, hack_nodes2],
    [150, 250, 0.5, bp_override, hack_nodes2],
]
