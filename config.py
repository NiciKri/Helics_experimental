# config.py

"""for no attacks, use hacks_list = []
for no adaptive controller, set threshold in federate to 1e6"""

# Base directory for your simulation
BASE_DIR = r"C:/Users/nicol/Helics_experimental"

# Data directory (you can adjust this if needed)
DATA_DIR = BASE_DIR + r"/data"

SAVE_LOGS = False  # Set to True to save logs, False to skip saving

# Simulation parameters
SIMULATION_TIME = 30  # Total simulation time in seconds, hardcoded max time 500 inC:\Users\nicol\Helics_experimental\Environment\DRLController.py
TIME_STEP = 1.0        # Time step in seconds

# Scaling factors for the simulation
Sbar_scaling = 1.1

bp_701a = [0.974351955, 1.004351955, 1.004351955, 1.034351955, 1.064351955]

hack_nodes = ["s701a", "s701b"]
hack_nodes2 = ["s701a", "s701b", "s701c"]
#hack_nodes = node_names  # Uncomment to attack all nodes
bp_override = [0.994, 0.995, 0.995, 0.996, 0.997] # Example breakpoint override

hacks_list = [
    #[100, 200, 0.4, bp_override, hack_nodes],
    #[100, 250, 0.5, bp_override, hack_nodes2],
]
