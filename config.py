# config.py

"""for no attacks, use hacks_list = []
for no adaptive controller, set threshold in federate to 1e6"""

# Base directory for your simulation
BASE_DIR = r"C:/Users/nicol/Helics_experimental"

# Data directory (you can adjust this if needed)
DATA_DIR = BASE_DIR + r"/data"

SAVE_LOGS = False  # Set to True to save logs, False to skip saving

# Simulation parameters
SIMULATION_TIME = 300  # Total simulation time in seconds, hardcoded max time 500 inC:\Users\nicol\Helics_experimental\Environment\DRLController.py
# 5-10 minutes is a realistic simulation time for testing
ACTION_INTERVAL = 10  # How often to apply the action (in seconds), 10-30 is realistic

TIME_STEP = 1.0        # Time step in seconds, also hardcoded in some federates

# Scaling factors for data loading
start_time = 0  # Start time for data loading, can be adjusted
solar_scaling_factor = 1.0  # Scaling factor for solar data
load_scaling_factor = 1.0  # Scaling factor for load data

TARGET_NODE = "s701"

# Scaling factors for the simulation
Sbar_scaling = 1.1

#bp_701a = [0.974351955, 1.004351955, 1.004351955, 1.034351955, 1.064351955]

adaptive_controller_on = False  # Set to True to enable adaptive controller, False to disable

hack_nodes = ["s701a", "s701b"]
hack_nodes2 = ["s701a", "s701b", "s701c"]
#hack_nodes = node_names  # Uncomment to attack all nodes
bp_override = [0.994, 0.995, 0.995, 0.996, 0.997] # Example breakpoint override

hacks_list = [
    #[80, 150, 0.5, bp_override, hack_nodes2],
    #[None, None, None, bp_override, 3],
    [100, 250, 0.5, bp_override, hack_nodes2],
]
