import os
import sys
import pandas as pd
from datetime import datetime

# Define the base swarm directory
SWARM_DIR = "/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/swarms"

def load_parameters(swarm_name):
    """Load parameters from the parameter file."""
    param_file = os.path.join(SWARM_DIR, swarm_name, f"parameters{swarm_name}.txt")

    if not os.path.exists(param_file):
        raise FileNotFoundError(f"Parameter file not found: {param_file}")

    parameters = {}
    with open(param_file, 'r') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                key, value = line.split('=', 1)
                parameters[key.strip()] = value.strip()

    return parameters, param_file

def save_parameters(param_file, parameters):
    """Save updated parameters back to the file."""
    with open(param_file, 'w') as f:
        for key, value in parameters.items():
            f.write(f"{key}={value}\n")

def log_change(swarm_name, changes, comment):
    """Log parameter changes to the swarm-specific history file."""
    swarm_path = os.path.join(SWARM_DIR, swarm_name)
    history_file = os.path.join(swarm_path, "parameter_history.csv")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Ensure the swarm directory exists
    if not os.path.exists(swarm_path):
        raise FileNotFoundError(f"Swarm directory not found: {swarm_path}")

    # Convert changes dictionary to a string
    changes_str = str(changes)

    # Load existing history if available
    if os.path.exists(history_file):
        history_df = pd.read_csv(history_file)
    else:
        history_df = pd.DataFrame(columns=["date", "parameters", "comments"])

    # Append new entry
    new_entry = pd.DataFrame([{
        "date": timestamp,
        "parameters": changes_str,
        "comments": comment
    }])

    history_df = pd.concat([history_df, new_entry], ignore_index=True)
    history_df.to_csv(history_file, index=False)

def update_parameters(swarm_name, changes, comment):
    """Main function to update parameters and log changes."""
    parameters, param_file = load_parameters(swarm_name)

    # Apply changes
    for key, value in changes.items():
        if key in parameters:
            print(f"Updating {key}: {parameters[key]} → {value}")
        else:
            print(f"Adding new parameter: {key}={value}")
        parameters[key] = value

    # Save updated parameters
    save_parameters(param_file, parameters)

    # Log the change to the swarm's specific history file
    log_change(swarm_name, changes, comment)

    print(f"✅ Parameters updated and logged for swarm {swarm_name}")

if __name__ == "__main__":
    # Example usage: update_parameters.py Mar2014 "min_snr= 2.0" "min_link=3"  "Lower SNR and min link to keep more channels"")
    if len(sys.argv) < 4:
        print("Usage: python update_parameters.py <swarm_name> <param1=value1 param2=value2 ...> <comment>")
        sys.exit(1)

    swarm_name = sys.argv[1]
    param_changes = {}
    for arg in sys.argv[2:-1]:
        key, value = arg.split("=", 1)
        param_changes[key] = value
    comment = sys.argv[-1]

    update_parameters(swarm_name, param_changes, comment)
1
