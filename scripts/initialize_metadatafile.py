import os
import pandas as pd
import re

# Directories
swarm_dir = "/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/swarms"
metadata_file = "/hpceliasrafn/haa53/EQcorrscan_pipeline/h_eq_fmf/postwork/swarm_metadata.csv"

# Function to find the highest numbered successful run
def find_latest_successful_run(swarm_path):
    runs = [d for d in os.listdir(swarm_path) if d.startswith('run_') and os.path.isdir(os.path.join(swarm_path, d))]
    
    # Filter out runs with non-numeric suffixes
    valid_runs = [r for r in runs if re.match(r'run_\d+$', r)]
    
    # Sort based on numeric part
    runs_sorted = sorted(valid_runs, key=lambda x: int(x.split('_')[1]), reverse=True)
    
    for run in runs_sorted:
        run_path = os.path.join(swarm_path, run)
        out_trace_path = os.path.join(run_path, 'out', 'out.trace1D.cat')
        
        if os.path.exists(out_trace_path):
            return run_path, out_trace_path
    
    return None, None

# Load existing metadata
if os.path.exists(metadata_file):
    metadata_df = pd.read_csv(metadata_file)
else:
    metadata_df = pd.DataFrame(columns=['swarm_name', 'output_catalog', 'input_catalog', 'growclust_catalog'])

# Update output catalog paths based on the latest successful run
for index, row in metadata_df.iterrows():
    swarm_name = row['swarm_name']
    swarm_path = os.path.join(swarm_dir, swarm_name, "h_eq_pipeline_runs")
    latest_run_path, new_output_catalog = find_latest_successful_run(swarm_path)
    if new_output_catalog:
        metadata_df.at[index, 'output_catalog'] = new_output_catalog

# Save updated metadata
metadata_df.to_csv(metadata_file, index=False)

print("Swarm metadata updated with latest successful run output catalog paths.")