import os
import subprocess
import sys
from datetime import datetime
from utils.slurmtaskwritter import write_slurm_script

base_dir = "/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/swarms"
def create_run_directory(swarm_name):
    # Create the swarm directory if it doesn't exist
    swarm_dir = os.path.join(base_dir, swarm_name)
    os.makedirs(swarm_dir, exist_ok=True)
    
    # Create the run directory with an incrementing run_id
    run_id = 1
    date_str = datetime.now().strftime("%Y%m%d")
    run_dir = os.path.join(swarm_dir, f"run_{date_str}_{run_id}")
    while os.path.exists(run_dir):
        run_id += 1
        run_dir = os.path.join(swarm_dir, f"run_{date_str}_{run_id}")
    
    os.makedirs(run_dir)
    return run_dir

def load_parameter_file(swarm_name):
    swarm_dir = os.path.join(base_dir, swarm_name)
    param_path = os.path.join(swarm_dir, f"parameters{swarm_name}.txt")
    params = {}
    try:
        with open(param_path, 'r') as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    params[key.strip()] = value.strip()
    except FileNotFoundError:
        print(f"Warning: Parameter file for {swarm_name} not found. Using defaults.")
    return params

def submit_slurm_job(run_dir):
    slurm_script_path = os.path.join(run_dir, "slurm_script.sh")
    result = subprocess.run(['sbatch', slurm_script_path], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Job submitted successfully: {result.stdout}")
    else:
        print(f"Error in submitting job: {result.stderr}")

def main(swarm_name):
    
    # Step 1: Create directories
    run_dir = create_run_directory(swarm_name)

    # Step 2: Load parameters
    params = load_parameter_file(swarm_name)
    if "pipeline_partition_string" not in params:
        print("Using default partition string for pipeline.")
    if "pipeline_time" not in params:
        print("Using default time limit for pipeline.")
    partition = params.get("pipeline_partition_string", "gpu-1xA100,gpu-2xA100,gpu-8xA100")
    time_limit = params.get("pipeline_time", "2-00:00:00")

    

    # Step 3: Write the SLURM script
    write_slurm_script(swarm_name, run_dir, time=time_limit, partition_string=partition)
    
    # Step 3: Submit the SLURM job
    submit_slurm_job(run_dir)
    
    print(f"SLURM job submitted for {swarm_name}. Logs will be in {run_dir}/slurm-%j.out and {run_dir}/slurm-%j.err.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python execute_run.py <swarm_name>")
        sys.exit(1)
    
    swarm_name = sys.argv[1]
    main(swarm_name)