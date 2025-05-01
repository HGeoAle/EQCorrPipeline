import os
import subprocess
import sys
import json
from utils.slurmtaskwritter import write_slurm_script

def find_run_directory(swarm_name,run_code):
    # Create the swarm directory if it doesn't exist
    base_dir = "/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/swarms"
    swarm_dir = os.path.join(base_dir, swarm_name)
    run_dir = os.path.join(swarm_dir, run_code)
    if os.path.exists(run_dir):
        return run_dir
    else:
        print(f"Could not find run {run_code} for swarm {swarm_name}. Expected directory {run_dir} not found")
        sys.exit(1)

def load_run_status(run_dir):
    run_file = os.path.join(run_dir, "run_file.json")
    if not os.path.exists(run_file):
        print(f"Run file not found in {run_dir}. Cannot determine rerun step.")
        sys.exit(1)

    with open(run_file, 'r') as f:
        run_data = json.load(f)

    completed = [s["step"] for s in run_data.get("completed_steps", [])]
    step_order = ['Tribe_construction', 'Detection', 'Declustering', 'Lag_calc', 'Magnitudes', "Correlations", "Depurate Correlations", "Relocations"]
    for step in reversed(step_order):
        if step in completed:
            return step
    return None

def load_parameters(swarm_name):
    param_path = f"/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/swarms/{swarm_name}/parameters{swarm_name}.txt"
    params = {}
    try:
        with open(param_path, 'r') as f:
            for line in f:
                if line.strip() and not line.startswith("#") and '=' in line:
                    key, value = line.strip().split("=", 1)
                    params[key.strip()] = value.strip()
    except FileNotFoundError:
        print(f"Warning: Parameter file for {swarm_name} not found. Using defaults.")
    return params


def submit_slurm_job(run_dir, file_name="slurm_script.sh"):
    slurm_script_path = os.path.join(run_dir, file_name)
    result = subprocess.run(['sbatch', slurm_script_path], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Job submitted successfully: {result.stdout}")
    else:
        print(f"Error in submitting job: {result.stderr}")

def main(swarm_name, run_code):
    
    run_dir = find_run_directory(swarm_name, run_code)

    last_step = load_run_status(run_dir)
    print(f"Last completed step: {last_step}")

    # Default to CPU
    partition = "any_cpu"
    time = "0-02:00:00"

    if last_step not in ["Magnitudes","Correlations", "Depurate Correlations", "Relocations"]:
        print("Heavy step required — using GPU partition.")
        params = load_parameters(swarm_name)
        if "pipeline_partition_string" not in params:
            print("Warning: 'pipeline_partition_string' not found in parameters. Using default.")
        partition = params.get("pipeline_partition_string", "gpu-1xA100,gpu-2xA100")
        time = params.get("pipeline_partition_time", "2-00:00:00")
 
    print(f"Using SLURM partition: {partition}")
    print(f"Using SLURM time limit: {time}")
    write_slurm_script(swarm_name, run_dir, type="rerun", file_name="slurm_rerun_script.sh",partition_string=partition, time=time)
    
    submit_slurm_job(run_dir,file_name="slurm_rerun_script.sh")
    
    print(f"SLURM job submitted for {swarm_name}. Logs will be in {run_dir}/slurm-%j.out and {run_dir}/slurm-%j.err.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python execute_rerun.py <swarm_name> <run_code>")
        sys.exit(1)
    
    swarm_name = sys.argv[1]
    run_code = sys.argv[2]
    main(swarm_name, run_code)