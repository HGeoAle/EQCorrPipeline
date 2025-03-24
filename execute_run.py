import os
import subprocess
import sys
from datetime import datetime
from utils.slurmtaskwritter import write_slurm_script

def create_run_directory(swarm_name):
    # Create the swarm directory if it doesn't exist
    base_dir = "/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/swarms"
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

    # Step 2: Write the SLURM script
    write_slurm_script(swarm_name, run_dir)
    
    # Step 3: Submit the SLURM job
    submit_slurm_job(run_dir)
    
    print(f"SLURM job submitted for {swarm_name}. Logs will be in {run_dir}/slurm-%j.out and {run_dir}/slurm-%j.err.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python execute_run.py <swarm_name>")
        sys.exit(1)
    
    swarm_name = sys.argv[1]
    main(swarm_name)