import os
import subprocess
import sys
from datetime import datetime
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
    return run_dir

def submit_slurm_job(run_dir, file_name="slurm_script.sh"):
    slurm_script_path = os.path.join(run_dir, file_name)
    result = subprocess.run(['sbatch', slurm_script_path], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Job submitted successfully: {result.stdout}")
    else:
        print(f"Error in submitting job: {result.stderr}")

def main(swarm_name, run_code):
    
    # Step 1: Create directories
    run_dir = find_run_directory(swarm_name, run_code)

    # Step 2: Write the SLURM script
    write_slurm_script(swarm_name, run_dir, type="rerun", file_name="slurm_rerun_script.sh")
    
    # Step 3: Submit the SLURM job
    submit_slurm_job(run_dir,file_name="slurm_rerun_script.sh")
    
    print(f"SLURM job submitted for {swarm_name}. Logs will be in {run_dir}/slurm-%j.out and {run_dir}/slurm-%j.err.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python execute_rerun.py <swarm_name> <run_code>")
        sys.exit(1)
    
    swarm_name = sys.argv[1]
    run_code = sys.argv[2]
    main(swarm_name, run_code)