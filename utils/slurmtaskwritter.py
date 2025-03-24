import os

def write_slurm_script(swarm_name, run_dir, partition_string="gpu-1xA100,gpu-2xA100", time="2-00:00:00", type="new_run", file_name="slurm_script.sh"):
    job_name = f"{swarm_name}_{type}"

    if type == "new_run":
        section1 = """source /hpcapps/lib-mimir/software/Anaconda3/2021.11/etc/profile.d/conda.sh
conda activate hugo_eqscan_develop"""
        section2 = f"python /hpceliasrafn/haa53/EQcorrscan_pipeline/EQCorrPipeline/Pipeline.py {swarm_name} {run_dir}"
    elif type == "rerun":
        section1 = """source /hpcapps/lib-mimir/software/Anaconda3/2021.11/etc/profile.d/conda.sh
conda activate hugo_eqscan_develop"""
        section2 = f"python /hpceliasrafn/haa53/EQcorrscan_pipeline/EQCorrPipeline/Pipeline.py {swarm_name} {run_dir} rerun"
    elif type == "correlate":
        section1 = """source /hpcapps/lib-mimir/software/Anaconda3/2021.11/etc/profile.d/conda.sh
conda activate hugo_eqscan_develop"""
        section2 = f"python /hpceliasrafn/haa53/EQcorrscan_pipeline/EQCorrPipeline/execute_correlator.py {swarm_name} {run_dir}"
    elif type == "relocate":
        section1 = """module use /hpcapps/lib-edda/modules/all/Core
module use /hpcapps/lib-geo/modules/all
module load GrowClust3D.jl"""
        section2 = f"""cd {run_dir}
start_time=$(date +"%Y-%m-%d %H:%M:%S")
julia -t64 /hpceliasrafn/haa53/EQcorrscan_pipeline/EQCorrPipeline/run_growclust3D.jl swarm_relocation.inp
python /hpceliasrafn/haa53/EQcorrscan_pipeline/EQCorrPipeline/scripts/update_relocate_status.py {run_dir} "$start_time" """ 
    else:
        raise ValueError(f"Unknown pipeline type: {type}")
    


    slurm_script = f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --mail-type=ALL
#SBATCH --mail-user=haa53@hi.is
#SBATCH --partition={partition_string}
#SBATCH --time={time}
#SBATCH --output={run_dir}/slurm-%j.out
#SBATCH --error={run_dir}/slurm-%j.err

{section1}
{section2}
"""
    script_path = os.path.join(run_dir, file_name)
    
    with open(script_path, 'w') as f:
        f.write(slurm_script)