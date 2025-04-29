import os
import sys
import pandas as pd
import subprocess
from datetime import datetime
from obsplus import WaveBank
from utils.slurmtaskwritter import write_slurm_script
from utils.run_logger import update_completed_step
from obspy.core.event.catalog import _read
from modules.correlator import Correlator

def load_parameters(parameter_file):
    parameters = {}
    if os.path.exists(parameter_file):
        with open(parameter_file, 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    parameters[key.strip()] = value.strip()
    return parameters

def run_correlator(run_dir, parameters):
    archive_path = "/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/ARCHIVE"
    catalog_path = os.path.join(run_dir, "catalog_w_magnitudes.cat")
    catalog = _read(catalog_path, format="QUAKEML")
    bank = WaveBank(archive_path)

    lowcut=float(parameters.get('lowcut'))
    highcut=float(parameters.get('highcut'))
    dt_length = float(parameters.get("dt_length"))
    dt_prepick = float(parameters.get("dt_prepick"))
    shift_len = float(parameters.get('shift_len'))
    max_sep = float(parameters.get('max_sep'))
    min_link = float(parameters.get('min_link'))
    min_cc = float(parameters.get('dt_min_cc'))

    dtcc_path = os.path.join(run_dir, "dt.cc")
    correlator = Correlator(
        minlink=min_link,
        min_cc=min_cc,
        maxsep=max_sep,
        shift_len=shift_len,
        pre_pick=dt_prepick,
        length=dt_length,
        lowcut=lowcut,
        highcut=highcut,
        interpolate=True,
        client=bank,  # Or any client-like object - if using a wavebank, the wavebank needs to exist
        max_event_links=None,  # Limit to correlate to only the n nearest events, can be set to None to run everything
        outfile=dtcc_path,
        weight_by_square=True)

    correlator.add_events(catalog)

    print(f"Correlation completed successfully. Output saved to {dtcc_path}")

def depurate_dtcc(params, run_dir):
    dtcc_file = os.path.join(run_dir, 'dt.cc')
    backup_file = os.path.join(run_dir, 'dtcc.backup')

    dt_min_cc_sq = float(params.get("dt_min_cc"))**2
    min_link = float(params.get('min_link'))
    shift_len = float(params.get('shift_len'))

    cleaned_data = []

    if not os.path.exists(backup_file):
        os.rename(dtcc_file,backup_file)
    
    with open(backup_file, 'r') as f:
        lines = f.readlines()

    current_event_pair = None
    event_pair_data = []

    for line in lines:
        if line.startswith('#'):
            # If there's an existing event pair, process and clean it
            if current_event_pair:
                cleaned_event_pair = process_event_pair(event_pair_data, dt_min_cc_sq, min_link, shift_len)
                if cleaned_event_pair:
                    cleaned_data.append(current_event_pair)
                    cleaned_data.extend(cleaned_event_pair)
            
            # Start new event pair
            current_event_pair = line.strip()
            event_pair_data = []
        else:
            event_pair_data.append(line.strip())
    
    # Process last event pair
    if current_event_pair:
        cleaned_event_pair = process_event_pair(event_pair_data, dt_min_cc_sq, min_link, shift_len)
        if cleaned_event_pair:
            cleaned_data.append(current_event_pair)
            cleaned_data.extend(cleaned_event_pair)
    
    # Write cleaned dt.cc file
    with open(dtcc_file, 'w') as f:
        f.write("\n".join(cleaned_data) + "\n")
    
    print(f"Depuration complete. Cleaned file saved as: {dtcc_file}, backup saved as: {backup_file}")

def process_event_pair(event_data, dt_min_cc_sq, min_link, shift_len):
    """
    Cleans the individual event pair by filtering and deduplicating S-phases.
    
    :param event_data: List of lines containing station data
    :param dt_min_cc: Minimum correlation threshold
    :param min_link: Minimum required number of stations
    :return: Cleaned event data list or None if the event pair should be removed
    """
    df = []
    
    for line in event_data:
        parts = line.split()
        if len(parts) != 4:
            continue  # Skip malformed lines
        
        station, time_lag, corr, phase = parts
        
        try:
            time_lag = float(time_lag)
            corr = float(corr)
        except ValueError:
            continue  # Skip lines with invalid numeric values
        
        # Apply correlation threshold
        if (corr >= dt_min_cc_sq) and (time_lag <= shift_len):
            df.append((station, time_lag, corr, phase))
    
    # Convert to DataFrame for processing
    df = pd.DataFrame(df, columns=['station', 'time_lag', 'corr', 'phase'])
    
    # Deduplicate S-phases per station, keeping the highest correlation
    df = df.sort_values(by=['station', 'phase', 'corr'], ascending=[True, True, False])
    df = df.drop_duplicates(subset=['station', 'phase'], keep='first')
    
    # Ensure min_link is respected
    if len(df) < min_link:
        return None  # Remove the event pair entirely
    
    # Convert back to list format
    return [f"{row['station']} {row['time_lag']:.3f} {row['corr']:.4f} {row['phase']}" for _, row in df.iterrows()]

def write_growclust_runfile(swarm_name, run_dir):
    file_text = f"""****  GrowClust Control File  *****
******   {swarm_name} Swarm Sequence   *******
********  Hugo Arteaga, 2025   **********
*******************************************
*
*******************************************
*************  Event list  ****************
*******************************************
* evlist_fmt (1 = phase, 2 = GrowClust, 3 = HypoInverse)
2
* fin_evlist (event list file name)
event_file.txt
*
*******************************************
************   Station list   *************
*******************************************
* stlist_fmt (1: station name, 2: incl. elev)
2
* fin_stlist (station list file name)
/hpceliasrafn/haa53/EQcorrscan_pipeline/EQCorrPipeline/metadata/askja_qm_stations_with_elev.txt
*
*******************************************
*************   XCOR data   ***************
*******************************************
* xcordat_fmt (1 = text), tdif_fmt (21 = tt2-tt1, 12 = tt1-tt2)
1  12
* fin_xcordat
{run_dir}/dt.cc
*
*******************************************
*** Velocity Model / Travel Time Tables ***
*******************************************
* ttabsrc: travel time table source ("trace" or "nllgrid")
trace
* fin_vzmdl (model name)
/hpceliasrafn/haa53/EQcorrscan_pipeline/EQCorrPipeline/metadata/askja_qm_vm.txt
* fdir_ttab (directory for travel time tables/grids or NONE)
tt/
* projection (proj, ellps, lon0, lat0, rotANG, [latP1, latP2])
lcc WGS84 -16.6 65.1 0.0 64.9 65.3
******************************************
***** Travel Time Table Parameters  ******
******************************************
* vpvs_factor  rayparam_min
  1.760             0.0
* tt_zmin  tt_zmax  tt_zstep
  -2.0        21.0       0.5
* tt_xmin  tt_xmax  tt_xstep
   0.0       100.0      1.0
*
******************************************
***** GrowClust Algorithm Parameters *****
******************************************
* rmin  delmax rmsmax 
   0.6    80    0.1
* rpsavgmin, rmincut  ngoodmin   iponly 
    0          0         0        0
*
******************************************
************ Output files ****************
******************************************
* nboot  nbranch_min
   0         2
* fout_cat (relocated catalog)
out/out.trace1D.cat
* fout_clust (relocated cluster file)
out/out.trace1D.clust
* fout_log (program log)
out/out.trace1D.log
* fout_boot (bootstrap distribution)
NONE
******************************************
******************************************
"""
    file_path = os.path.join(run_dir, "swarm_relocation.inp")
    with open(file_path, 'w') as f:
        f.write(file_text)

def run_relocations(swarm_name, run_dir):

    # Load parameter file to get relocation partition/time
    param_path = f"/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/swarms/{swarm_name}/parameters{swarm_name}.txt"
    params = {}
    try:
        with open(param_path, 'r') as f:
            for line in f:
                if line.strip() and not line.startswith("#") and '=' in line:
                    key, value = line.strip().split("=", 1)
                    params[key.strip()] = value.strip()
    except FileNotFoundError:
        print(f"Warning: Parameter file not found for {swarm_name}. Using default relocation resources.")
    
    partition = params.get("relocation_partition_string", "48cpu_192mem,64cpu_256mem")
    time = params.get("relocation_time", "3-00:00:00")

    print(f"Using relocation SLURM partition: {partition}")
    print(f"Using relocation SLURM time: {time}")

    write_growclust_runfile(swarm_name, run_dir)
    write_slurm_script(swarm_name, run_dir, type="relocate", file_name='slurm_relocate.sh', partition_string=partition, time=time)
    script_path = os.path.join(run_dir, 'slurm_relocate.sh')
    result = subprocess.run(['sbatch', script_path], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Job submitted successfully: {result.stdout}")
    else:
        print(f"Error in submitting job: {result.stderr}")

if __name__ == "__main__":
    
    swarms_directrory = "/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/swarms"

    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python execute_correlator.py <swarm_name> <run_directory>")
        sys.exit(1)

    swarm_name = sys.argv[1]
    run_dir = sys.argv[2]
    swarm_dir= os.path.join(swarms_directrory, swarm_name)

    parameter_file = os.path.join(swarm_dir, f"parameters{swarm_name}.txt")

    if not os.path.exists(parameter_file):
        raise FileNotFoundError(f"Parameter file {parameter_file} not found.")
    
    parameters = load_parameters(parameter_file)

    try:
        starttime = datetime.now()
        run_correlator(run_dir, parameters)
        update_completed_step(run_dir, "Correlations", starttime)
    except Exception as e:
        print(f"Error during correlation: {e}")
        sys.exit(1)

    try:
        starttime = datetime.now()
        depurate_dtcc(parameters, run_dir)
        update_completed_step(run_dir, "Depurate Correlations", starttime)
    except Exception as e:
        print(f"Error during dt.cc depuration: {e}")
        sys.exit(1)

    print(f"Correlation step completed successfully for {swarm_name}.")

    run_relocations(swarm_name, run_dir)



