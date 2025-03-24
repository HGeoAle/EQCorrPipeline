import os
import json
import pandas as pd
from datetime import datetime

def initialize_run_file(run_dir, parameters, pipeline_version):
    """
    Creates a new run file with basic metadata and parameter values.
    """
    run_file = os.path.join(run_dir, "run_file.json")

    # Ensure the run directory exists
    os.makedirs(run_dir, exist_ok=True)

    run_data = {
        "pipeline_version" : pipeline_version,
        "parameters": parameters,
        "completed_steps": []
    }

    with open(run_file, 'w') as f:
        json.dump(run_data, f, indent=4)

    print(f"✅ Initialized run file at {run_file}")

def update_completed_step(run_dir, step_name, start_time, count_dict=None):
    """
    Adds a completed step to the run file with a timestamp and optional multiple counts.
    count_dict should be a dictionary, e.g.:
        {"templates_generated": 45, "stations_used": 12}
    """
    run_file = os.path.join(run_dir, "run_file.json")

    if not os.path.exists(run_file):
        raise FileNotFoundError(f"Run file {run_file} not found.")

    with open(run_file, 'r') as f:
        run_data = json.load(f)

    endtime = datetime.now()
    step_entry = {
        "step": step_name,
        "starttime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "endtime": endtime.strftime("%Y-%m-%d %H:%M:%S"),
        "duration": str(endtime-start_time),
        "counts": count_dict if count_dict else {}
    }

    run_data["completed_steps"].append(step_entry)

    with open(run_file, 'w') as f:
        json.dump(run_data, f, indent=4)

    print(f"✅ Logged step '{step_name}' as completed with counts: {count_dict if count_dict else 'N/A'}")

def log_run_step(run_dir, step_name, duration, count_dict=None):
    """
    Logs the execution time and output count of a pipeline step.
    count_dict should be a dictionary of counts.
    """
    log_file = os.path.join(run_dir, "run_log.csv")

    # Convert count dictionary into a string representation for CSV storage
    count_str = json.dumps(count_dict) if count_dict else "N/A"

    log_entry = pd.DataFrame([[step_name, duration, count_str, datetime.now().strftime("%Y-%m-%d %H:%M:%S")]],
                             columns=["Step", "Duration (s)", "Count", "Timestamp"])
    
    if os.path.exists(log_file):
        log_entry.to_csv(log_file, mode='a', header=False, index=False)
    else:
        log_entry.to_csv(log_file, index=False)

    # Also update the run file for consistency
    update_completed_step(run_dir, step_name, count_dict)