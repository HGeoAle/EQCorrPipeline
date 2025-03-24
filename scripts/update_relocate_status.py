import json
import os
import sys
from datetime import datetime

def update_run_status(run_dir, start_time_str):
    """
    Update the run_file.json to mark 'Relocations' as completed.
    Appends a new entry to the 'completed_steps' list with a timestamp.
    """
    run_file = os.path.join(run_dir, "run_file.json")

    if not os.path.exists(run_file):
        print(f"⚠️ Run file not found: {run_file}")
        return

    with open(run_file, 'r') as f:
        run_data = json.load(f)

    # Ensure 'completed_steps' exists
    if "completed_steps" not in run_data:
        run_data["completed_steps"] = []

    start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.now()
    duration = str(end_time - start_time)
    
    # Create the new step entry
    relocation_entry = {
        "step": "Relocations",
        "starttime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "endtime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "duration": duration,
        "counts": {
            "events_relocated": None  # Placeholder, update manually if needed
        }
    }

    # Append to completed steps
    run_data["completed_steps"].append(relocation_entry)

    # Save back to file
    with open(run_file, 'w') as f:
        json.dump(run_data, f, indent=4)

    print(f"✅ 'Relocations' step successfully logged in {run_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python update_relocate_status.py <run_directory> <start_time>")
        sys.exit(1)

    update_run_status(sys.argv[1], sys.argv[2])