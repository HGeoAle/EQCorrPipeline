import os
import numpy as np
import pandas as pd
import sys
from eqcorrscan.core.match_filter import Tribe
import matplotlib.pyplot as plt
from obsplus import WaveBank

current_dir = os.path.dirname(os.path.abspath(__file__))
pipeline_root = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(pipeline_root)
from utils.loader import read_catalog_from_csv, check_picks


swarm_dir = "/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/swarms"
bank = WaveBank("/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/ARCHIVE")

for swarm_name in os.listdir(swarm_dir):
    swarm_path = os.path.join(swarm_dir, swarm_name)

    if os.path.isdir(swarm_path):
        parameters = {}
        parameter_file = os.path.join(swarm_path, f"parameters{swarm_name}.txt")

        if os.path.exists(parameter_file):
            with open(parameter_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        parameters[key.strip()] = value.strip()

        catalog_path = parameters.get('catalog_csv')
        if not catalog_path or not os.path.exists(catalog_path):
            print(f"⚠️ Missing catalog file for {swarm_name}, skipping...")
            continue

        cat = read_catalog_from_csv(parameters.get('catalog_csv'))

        if cat is None or len(cat) == 0:
            print(f"⚠️ Empty or unreadable catalog for {swarm_name}, skipping...")
            continue
        
        stations = {pick.waveform_id.station_code for event in cat for pick in event.picks}
        event_times = [event.preferred_origin().time for event in cat if event.preferred_origin()]
        
        if not cat or len(cat) == 0:
            print(f"⚠️ Empty catalog for {swarm_name}, skipping...")
            continue
            
        
        if not event_times:
            print(f"⚠️ No valid origins found for {swarm_name}, skipping...")
            continue

        start_time = min(event_times)
        end_time = max(event_times)

        av = bank.get_availability_df(station=stations, starttime= start_time, endtime = end_time)

        cat = check_picks(cat, av, send_warning=False)

        print(f"🛠 Constructing Tribe for {swarm_name} (No SNR Filtering)...")
        tribe = Tribe().construct(
            method="from_client",
            client_id=bank,
            catalog=cat,
            lowcut=float(parameters.get('lowcut')),
            highcut=float(parameters.get('highcut')),
            samp_rate=int(parameters.get('samp_rate')),
            filt_order=int(parameters.get('filt_order')),
            length=float(parameters.get('length')),
            prepick=float(parameters.get('prepick')),
            swin="all",
            all_horiz=False,
            min_snr=None,
            parallel=True
        )

        snr_data = []

        for template in tribe:
            for trace in template.st:  # Iterate over all traces in the template
                station = trace.stats.station
                channel = trace.stats.channel
                phase = "P" if channel.endswith("Z") else "S" if channel.endswith(("N", "E")) else "Unknown"


                signal_max = np.max(np.abs(trace.data))
                noise_rms = np.sqrt(np.mean(trace.data ** 2))
                snr = signal_max / noise_rms if noise_rms > 0 else np.nan
                snr_data.append({
                    "EventID": template.event.resource_id.id.split('/')[-1], 
                    "Station": station,
                    "Channel": channel,
                    "Phase": phase,
                    "SNR": snr
                })

        snr_df = pd.DataFrame(snr_data)

        output_plot_path = os.path.join(swarm_path, "snr_distribution.png")
        output_plot_path_p = os.path.join(swarm_path, "snr_distribution_P.png")
        output_plot_path_s = os.path.join(swarm_path, "snr_distribution_S.png")

        # General SNR distribution
        plt.figure(figsize=(8, 5))
        plt.hist(snr_df["SNR"].dropna(), bins=50, edgecolor="black", alpha=0.7)
        plt.xlabel("SNR")
        plt.ylabel("Count")
        plt.title("SNR Distribution (All Phases)")
        plt.yscale("log")  # Log scale for better visibility
        plt.grid()
        plt.savefig(output_plot_path)
        plt.close()

        # P-phase distribution
        snr_p = snr_df[snr_df["Phase"] == "P"]
        if not snr_p.empty:
            plt.figure(figsize=(8, 5))
            plt.hist(snr_p["SNR"].dropna(), bins=50, edgecolor="black", alpha=0.7, color="blue")
            plt.xlabel("SNR")
            plt.ylabel("Count")
            plt.title("SNR Distribution (P-Phase)")
            plt.yscale("log")
            plt.grid()
            plt.savefig(output_plot_path_p)
            plt.close()

        # S-phase distribution
        snr_s = snr_df[snr_df["Phase"] == "S"]
        if not snr_s.empty:
            plt.figure(figsize=(8, 5))
            plt.hist(snr_s["SNR"].dropna(), bins=50, edgecolor="black", alpha=0.7, color="red")
            plt.xlabel("SNR")
            plt.ylabel("Count")
            plt.title("SNR Distribution (S-Phase)")
            plt.yscale("log")
            plt.grid()
            plt.savefig(output_plot_path_s)
            plt.close()

        print(f"📊 Saved SNR distribution plots for {swarm_name}")