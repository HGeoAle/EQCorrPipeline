import os
import json
import pickle
import logging
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import utils.run_logger as run_log

from datetime import datetime
from eqcorrscan import Tribe
from obspy import UTCDateTime
from utils.products import create_catalog_file
from obspy.core.event import Catalog, Magnitude
from utils.slurmtaskwritter import write_slurm_script
from modules.Tribe_constructor import TribeConstructor
from eqcorrscan.utils.mag_calc import relative_magnitude
from modules.client_lag_calc import client_party_lag_calc

from version import __version__
from execute_correlator import run_relocations

metadata_file = "/hpceliasrafn/haa53/EQcorrscan_pipeline/EQCorrPipeline/metadata/swarm_metadata.csv"
swarms_directrory = "/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/swarms"

class EQ_Pipeline:
    def __init__(self, swarm_name, run_dir, run_mode):
        
        self.swarm_name = swarm_name
        self.run_mode = run_mode
        self.pipeline_version = __version__
        self.run_dir = run_dir
        swarm_dir= os.path.join(swarms_directrory, swarm_name)

        # Parameters Load
        if run_mode == "new_run":
            parameter_file = os.path.join(swarm_dir, f"parameters{swarm_name}.txt")
            self.parameters = self._load_parameters(parameter_file)
        elif run_mode == "rerun":
            run_file = os.path.join(self.run_dir, "run_file.json")
            with open(run_file, 'r') as f:
                run_data = json.load(f)

            self.parameters = run_data.get("parameters", {})
            ### THERE HAS TO BE SOME CHECKS IN CASE OF CHANGE OF PARAMETERS IN A RERUN # implement later
        
       


        # Contruction of the Tribe is manage by another module since it has it's own complexity and diagnostics 
        self.tribe_constructor = None
        # Last Version of the party
        self.party = None
        # Ditionary of Self-Detections for each template
        self.self_detections = {}
        # Output Catalog
        self.out_catalog = Catalog()
        # Run directory
        self.run_path = ""

    def __repr__(self):
        return f"EQ_Pipeline(swarm_name={self.swarm_name})"
    

    def new_run(self):
        
        run_log.initialize_run_file(self.run_dir,self.parameters, self.pipeline_version)

        # Generate and run tribe constructor
        starttime = datetime.now()
        self.construct_tribe()
        loaded_events = len(self.tribe_constructor.catalog)
        generated_templates = len(self.tribe_constructor.tribe)
        count1 = {'loaded_events':loaded_events, "generated_templates":generated_templates}
        run_log.update_completed_step(self.run_dir,"Tribe_construction",starttime, count1)

        # Detect
        starttime = datetime.now()
        self.detect()
        detection_count = 0
        non_zero_families = 0
        for family in self.party:
            f_num_detections = len(family.detections)
            if f_num_detections > 0:
                detection_count += f_num_detections
                non_zero_families += 1
        count2 = {'families':non_zero_families,
                  'detections': detection_count}
        run_log.update_completed_step(self.run_dir,"Detection",starttime,count2)

        # Decluster
        starttime = datetime.now()
        self.decluster_party()
        channel_count = 0
        detection_count = 0
        non_zero_families = 0
        for family in self.party:
            f_num_detections = len(family.detections)
            if f_num_detections > 0:
                detection_count += f_num_detections
                non_zero_families += 1
                for detection in family.detections:
                    channel_count += detection.no_chans
        count3 = {'families':non_zero_families,
                  'detections': detection_count,
                  'channels': channel_count}
        run_log.update_completed_step(self.run_dir,"Declustering",starttime,count3)

        # Lag_calc
        starttime = datetime.now()
        self.do_lag_calc()
        detection_w_picks = 0
        non_zero_families = 0
        channel_count = 0
        picks_count = 0
        for family in self.party:
            f_detection_w_picks = 0
            for event in family.catalog:
                channels = set()
                if len(event.picks) > 0:
                    f_detection_w_picks +=1
                    for pick in event.picks:
                        cha = pick.waveform_id.station_code + pick.waveform_id.channel_code
                        channels.add(cha)
                        picks_count += 1
                    channel_count += len(channels)
            if f_detection_w_picks > 0:
                non_zero_families += 1
                detection_w_picks += f_detection_w_picks
        count4 = {'families': non_zero_families,
                  'events_w_picks': detection_w_picks,
                  "channels": channel_count,
                  "picks": picks_count}
        run_log.update_completed_step(self.run_dir,"Lag_calc",starttime,count4)

        # Magnitudes
        starttime = datetime.now()
        self.get_relative_magnitudes()
        events_w_magnitudes = len(self.out_catalog)
        channel_count = 0
        picks_count = 0
        for event in self.out_catalog:
            channels = set()
            for pick in event.picks:
                cha = pick.waveform_id.station_code + pick.waveform_id.channel_code
                channels.add(cha)
                picks_count += 1
            channel_count += len(channels)
        count5 = {"events_w_magnitudes": events_w_magnitudes,
                  "channels": channel_count,
                  "picks": picks_count}
        run_log.update_completed_step(self.run_dir,"Magnitudes", starttime, count5)  

        # Export Catalog file for relocations
        self.generate_event_textfile()
        time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Basic pipeline of {self.swarm_name} completed, {time}")

        self.correlator_run()


    def _load_parameters(self, parameter_file):
        parameters = {}
        if os.path.exists(parameter_file):
            with open(parameter_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        parameters[key.strip()] = value.strip()
            return parameters
        raise FileNotFoundError(f"Parameter {parameter_file} file for swarm {self.swarm_name} not found.")

    def _load_metadata(self):
        if os.path.exists(self.metadata_file):
            row = pd.read_csv(self.metadata_file, index_col='swarm_name').loc[self.swarm_name]
            return row.to_dict()
        raise FileNotFoundError(f"Metadata for swarm {self.swarm_name} not found.")
    
    
    def construct_tribe(self):
        self.tribe_constructor = TribeConstructor(self.parameters, self.run_dir)
        self.tribe_constructor.run()

    def detect(self):
        tribe = self.tribe_constructor.tribe
        bank = self.tribe_constructor.bank
        params = self.parameters
        starttime = UTCDateTime(params.get('starttime'))
        endtime = UTCDateTime(params.get('endtime'))
        threshold = float(params.get('threshold'))
        threshold_type = params.get('threshold_type')
        trig_int = float(params.get('detect_trig_int'))
        self.party = tribe.client_detect(
            client = bank,
            starttime = starttime,
            endtime = endtime,
            threshold = threshold,
            threshold_type = threshold_type,
            trig_int = trig_int,
            xcorr_func = 'fmf',
            concurrent_processing = True,
            parallel_process = True,
            export_cccsums = False,
            ignore_bad_data = True
        )

        self.export_party(name="Party_pre-decluster.pkl")
    
    def export_party(self, name="party.pkl"):
        path = os.path.join(self.run_dir, name)
        pkl_output = open(path, 'wb')
        pickle.dump(self.party, pkl_output)
        pkl_output.close()


    def decluster_party(self):
        starttime = UTCDateTime(self.parameters.get('starttime'))
        endtime = UTCDateTime(self.parameters.get('endtime'))
        decluster_trig_int = float(self.parameters.get('decluster_trig_int'))
        min_chans = int(self.parameters.get('min_chans'))

        selfdetections_predecluster = {}
        selfdetections = {}
        party = self.party
        for family in party:
         if not family.detections:
              logging.warning(f"Template {family.template.name} has no detections. Not used")
        party = party.filter(dates=[starttime, endtime])
        party.sort()
        
        fig01, ax = plt.subplots(figsize=(12, 6))
        for n, family in enumerate(party.families):
            template_name = family.template.name
            highest_index = max(range(len(family.detections)), key=lambda i: family.detections[i].detect_val)
            self_detect = family.detections[highest_index]
            selfdetections_predecluster[template_name]=self_detect
            ax.scatter([d.event.origins[0].time.datetime for i,d in enumerate(family.detections) if i != highest_index], [n] * (len(family.detections)-1), color = 'gray', label='Detections', s=2)
            ax.scatter(self_detect.event.origins[0].time.datetime, n, color = 'blue', label= "Self Detections", s=2)

        ax.set_xlabel('Time')
        ax.set_ylabel('Template Number')
        ax.set_title('Summary of Detections')
        ax.xaxis_date()
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
        fig01.autofmt_xdate()
        handles, labels = ax.get_legend_handles_labels()
        unique_labels = dict(zip(labels, handles))
        ax.legend(unique_labels.values(), unique_labels.keys(), loc='lower right')
        fig01.savefig(os.path.join(self.run_dir, "detections_before_declustering.png"))

        party.decluster(
            trig_int=decluster_trig_int,
            hypocentral_separation=None,
            min_chans=0, # SHOULD BE DIAGNOSED!!
            absolute_values=True
            )
        
        party.sort()
        party = party.filter(dates=[starttime, endtime])
        removed_selfdetections = {}


        fig02, ax2 =plt.subplots(figsize=(12, 6))
        for n, family in enumerate(party.families):
            template_name = family.template.name
            highest_index = max(range(len(family.detections)), key=lambda i: family.detections[i].detect_val)
            self_detect = family.detections[highest_index]
            ax2.scatter([t.event.origins[0].time.datetime for t in family.detections], [n] * len(family.detections), color = 'gray', label='Detections', s=2)
            if self_detect == selfdetections_predecluster[template_name]:
                selfdetections[template_name]=self_detect
                ax2.scatter(self_detect.event.origins[0].time.datetime, n, color = 'blue', label= "Self Detections", s=2)
            else:
                logging.warning(f"Template {template_name} lost its self detection on the declustering process")
                selfdetections[template_name]=selfdetections_predecluster[template_name]
                removed_selfdetections[template_name]= selfdetections_predecluster[template_name]
                ax2.scatter(selfdetections_predecluster[template_name].event.origins[0].time.datetime, n, color = 'red', label= "Removed Self Detections", s=2)
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Template Number')
        ax2.set_title('Summary of Detections')
        # Convert x-axis to datetime format
        ax2.xaxis_date()
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
        handles, labels = ax2.get_legend_handles_labels()
        unique_labels = dict(zip(labels, handles))
        ax2.legend(unique_labels.values(), unique_labels.keys(), loc='lower right')
        fig02.autofmt_xdate()
        fig02.savefig(os.path.join(self.run_dir, "detections_after_declustering.png"))

        self.party = party    
        self.self_detections = selfdetections
        self.export_party(name="Party_declustered.pkl")

        
    def do_lag_calc(self):
        min_cc = float(self.parameters.get('min_cc'))
        shift_len = float(self.parameters.get('shift_len'))
        bank = self.tribe_constructor.bank

        self.party, cat = client_party_lag_calc(self.party, bank, pre_processed=False, shift_len=shift_len, min_cc=min_cc, interpolate=True, parallel= True, use_new_resamp_method=True)
        self.export_party(name="Party_with-picks.pkl")
    
    def catalog_to_tribe(self, catalog, length, prepick):
        # Load Bank
        bank = self.tribe_constructor.bank

        # Parameters Parse
        lowcut=float(self.parameters.get('lowcut'))
        highcut=float(self.parameters.get('highcut'))
        samp_rate=int(self.parameters.get('samp_rate'))
        filt_order=int(self.parameters.get('filt_order'))

        tribe = Tribe().construct(
            method="from_client",
            client_id= bank,
            catalog=catalog,
            lowcut=lowcut,
            highcut=highcut,
            samp_rate=samp_rate,
            filt_order=filt_order,
            length=length,
            prepick=prepick,
            swin="all",
            all_horiz=False,
            min_snr= 0.0,
            parallel=True
        )

        return tribe
    
    def get_relative_magnitudes(self):
        noise_window = float(self.parameters.get('magnitude_noise'))
        prepick = float(self.parameters.get('magnitude_prepick'))
        length = float(self.parameters.get('magnitude_length'))

        # Parent Catalog 
        og_tribe = self.tribe_constructor.tribe
        og_catalog = self.tribe_constructor.catalog
        og_augmented_tribe = self.catalog_to_tribe(og_catalog, (noise_window+ prepick+ length)*2, (noise_window+prepick)*2)

        # Detection Catalog
        detection_catalog = self.party.get_catalog()
        detections_tribe = self.catalog_to_tribe(detection_catalog,(noise_window+ prepick+ length)*2, (noise_window+prepick)*2 )

        # Link detections tribe with augmented tribe through the original tribe.
        template_mapping = {}
        for template in detections_tribe:
            comment = template.event.comments[0].text if template.event.comments else None
            if comment and "Template" in comment:
                template_name1= comment.split()[1]
                og_template = next((t for t in og_tribe if t.name == template_name1), None)
                og_event_id = og_template.event.resource_id
                if og_template:
                    augmented_template = next((t for t in og_augmented_tribe if t.event.resource_id == og_event_id), None)
                    if augmented_template:
                        template_mapping[template.name] = augmented_template
                    else:
                        print(f"No matching augmented template found for original template: {og_template.name}. Check generation of augmented tribe")
                else:
                    print(f"No matching original template found for detected event: {template.name}. Check if the template name exist or if well written")
            else:
                print(f"No comment found for event: {template.name}")
        
        no_mag_calc = 0

        # Calcualtion of Magnitudes
        for template in detections_tribe:
            event = template.event
            stream = template.st
            parent_template = template_mapping[template.name]
            parent_event = parent_template.event
            parent_magnitude = (parent_template.event.preferred_magnitude() or parent_template.event.magnitudes[0])
            parent_stream = parent_template.st
            relative_magnitudes = relative_magnitude(parent_stream, stream, parent_event, event, noise_window=(-noise_window,0), signal_window=(0,length), min_snr=0, min_cc=0, use_s_picks=True)
            values = relative_magnitudes.values()
            if len(values) == 0:  
                print(f"Could not calculate magnitude for detected event {template.event}")
                logging.warning(f"Could not calculate magnitude for detected event {template.event}")
                no_mag_calc +=1
                continue
            relative_mag = sum(values) / len(values)
            new_mag = parent_magnitude.mag + relative_mag
            mag = Magnitude()
            mag.mag = new_mag
            mag.mag_errors.uncertainty = parent_magnitude.mag_errors.uncertainty
            mag.magnitude_type = parent_magnitude.magnitude_type
            mag.evaluation_mode = "automatic"
            event.magnitudes = [mag]
            event.preferred_magnitude_id = mag.resource_id
            self.out_catalog.append(event)

        if not no_mag_calc==0:
            print(f"{no_mag_calc} event detection removed because had no magnitude")
        self.out_catalog = Catalog(sorted(self.out_catalog, key=lambda event: event.origins[0].time))
        path = os.path.join(self.run_dir, "catalog_w_magnitudes.cat")
        self.out_catalog.write(path, "QUAKEML")

        
    def generate_event_textfile(self):
       
        id_mapper = {}
        for i, event in enumerate(self.out_catalog, start=1):
            event_id = event.resource_id.id
            id_mapper[event_id] = i

        output_file = os.path.join(self.run_dir, "event_file.txt")
        create_catalog_file(self.out_catalog, id_mapper, filename=output_file)

    def correlator_run(self, file_name= "correlate_script.sh"):
        write_slurm_script(self.swarm_name, self.run_dir, partition_string="gpu-1xA100,gpu-2xA100", time="3-00:00:00", type="correlate", file_name=file_name)
        script_path = os.path.join(self.run_dir, file_name)
        result = subprocess.run(['sbatch', script_path], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Job submitted successfully: {result.stdout}")
        else:
            print(f"Error in submitting job: {result.stderr}")

    def check_run_status(self):
        run_file = os.path.join(self.run_dir, "run_file.json")
        step_order = ['Tribe_construction', 'Detection', 'Declustering', 'Lag_calc', 'Magnitudes', "Correlations", "Relocations"]
        state = {step: False for step in step_order}

        if not os.path.exists(run_file):
            raise FileNotFoundError(f"Run file {run_file} not found.")
        
        with open(run_file, 'r') as f:
            run_data = json.load(f)

        completed_steps = [step["step"] for step in run_data.get("completed_steps", [])]

        for step in step_order:
            if step in completed_steps:
                state[step] = True
        
        last_completed_step = None
        for step in step_order:
            if state[step]:
                last_completed_step = step 

        return last_completed_step



    def check_parameters_changes(self):
        
        param_dict = {
            "starttime": "Tribe_construction",
            "endtime": "Tribe_construction",
            "min_stations": "Tribe_construction",
            "length": "Tribe_construction",
            "prepick": "Tribe_construction",
            "min_snr": "Tribe_construction",
            "lowcut": "Tribe_construction",
            "highcut": "Tribe_construction",
            "samp_rate": "Tribe_construction",
            "filt_order": "Tribe_construction",
            "enforce_pl": "Tribe_construction",
            "pl": "Tribe_construction",
            
            "threshold": "Detection",
            "threshold_type": "Detection",
            "arch": "Detection",
            "detect_trig_int": "Detection",
            
            "decluster_trig_int": "Declustering",
            "min_chans": "Declustering",
            
            "min_cc": "Lag_calc",
            "shift_len": "Lag_calc",
            
            "magnitude_noise": "Magnitudes",
            "magnitude_prepick": "Magnitudes",
            "magnitude_length": "Magnitudes",
            
            "dt_prepick": "Correlations",
            "dt_length": "Correlations",
            "max_sep": "Correlations",
            "min_link": "Correlations",
            "dt_min_cc": "Correlations"
        }
        
        step_order = ['Tribe_construction', 'Detection', 'Declustering', 'Lag_calc', 'Magnitudes', "Correlations", "Relocations"]
        changed_steps = set()

        run_parameters = self.parameters
        swarm_dir= os.path.join(swarms_directrory, self.swarm_name)
        swarm_parameter_file = os.path.join(swarm_dir, f"parameters{swarm_name}.txt")
        updated_parameters = self._load_parameters(swarm_parameter_file)

        
        for parameter, associated_step in param_dict.items():
            if run_parameters.get(parameter, None) != updated_parameters.get(parameter, None):
                print(f"parameter {parameter} has been changed since this run was created. Updating!")
                changed_steps.add(associated_step)

        if not changed_steps:
            print("No parameters has been changed")
            return

        self.parameters = updated_parameters
        for step in step_order:
            if step in changed_steps:
                return step
        

    def rerun(self):

        step_order = ['Tribe_construction', 'Detection', 'Declustering', 'Lag_calc', 'Magnitudes', "Correlations", "Depurate Correlations", "Relocations"]

        last_ran_step = self.check_run_status()
        last_ran_step_index = step_order.index(last_ran_step)
        print(f"The Last step ran was: {last_ran_step}")
        
        earliest_changed_step = self.check_parameters_changes()
        if earliest_changed_step:
            earliest_changed_step_index = step_order.index(earliest_changed_step)
            start_index = min(last_ran_step_index+1, earliest_changed_step_index)
        else:
            start_index = last_ran_step_index+1
        
        if start_index > step_order.index("Relocations"):
            print("This run is already completed with current parameters. No changes will be done.")
            return
        
        print(f"Starting rerun from {step_order[start_index]}")

        if start_index == step_order.index("Relocations"):
            run_relocations(self.swarm_name, self.swarm_dir)
            return
        if start_index == step_order.index("Correlations"):
            self.correlator_run()
            return
        if start_index == step_order.index("Tribe Construction"):
            print("Rerun overhaul the whole run from start. The run file will be overwrtien")
            self.new_run()
            return
        
        self.tribe_constructor = TribeConstructor(self.parameters, self.run_dir)
        self.tribe_constructor.load_catalog()
        self.tribe_constructor.tribe = Tribe().read(os.path.join(self.run_dir, f"{self.swarm_name}_rawtribe.tgz"))
        

        if start_index > step_order.index("Lag_calc"):
            self.party = self.load_party("Party_with-picks.pkl")

        elif start_index > step_order.index("Declustering"):
            self.party = self.load_party("Party_pre-decluster.pkl")

        elif start_index > step_order.index("Detection"):
            self.party = self.load_party("Party_declustered.pkl")


        # Loop through remaining steps dynamically
        for step in step_order[start_index:]:
            if step == "Detection":
                starttime = datetime.now()
                self.detect()
                detection_count = sum(len(family.detections) for family in self.party)
                non_zero_families = sum(1 for family in self.party if len(family.detections) > 0)
                run_log.update_completed_step(self.run_dir, "Detection", starttime, {"families": non_zero_families, "detections": detection_count})

            elif step == "Declustering":
                starttime = datetime.now()
                self.decluster_party()
                detection_count = sum(len(family.detections) for family in self.party)
                non_zero_families = sum(1 for family in self.party if len(family.detections) > 0)
                run_log.update_completed_step(self.run_dir, "Declustering", starttime, {"families": non_zero_families, "detections": detection_count})

            elif step == "Lag_calc":
                starttime = datetime.now()
                self.do_lag_calc()
                detection_w_picks = sum(len(event.picks) for family in self.party for event in family.catalog)
                non_zero_families = sum(1 for family in self.party if any(len(event.picks) > 0 for event in family.catalog))
                run_log.update_completed_step(self.run_dir, "Lag_calc", starttime, {"families": non_zero_families, "events_w_picks": detection_w_picks})

            elif step == "Magnitudes":
                starttime = datetime.now()
                self.get_relative_magnitudes()
                run_log.update_completed_step(self.run_dir, "Magnitudes", starttime, {"events_w_magnitudes": len(self.out_catalog)})
                self.generate_event_textfile()
                time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"Basic pipeline rerun of {self.swarm_name} completed, {time}")


    def load_party(self, filename):
        path = os.path.join(self.run_dir, filename)
        if os.path.exists(path):
            with open(path, 'rb') as f:
                return pickle.load(f)
        else:
            raise FileNotFoundError(f"Party file {filename} not found.")

    def load_catalog(self, filename):
        path = os.path.join(self.run_dir, filename)
        if os.path.exists(path):
            return Catalog().read(path, format="QUAKEML")
        else:
            raise FileNotFoundError(f"Catalog file {filename} not found.")
    
    def correlator_run(self):
        """
        Prepares and submits the correlation job using SLURM.
        """
        print(f"Submitting correlation job for {self.swarm_name}...")

        # Define the name of the SLURM script
        script_name = "correlate_script.sh"

        write_slurm_script(
        swarm_name=self.swarm_name,
        run_dir=self.run_dir,
        partition_string="gpu-long",  # Adjusted for longer tasks
        time="7-00:00:00",  # 7 days max time
        type="correlate",
        file_name=script_name
        )

        # Submit the SLURM job
        script_path = os.path.join(self.run_dir, script_name)
        result = subprocess.run(['sbatch', script_path], capture_output=True, text=True)

        if result.returncode == 0:
            print(f"Correlation job submitted successfully: {result.stdout.strip()}")
        else:
            print(f"Error submitting correlation job: {result.stderr.strip()}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python Pipeline.py <swarm_name> <run_directory> [rerun]")
        sys.exit(1)

    swarm_name = sys.argv[1]
    run_dir = sys.argv[2]
    
    # Check if "rerun" is passed as an optional third argument
    run_mode = "rerun" if len(sys.argv) == 4 and sys.argv[3] == "rerun" else "new_run"

    pipe = EQ_Pipeline(swarm_name, run_dir, run_mode)

    if run_mode == "new_run":
        pipe.new_run()
    else:
        # Implement rerun logic later
        pipe.rerun()


        





        
        


        

        

