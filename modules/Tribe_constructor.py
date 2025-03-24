import os
import sys
import logging
import pickle
from obspy import UTCDateTime
from eqcorrscan.core.match_filter import Tribe
from obsplus import WaveBank

current_dir = os.path.dirname(os.path.abspath(__file__))
pipeline_root = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(pipeline_root)
from utils.loader import read_catalog_from_csv, check_picks

archive_path="/hpceliasrafn/haa53/EQcorrscan_pipeline/Swarm_data/ARCHIVE"

class TribeConstructor:
    def __init__(self, params, run_dir, bad_station_list=None):
        self.run_dir = run_dir
        self.params = params
        self.swarm_name = self.params.get("swarm_name")
        self.bad_station_list = bad_station_list if bad_station_list else [] ## THIS SHOULD BE EVALUATED IN THE RUN
        self.bank = WaveBank(archive_path)
        
        self.starttime = UTCDateTime(self.params.get('starttime'))
        self.endtime = UTCDateTime(self.params.get('endtime'))
        self.catalog = None
        self.tribe = None
        self.stations = set()

    def load_catalog(self):
        logging.info("Loading catalog...")
        self.catalog = read_catalog_from_csv(self.params.get('catalog_csv'))
        self.catalog = self.catalog.filter(f"time >= {self.starttime}", f"time <= {self.endtime}")
        
        for event in self.catalog:
            for pick in event.picks:
                self.stations.add(pick.waveform_id.station_code)
        
        self.stations -= set(self.bad_station_list)
        logging.info(f"Catalog loaded with {len(self.catalog)} events and {len(self.stations)} stations.")

    def update_picks(self):
        logging.info("Updating pick codes...")
        av = self.bank.get_availability_df()
        self.catalog = check_picks(self.catalog, av, send_warning=False)

    def construct_tribe(self):
        logging.info("Constructing tribe templates...")
        self.tribe = Tribe().construct(
            method="from_client",
            client_id=self.bank,
            catalog=self.catalog,
            lowcut=float(self.params.get('lowcut')),
            highcut=float(self.params.get('highcut')),
            samp_rate=int(self.params.get('samp_rate')),
            filt_order=int(self.params.get('filt_order')),
            length=float(self.params.get('length')),
            prepick=float(self.params.get('prepick')),
            swin="all",
            all_horiz=False,
            min_snr=float(self.params.get('min_snr')),
            parallel=True
        )
        logging.info(f"Tribe created with {len(self.tribe)} templates.")

    def filter_templates(self):
        min_stations = int(self.params.get('min_stations'))
        self.tribe.templates = [t for t in self.tribe if len({tr.stats.station for tr in t.st}) >= min_stations]
        logging.info(f"Filtered tribe now contains {len(self.tribe)} templates.")

    def sanitize_process_length(self, process_length = 86400):
        for template in self.tribe:
            if template.process_length != process_length:
                logging.warning(f"Template {template.name} has a uncoherent process length of {template.process_length}. May be caused by gaps in data. It will be forced to {process_length}")
                template.process_length = process_length

    def save_tribe(self):
        name = f"{self.swarm_name}_rawtribe"
        path = os.path.join(self.run_dir, name)
        self.tribe.write(path, compress=True, catalog_format='QUAKEML')
        # with open('stations.pkl', 'wb') as f:
        #     pickle.dump(self.stations, f)
        logging.info("Tribe saved.")

    def run(self):
        logging.info("Starting Tribe Construction Process...")
        self.load_catalog()
        self.update_picks()
        self.construct_tribe()
        self.filter_templates()
        self.sanitize_process_length()
        self.save_tribe()
        logging.info("Tribe construction process completed.")
        return self.tribe