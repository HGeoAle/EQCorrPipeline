import os
import pathlib
import logging
import pandas as pd
from obspy import Catalog, UTCDateTime, read, __version__
from obspy.geodetics import kilometer2degrees
from obspy.core import AttribDict
from obspy.core.stream import Stream
from obspy.core.event import (
    Event,
    Origin,
    OriginUncertainty,
    ConfidenceEllipsoid,
    Pick,
    WaveformStreamID,
    CreationInfo,
    Magnitude,
)


def read_catalog_from_csv(csv_filename, units='m', GAU=True):
    """
    Reads a CSV file and gathers the picks from a 'picks' directory in the same location.
    Automatically detects if the CSV file has a header.
    It requires a directory called 'picks' in the same location as csv to read the picks files
    """

    column_names = [
        "EventID", "DT", "X", "Y", "Z", "COA", "COA_NORM",
        "GAU_X", "GAU_Y", "GAU_Z", "GAU_ErrX", "GAU_ErrY", "GAU_ErrZ",
        "COV_ErrX", "COV_ErrY", "COV_ErrZ", "TRIG_COA", "DEC_COA",
        "DEC_COA_NORM", "ML", "ML_Err", "ML_r2", "COV_Err_XYZ", "seq"
    ]
    
    csv_path = pathlib.Path(csv_filename).resolve()
    picks_dir = csv_path.parent / "picks"

    # Check for Header
    with open(csv_path, 'r') as f:
        first_row = f.readline().strip().split(',')
    has_header = all(col in column_names for col in first_row)

    if has_header:
        print("The CSV file has a header")
        swarm_table = pd.read_csv(csv_path)
    else:
        print("The CSV file doesn't have a header. Impossing header")
        swarm_table = pd.read_csv(csv_path, header=None, names=column_names)

    cat = Catalog()
    
    # Check if it can find picks directory

    if not picks_dir.is_dir():
        print("Could not find picks diretory")
        print("No picks will be loaded")
        return None

    # Checks and assings unit factor
    if units == "km":
        factor = 1e3
    elif units == "m":
        factor = 1
    else:
        raise AttributeError(f"units must be 'km' or 'm'; not {units}")
    
    # Iterate on each line of the CSV and creates an event for each row
    n = 0 
    for _, event_info in swarm_table.iterrows():
        # Create event origin
        event = Event()
        event_uid = str(event_info["EventID"])
        ns = event_uid

        # Add Basic info
        event.resource_id = str(event_uid)
        event.creation_info = CreationInfo(
        author = "Hugo",
        date =  str(UTCDateTime()))
        
        # Add COA info to extra
        event.extra = AttribDict()
        event.extra.coa = {"value": event_info["COA"], "namespace": ns}
        event.extra.coa_norm = {"value": event_info["COA_NORM"], "namespace": ns}
        event.extra.trig_coa = {"value": event_info["TRIG_COA"], "namespace": ns}
        event.extra.dec_coa = {"value": event_info["DEC_COA"], "namespace": ns}
        event.extra.dec_coa_norm = {"value": event_info["DEC_COA_NORM"], "namespace": ns}

        # Create origin with spline location and set to preferred event origin.
        origin = Origin()
        origin.method_id = "spline"
        origin.longitude = event_info["X"]
        origin.latitude = event_info["Y"]
        origin.depth = event_info["Z"] * factor
        origin.time = UTCDateTime(event_info["DT"])
        event.origins = [origin]
        event.preferred_origin_id = origin.resource_id

        # Create origin with gaussian location and associate with event
        if GAU:
            origin = Origin()
            origin.method_id = "gaussian"
            origin.longitude = event_info["GAU_X"]
            origin.latitude = event_info["GAU_Y"]
            origin.depth = event_info["GAU_Z"] * factor
            origin.time = UTCDateTime(event_info["DT"])
            event.origins.append(origin)

        # Set confidence ellipsoid and uncertainties for both as the gaussian uncertainties 
        ouc = OriginUncertainty()
        ce = ConfidenceEllipsoid()
        ce.semi_major_axis_length = event_info["COV_ErrY"] * factor
        ce.semi_intermediate_axis_length = event_info["COV_ErrX"] * factor
        ce.semi_minor_axis_length = event_info["COV_ErrZ"] * factor
        ce.major_axis_plunge = 0
        ce.major_axis_azimuth = 0
        ce.major_axis_rotation = 0
        ouc.confidence_ellipsoid = ce
        ouc.preferred_description = "confidence ellipsoid"

        # Set uncertainties for both as the gaussian uncertainties
        for origin in event.origins:
            origin.longitude_errors.uncertainty = kilometer2degrees( event_info["GAU_ErrX"] * factor / 1e3 )
            origin.latitude_errors.uncertainty = kilometer2degrees( event_info["GAU_ErrY"] * factor / 1e3  )
            origin.depth_errors.uncertainty = event_info["GAU_ErrZ"] * factor
            origin.origin_uncertainty = ouc
        
        # Add OriginQuality info to each origin
        for origin in event.origins:
            origin.origin_type = "hypocenter"
            origin.evaluation_mode = "automatic"
        
        # Add Magnitude to event
        mag = Magnitude()
        mag.extra = AttribDict()
        mag.mag = event_info["ML"]
        mag.mag_errors.uncertainty = event_info["ML_Err"]
        mag.magnitude_type = "ML"
        mag.evaluation_mode = "automatic"
        mag.extra.r2 = {"value": event_info["ML_r2"], "namespace": ns}

        event.magnitudes = [mag]
        event.preferred_magnitude_id = mag.resource_id

        # Handle Picks
        pick_file = picks_dir / event_uid
        if pick_file.with_suffix(".picks").is_file():
            picks = pd.read_csv(pick_file.with_suffix(".picks"))
            n += 1
        else:
            print("No pick file found for the event: " + ns)
            continue
        
        for _, pickline in picks.iterrows():
            station = str(pickline["Station"])
            phase = str(pickline["Phase"])
            channels = str(pickline["SEED_ids"]).strip("[]").replace("'", "").split(",")

            if (len(channels) > 1 and phase =="S"):
                for chanal in channels:
                    network, _station, _loc, ch = chanal.split(".")
                    wid = WaveformStreamID(network_code=network, station_code=station, location_code=_loc, channel_code=ch)
                    pick = Pick()
                    pick.extra = AttribDict()
                    pick.waveform_id = wid
                    pick.method_id = "automatic"
                    pick.phase_hint = phase
                    if str(pickline["PickTime"]) != "-1":
                        pick.time = UTCDateTime(pickline["PickTime"])
                        pick.time_errors.uncertainty = float(pickline["PickError"])
                        pick.extra.snr = {"value": float(pickline["SNR"]), "namespace": ns}
                    else:
                        continue
                    event.picks.append(pick)
            else:
                chanel = channels[0]
                network, _station, _loc, ch = chanel.split(".")
                wid = WaveformStreamID(network_code=network, station_code=station, channel_code=ch)
                pick = Pick()
                pick.extra = AttribDict()
                pick.waveform_id = wid
                pick.method_id = "automatic"
                pick.phase_hint = phase
                if str(pickline["PickTime"]) != "-1":
                    pick.time = UTCDateTime(pickline["PickTime"])
                    pick.time_errors.uncertainty = float(pickline["PickError"])
                    pick.extra.snr = {"value": float(pickline["SNR"]), "namespace": ns}
                else:
                    continue
                event.picks.append(pick)
        cat.append(event)
    
    print("Succesfully loaded " + str(n) + " events to catalog")
    return cat    

def check_picks(catalog, av, send_warning=False):
    """
    Checks and fixes that picks and traces have the same network code and channel code based on the time of the pick.
    
    Parameters:
    - catalog: Obspy Catalog object containing events and picks.
    - av: Pandas DataFrame containing availability information with columns 'network', 'station', 'channel', 'starttime', and 'endtime'.
    - send_warning: If True, prints warnings for discrepancies found.
    
    Returns:
    - A new Obspy Catalog object with updated picks and filtered events.
    """
    # Define a mapping for channel homologs (e.g., BHE -> HHE)
    homolog_channels = {'BHE': 'HHE', 'BHN': 'HHN', 'BHZ': 'HHZ',
                        'HHE': 'BHE', 'HHN': 'BHN', 'HHZ': 'BHZ'}

    j_sum = 0 # Total number of network code discrepancies
    k_sum = 0 # Total number of channel code discrepancies

    for event in catalog:
        j = 0 # To count the number of discrepancies in the network code of the event
        k = 0 # To count the number of discrepancies in the channel code of the event

        for pick in event.picks:
            # Extract station, channel, network, and time from the pick
            station = pick.waveform_id.station_code
            s_network = pick.waveform_id.network_code
            s_channel = pick.waveform_id.channel_code
            pick_time = pick.time.datetime

            # Find the corresponding row in the availability DataFrame based on the station, time, and channel
            station_info = av[
                (av['station'] == station) & 
                (av['channel'] == s_channel) &
                (av['starttime'] <= pick_time) & 
                (av['endtime'] >= pick_time)
            ]

            # If station_info is empty, try with homologous channel code
            if station_info.empty and s_channel in homolog_channels:
                homolog_channel = homolog_channels[s_channel]
                station_info = av[
                    (av['station'] == station) & 
                    (av['channel'] == homolog_channel) &
                    (av['starttime'] <= pick_time) & 
                    (av['endtime'] >= pick_time)
                ]
                k += 1
                pick.waveform_id.channel_code = homolog_channel

            # If station_info is still empty, raise a warning (if necessary) and skip
            if station_info.empty:
                logging.warning(f"Warning: No valid network/channel found for pick at station {station}, time {pick_time}.")
                continue

            # If station_info is not empty, check network and channel codes
            correct_network = station_info.iloc[0]['network']

            # Check if the network code matches and update if necessary
            if s_network != correct_network:
                pick.waveform_id.network_code = correct_network
                j += 1  # Increment for network discrepancy

        if j>0:
            j_sum += j
            if send_warning:
                logging.warning(f"Event {event.resource_id} had {j} discrepancies on the network code")
        if k>0:
            k_sum += k
            if send_warning:
                logging.warning(f"Event {event.resource_id} had {k} discrepancies on the channel code")

    logging.warning(f"A total of {j_sum} network codes and {k_sum} channel codes have been updated")

    # Filter out events with no picks
    filtered_events = [event for event in catalog if event.picks]
    
    if not filtered_events:
        logging.error("No events with picks found in the catalog.")
        raise ValueError("No events with picks found in the catalog.")
    
    # Create a new Catalog object with filtered events
    return Catalog(events=filtered_events)




