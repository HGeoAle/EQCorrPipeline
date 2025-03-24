import logging
import pickle
import numpy as np
from obsplus import WaveBank
from obspy import Stream, UTCDateTime, Catalog
from eqcorrscan.utils import pre_processing
from eqcorrscan.core.match_filter.family import Family
from eqcorrscan.core.match_filter.party import Party

Logger = logging.getLogger(__name__)

class LagcalcLoad(Exception):
    """
    Default error for template generation errors.
    """
    def __init__(self, value):
        """
        Raise error.
        """
        self.value = value

    def __repr__(self):
        return self.value

    def __str__(self):
        return 'Lagcalc_LoadError: ' + self.value
    
def _group_detections(family, data_pad):

    # If not detections in the family, stops
    assert len(family), "No events to group"
    # if there is only one detection there is no groups to make
    if len(family) == 1:
        return [family]
    # In other case we create a list of sub families to group
    sub_families = []
    
    process_len = family.template.process_length
    template_length = len(family.template.st[0]) / family.template.samp_rate
    

    family.detections = sorted(family.detections, key=lambda d: d.detect_time)
    sub_family = Family(template=family.template, detections = family.detections[0])

    for detection in family.detections[1:]:
        event = detection.event
        origin_time = (event.preferred_origin() or event.origins[0]).time
        last_pick = sorted(event.picks, key=lambda p: p.time)[-1]
        max_diff = (
            process_len - (last_pick.time - origin_time) - template_length)
        max_diff -= 2 * data_pad
        if origin_time - sub_family[0].event.origins[0].time < max_diff:
            sub_family += detection
        else:
            sub_families.append(sub_family)
            sub_family = Family(template=family.template, detections = detection)
    sub_families.append(sub_family)
    return sub_families

def load_from_client(client, family, data_pad, available_stations=[]):
    process_len = family.template.process_length
    st = Stream()
    family = family.sort()

    # Collect information of traces to load
    all_waveform_info = []
    for detection in family.detections:
        event = detection.event
        for pick in event.picks:
            if not pick.waveform_id:
                Logger.warning(
                    "Pick not associated with waveforms, will not use:"
                    " {0}".format(pick))
                continue
            channel_code = pick.waveform_id.channel_code[0:2] + "?"
            if pick.waveform_id.station_code is None:
                Logger.error("No station code for pick, skipping")
                continue
            all_waveform_info.append((
                pick.waveform_id.network_code or "*",
                pick.waveform_id.station_code,
                channel_code, pick.waveform_id.location_code or "*"))
    starttime = UTCDateTime(family.detections[0].event.origins[0].time - data_pad)
    endtime = starttime + process_len

    # Error if the last detection is outside the window
    if not endtime > family.detections[-1].event.origins[0].time + data_pad:
        raise LagcalcLoad(
            'Events do not fit in processing window')

    all_waveform_info = sorted(list(set(all_waveform_info)))
    dropped_pick_stations = 0
    for waveform_info in all_waveform_info:
        net, sta, chan, loc = waveform_info
        Logger.info('Downloading for start-time: {0} end-time: {1}'.format(
            starttime, endtime))
        Logger.debug('.'.join([net, sta, loc, chan]))
        query_params = dict(
            network=net, station=sta, location=loc, channel=chan,
            starttime=starttime, endtime=endtime)
        try:
            st += client.get_waveforms(**query_params)
        except Exception as e:
            Logger.error(e)
            Logger.error('Found no data for this station: {0}'.format(
                query_params))
            dropped_pick_stations += 1
    if not st and dropped_pick_stations == len(event.picks):
        raise Exception('No data available, is the server down?')
    st.merge()

    # clients download chunks, we need to check that the data are
    # the desired length
    final_channels = []
    for tr in st:
        tr.trim(starttime, endtime)
        if len(tr.data) == (process_len * tr.stats.sampling_rate) + 1:
            Logger.info(f"{tr.id} is overlength dropping first sample")
            tr.data = tr.data[1:len(tr.data)]
            tr.stats.starttime += tr.stats.delta
        if tr.stats.endtime - tr.stats.starttime < 0.8 * process_len:
            Logger.warning(
                "Data for {0}.{1} on the segment {3}-{4} is {2} hours long, which is less than 80 "
                "percent of the desired length, will not use".format(
                    tr.stats.station, tr.stats.channel,
                    (tr.stats.endtime - tr.stats.starttime) / 3600,
                    starttime, endtime))
        elif not pre_processing._check_daylong(tr.data):
            Logger.warning(
                "Data are mostly zeros, removing trace: {0}".format(tr.id))
        else:
            final_channels.append(tr)
    st.traces = final_channels
    return st

def client_family_lag_calc(family, client, pre_processed, shift_len =0.2, min_cc=0.4,
                    min_cc_from_mean_cc_factor= None, vertical_chans=['Z'],
                    horizontal_chans=['E', 'N', '1', '2'], cores=1, interpolate=False,
                    plot= False, plotdir=None, parallel=True, process_cores=None, ignore_length=False,
                    skip_short_chans=False, ignore_bad_data= False, export_cc = False, cc_dir=None,
                    **kwargs):
    
    data_pad = kwargs.get('data_pad', 90)
    template = family.template
    process_len = template.process_length
    sub_families = _group_detections(family, data_pad)

    family_out = Family(template=family.template, detections = [])
    catalog = Catalog()

    counter = 1
    for sub_family in sub_families:
        print(sub_family)
        Logger.info(f"Loading waveform data for detection of template {sub_family} {counter}")
        st = load_from_client(client, sub_family, data_pad, available_stations=[])
        Logger.info('Pre-processing data')
        st.merge()
        if len(st) == 0:
            Logger.info("No data")
            continue


        for tr in st:
            if np.ma.is_masked(tr.data):
                _len = np.ma.count(tr.data) * tr.stats.delta
            else:
                _len = tr.stats.npts * tr.stats.delta
            if _len < process_len * .8:
                Logger.info(
                    "Data for {0} are too short, skipping".format(
                        tr.id))
                if skip_short_chans:
                    continue
            # Trim to enforce process-len
            tr.data = tr.data[0:int(process_len * tr.stats.sampling_rate)]

        if len(st) == 0:
            Logger.warning("No data in stream of sub_family {0}".format(sub_family))

        Logger.info('Pre-processing data')
        processed_stream = sub_family._process_streams(stream=st, pre_processed=pre_processed,
            process_cores=process_cores, parallel=parallel, 
            ignore_bad_data=ignore_bad_data, ignore_length=ignore_length,
            select_used_chans = False)
        
        catalog += sub_family.lag_calc(
                    stream=processed_stream, pre_processed=True,
                    shift_len=shift_len, min_cc=min_cc,
                    min_cc_from_mean_cc_factor=min_cc_from_mean_cc_factor,
                    horizontal_chans=horizontal_chans,
                    vertical_chans=vertical_chans, cores=cores,
                    interpolate=interpolate, plot=plot, plotdir=plotdir,
                    export_cc=export_cc, cc_dir=cc_dir,
                    parallel=parallel, process_cores=process_cores,
                    ignore_bad_data=ignore_bad_data,
                    ignore_length=ignore_length, **kwargs)
        
        family_out += sub_family
    
    return family_out, catalog

def client_party_lag_calc(party, client, pre_processed, shift_len =0.2, min_cc=0.4,
                    min_cc_from_mean_cc_factor= None, vertical_chans=['Z'],
                    horizontal_chans=['E', 'N', '1', '2'], cores=1, interpolate=False,
                    plot= False, plotdir=None, parallel=True, process_cores=None, ignore_length=False,
                    skip_short_chans=False, ignore_bad_data= False, export_cc = False, cc_dir=None,
                    **kwargs):
    process_cores = process_cores or cores
    catalog = Catalog()
    out_party = Party()
    for family in party:
        new_family, family_catalog = client_family_lag_calc(family, client, pre_processed= pre_processed, shift_len = shift_len, min_cc=min_cc,
                                          min_cc_from_mean_cc_factor= min_cc_from_mean_cc_factor, vertical_chans=vertical_chans,
                                          horizontal_chans=horizontal_chans, cores= cores, interpolate = interpolate,
                                          plot=plot, plotdir=plotdir, parallel=parallel, process_cores=process_cores, ignore_length=ignore_length,
                                          skip_short_chans=skip_short_chans, ignore_bad_data=ignore_bad_data, export_cc = export_cc, cc_dir = cc_dir,
                                          **kwargs)
        out_party += new_family
        catalog += family_catalog
    
    return out_party, catalog