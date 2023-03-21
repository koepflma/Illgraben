# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# Filename: compute_all_attributes.py
#  Purpose: Compute features for signal classification
#   Author: Michaela Wenner
#    Email: wenner@vaw.baug.ethz.ch
# ------------------------------------------------------------
"""
Create daily file containing attributes of 40s windows for all 
ILL Stations 
"""
import obspy
import numpy as np
import pandas as pd
import datetime
from ComputeAttributes_CH_V1 import *
from obspy import signal
from obspy.signal import cross_correlation
from obspy.clients.filesystem.sds import Client
import time
import os
import glob
import multiprocessing
from functools import partial

# =================== set parameters ==================
year = 2017
jdays = np.arange(1,366+1,1)
network = 'XP'
stations = ['ILL06','ILL07','ILL08']
channels = ['EHN', 'EHE', 'EHZ']

# =================== define functions ==================
def get_mseed(time, client=Client('/data/wsd03/data_manuela/Illgraben/miniseed/'), 
              network=network,stations=stations, channels=channels, locations=[''], 
              prepick=0, window_length=24*60*60.):

    st = obspy.Stream()
    for station in stations:
        for location in locations:
            for channel in channels:
                try:           
                    new_st = client.get_waveforms(network, station, location, channel,
                                                obspy.UTCDateTime(time)-prepick, 
                                                obspy.UTCDateTime(time)-prepick+window_length)
                    st += new_st
                except:
                    continue
    return(st)


def comp_features(i, year=year, network=network, stations=stations, channels=channels):
    t0 = time.time()
#    st = obspy.read(f'/media/manuela/Elements/illgraben/miniseed/{year}/XP/ILL??/EHZ.D/*{i}')
    st = get_mseed(obspy.UTCDateTime(f'{year}-{i}'), network=network, stations=stations, channels=channels)
    print(st[0].stats.starttime)
    st.merge(fill_value = 'interpolate')
    stt = st[0].stats.sampling_rate
    st.detrend('demean')
    st1 = st.copy()
#     st1.filter('bandpass', freqmin=1, freqmax=10)

    tt = st[0].stats.starttime
    #dat = np.zeros(3, len(st[0].data))
    #for idx,tr in enumerate(st):
    #    dat[idx] = tr.data

    j = 0
    while j < len(st1[0].data) - stt*40:
        feats = np.array([])
        try:
            for tr in st1:
                feats = np.append(feats, calculate_all_attributes(tr.data[int(j):int(j+stt*40)],stt,0))
        except:
            print(f"Error at {tt.julday}")
        #data = dat[:,j:j+stt*30]
        #for statdat in data:
        #    feats = np.append(feats, calculate_all_attributes(statdat, stt,0))
        '''
        cc1 = obspy.signal.cross_correlation.correlate(st1[0].data[int(j):int(j+stt*30)], st1[1].data[int(j):int(j+stt*30)], shift=5*200)
        s1, v1 = obspy.signal.cross_correlation.xcorr_max(cc1, abs_max=True)
        cc2 = obspy.signal.cross_correlation.correlate(st1[0].data[int(j):int(j+stt*30)], st1[2].data[int(j):int(j+stt*30)],shift=5*200)
        s2, v2 = obspy.signal.cross_correlation.xcorr_max(cc2, abs_max=True)
        cc3 = obspy.signal.cross_correlation.correlate(st1[1].data[int(j):int(j+stt*30)], st1[2].data[int(j):int(j+stt*30)], shift=5*200)
        s3, v3 = obspy.signal.cross_correlation.xcorr_max(cc3, abs_max=True)
        cross_cors = np.array([s1,v1,s2,v2,s3,v3])
        feats = np.append(feats,cross_cors)
        '''
        feats = np.reshape(feats, (1, len(feats)))
        d = pd.DataFrame(feats)
        
        file_path = '/data/wsd03/data_manuela/Illgraben/feature_files/{}/40_seconds_unfilt/'.format(year)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        
        with open(file_path + f'{tt.julday}_{tt.hour}-{tt.minute}-{tt.second}.{tt.microsecond}.csv', "a+") as f:
            d.to_csv(f, header=False, index=False)
        j += stt*10
    t1 = time.time()
    #print(t1-t0)
    return t1-t0
        #print((len(st[0].data) - j)/stt)

# =================== test data avaivability ==================
jday_list = []

for jday in jdays:
    count = 0
    for sta in stations:
        filepath = '/data/wsd03/data_manuela/Illgraben/miniseed/{}/{}/{}/{}.D/*.{:03d}'.format(int(year),network, sta, 'EHZ', int(jday))
#         filepath = '/data/wsd03/data_manuela/Illgraben/miniseed/{}/*/{}/*.D/*.{:03d}'.format(int(year), sta, int(jday))
        files = glob.glob(filepath)
#         if len(files) >= 1:
        if len(files) == 1:
            count+=1
    if count == len(stations):
        jday_list.append(jday)
    
# =================== multi-processing ==================    
pool = multiprocessing.Pool(24)
#[print(f'{i}') for i in pool.map(comp_features, range(173, 204))]
# [print(f'{i}') for i in pool.map(partial(comp_features, year=year, stations=stations, channels=channels), jday_list)]

for i in pool.map(partial(comp_features, year=year, network=network, stations=stations, channels=channels), jday_list):
    print(f'{i}')
pool.close()
pool.join()
