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
client = Client('/data/wsd03/data_manuela/Illgraben/miniseed/')
network = 'XP'
stations = ['ILL06','ILL07','ILL08']
channels = ['EHN', 'EHE', 'EHZ'] # check code before changing -> not tested

# =================== define functions ==================
def get_mseed(time, client, network, stations, channels, locations=[''], 
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


def comp_features(i, year=year, client=client, network=network, stations=stations, channels=channels):
    
    file_path = '/data/wsd03/data_manuela/Illgraben/feature_files/{}/40_seconds_2freq/'.format(year)
    if not os.path.exists(file_path):
        os.makedirs(file_path)    
    
    
    t0 = time.time()
    st = get_mseed(obspy.UTCDateTime(f'{year}-{i}'), client=client, network=network, stations=stations, channels=channels)
    print(st[0].stats.starttime)
    st.merge(fill_value = 'interpolate')
    stt = st[0].stats.sampling_rate
    st.detrend('demean')
    # ------------------------ first frequency band ------------------------
    st1 = st.copy()
    st1.filter('bandpass', freqmin=1, freqmax=10)

    tt = st[0].stats.starttime

    j = 0
    while j < len(st1[0].data) - stt*40:
        feats = np.array([])
        try:
            for tr in st1:
                feats = np.append(feats, calculate_all_attributes(tr.data[int(j):int(j+stt*40)],stt,0))
        except:
            print(f"Error at {tt.julday}")
            
        feats = np.reshape(feats, (1, len(feats)))
        d = pd.DataFrame(feats)
 
        
        with open(file_path + f'{tt.julday}_{tt.hour}-{tt.minute}-{tt.second}.{tt.microsecond}.csv', "a+") as f:
            d.to_csv(f, header=False, index=False)
        j += stt*10
        
    t1 = time.time()
    print('First part tooks {}'.format(t1-t0))
        
    # ------------------------ second frequency band ------------------------    
    st1 = st.copy()
    st1.filter('bandpass', freqmin=35, freqmax=45)

    tt = st[0].stats.starttime

    j = 0
    while j < len(st1[0].data) - stt*40:
        feats = np.array([])
        try:
            for tr in st1:
                feats = np.append(feats, calculate_all_attributes(tr.data[int(j):int(j+stt*40)],stt,0))
        except:
            print(f"Error at {tt.julday}")
            
        feats = np.reshape(feats, (1, len(feats)))
        d = pd.DataFrame(feats)
        
        with open(file_path + f'{tt.julday}_{tt.hour}-{tt.minute}-{tt.second}.{tt.microsecond}.csv', "a+") as f:
            d.to_csv(f, header=False, index=False)
        j += stt*10
        
    t2 = time.time()
    return t2-t0

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
p = multiprocessing.Pool(24)
#[print(f'{i}') for i in p.map(comp_features, range(173, 204))]
[print(f'{i}') for i in p.map(partial(comp_features,year=year,client=client,network=network,stations=stations,channels=channels), jday_list)]

# for i in p.map(partial(comp_features, year=year, network=network, stations=stations, channels=channels), jday_list):
#     print(f'{i}')
p.close()
p.join()
