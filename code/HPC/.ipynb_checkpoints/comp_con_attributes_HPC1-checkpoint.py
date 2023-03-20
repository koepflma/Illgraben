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
import os
import datetime
from ComputeAttributes_CH_V1 import *
from obspy import signal
from obspy.signal import cross_correlation
import time
import multiprocessing

def comp_features(i):
    year = 2020
    t0 = time.time()

    if not os.path.exists(f'../feature_files/{year}/ILLx8/{i}_*.csv'):
        st = obspy.read(f'../miniseed/{year}/use/ILL??/EHZ.D/*{i}')
        print(st[0].stats.starttime)
        st.merge(fill_value = 'interpolate')
        stt = st[0].stats.sampling_rate
        st.detrend('demean')
        st1 = st.copy()
        st1.filter('bandpass', freqmin=1, freqmax=10)

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
            with open(f'../feature_files/{year}/ILLx8/{tt.julday}_{tt.hour}-{tt.minute}-{tt.second}.{tt.microsecond}.csv', "a+") as f:
                d.to_csv(f, header=False, index=False)
            j += stt*10
    elif os.path.exists(f'../feature_files/{year}/ILLx8/{i}_*.csv'):
        pass

    t1 = time.time()
    #print(t1-t0)
    return t1-t0
        #print((len(st[0].data) - j)/stt)
pool = multiprocessing.Pool(24)
#[print(f'{i}') for i in pool.map(comp_features, range(173, 204))]
[print(f'{i}') for i in pool.map(comp_features, range(260, 367))]
