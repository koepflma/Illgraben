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
import sys

def comp_features(i):
    year = 2017
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

if __name__ == '__main__':

	ID = sys.argv[1]
	comp_features(ID)
