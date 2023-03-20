import numpy as np
import pandas as pd
import obspy
import matplotlib
import matplotlib.pyplot as plt
import os
from datetime import datetime, timedelta
import time
from scipy import signal
from ComputeAttributes_CH_V1 import *
from DF_detections import *


# Functions to read in and process detecton files and meteo files-----------------------------------------------
def read_features(year, file_ending):
    '''read in detection file'''
    class_res = pd.read_csv(f'../detections/{year}/{year}_{file_ending}.txt')
    class_res['datetimes'] = pd.to_datetime(class_res['datetimes'])
    dttimes = class_res['datetimes']
    all_class = class_res['classes']

    # Get times of all slope failure events
    tslops = []
    dslops = []
    for x,y in zip(dttimes, all_class):
        if y != 0:
            tslops.append(x) # list with timestamps when detection
            dslops.append(y) # list with 1's for slope failure detections
    return(class_res, dttimes, all_class, tslops, dslops)

def read_MeteoSwiss(path, files='all', header=14):
    '''Get precipitation data (CransMontana) (ns short for the german Niederschlag (precipitation))'''
    if files == 'all':
        lsmet = os.listdir(path)
    else:
        lsmet = files
        
    met = pd.DataFrame()
    for f in lsmet:
        filepath = os.path.join(path, f)
        d = pd.read_csv(filepath, header = header, delim_whitespace = True)
        d = d.assign(Date = pd.NaT)
        d.Date = pd.to_datetime(dict(year=d.JAHR, month=d.MO, day=d.TG, hour=d.HH, minute=d.MM)) # convert columns (dtype=object) to dates
        d = d.drop(['JAHR','MO','TG','HH','MM','STA'], axis=1) # drop cols, bcs they are in date now
        met = met.append(d)
    
    met = met.set_index('Date') 
    return met

def read_meteo(class_res, year):
    ''' Group by day for barplot (noise class will be 0 still)'''
    df_group_d = class_res.groupby([class_res['datetimes'].dt.date]).sum() # daily
    df_group = class_res.resample('H', on='datetimes').sum() # hourly

    ns_dat = read_MeteoSwiss('../meteodata/', files=[f'{year}.dat'])
    ns_dat['datetime'] = ns_dat.index
    ns_dat = ns_dat.replace(32767, np.nan) # replace value for no data with NaN
    
    # Group by date/hour and sum
    ns_group_d = ns_dat.groupby(ns_dat['datetime'].dt.date).sum() # daily
    ns_group = ns_dat.resample('H', on='datetime').sum() # hourly
    return(df_group_d, df_group, ns_dat, ns_group_d, ns_group)
    
# Functions for percipitation/detection plot to mask and resample--------------------------------------------------
def mask_debrisflow(class_res, ns_dat, ts, te):
    # Mask debris flow events (3h before start and 1h after end)
    
    class_mask = class_res.copy()

    ns_mask = (ns_dat['datetime'] > class_res['datetimes'][0]) & (ns_dat['datetime'] <= class_res['datetimes'][len(class_res)-1])
    ns_dat_per = ns_dat[ns_mask].copy()
    ns_dat_mask = ns_dat_per.copy()

    for i in range(len(ts)):
        tds = obspy.UTCDateTime(ts[i]).datetime
        tde = obspy.UTCDateTime(te[i]).datetime
        dec_mask = (class_res['datetimes'] > tds - timedelta(seconds=3*3600)) & (class_res['datetimes'] <= tde+ timedelta(seconds=1*3600))
        class_mask.loc[class_mask[dec_mask].index, 'classes'] = 0
        ns_mask = (ns_dat['datetime'] > tds - timedelta(seconds=3*3600)) & (ns_dat['datetime'] <= tde+ timedelta(seconds=1*3600))
        ns_dat_mask.loc[ns_dat_per[ns_mask].index, '267'] = 0
        
    return(class_mask, ns_dat_per, ns_dat_mask)

def mask_polar(class_res, class_mask, ns_dat, ns_dat_per, ns_dat_mask):
    # create df for polar plot
    df_polar = class_res.groupby([class_res['datetimes'].dt.hour]).sum()

    df_polar_mask = class_mask.groupby([class_mask['datetimes'].dt.hour]).sum()
    df_polar_mask.classes = df_polar_mask.classes

    ns_polar = ns_dat.groupby([ns_dat_per['datetime'].dt.hour]).sum()

    ns_polar_mask = ns_dat_mask.groupby([ns_dat_mask['datetime'].dt.hour]).sum()
    return(df_polar, df_polar_mask, ns_polar, ns_polar_mask)

def some_processing(class_res, class_mask, ns_dat_per, ns_dat_mask):
    # Group hourly
    group_DF = class_res.copy().resample('H', on='datetimes').sum()[1:] # hourly
    group_DF.index.rename('Date', inplace=True)
    group_cm = class_mask.copy().resample('H', on='datetimes').sum()[1:] # hourly masked DFs
    group_cm.index.rename('Date', inplace=True)

    # Merge classes and precipitation data
    group_all_DF = group_DF.merge(ns_dat_per, how='inner', on='Date') # hourly
    group_all = group_cm.merge(ns_dat_mask, how='inner', on='Date') # hourly masked DFs

    # Group every 6 hours for clearer picture
    group_all5_DF = group_all_DF.copy().resample('6H', on='datetime').sum() # resampled 6h
    group_all5 = group_all.copy().resample('6H', on='datetime').sum()
    return(group_all_DF, group_all, group_all5_DF, group_all5)
    
# Functions for moisture/temperature/percipitation/detection plot to mask and resample----------------------------
def mask_moisture(class_res, ts, te):
    df_mois = pd.read_csv(f'../meteodata/Illhorn1_2019-2021.csv') # load moisture and temperature (ground)
    df_mois['datetime'] = pd.to_datetime(df_mois['Illhorn1'])
    df_mois = df_mois.set_index('datetime', drop=False)
    mois_mask = (df_mois['datetime'] > class_res['datetimes'][0]) & (df_mois['datetime'] <= class_res['datetimes'][len(class_res)-1])
    df_mois_per = df_mois[mois_mask].copy() # df from begining to end of detection season
    df_mois_mask = df_mois_per.copy()

    for i in range(len(ts)):
        tds = obspy.UTCDateTime(ts[i]).datetime
        tde = obspy.UTCDateTime(te[i]).datetime
        mois_mask = (df_mois['datetime'] > tds - timedelta(seconds=3*3600)) & (df_mois['datetime'] <= tde+ timedelta(seconds=1*3600))
        df_mois_mask.loc[df_mois_per[mois_mask].index, df_mois.columns.values[1:8]] = 0 # df without DFs
    return(df_mois_per,df_mois_mask)

def some_processing_with_moisture(class_res, class_mask, ns_dat_per, ns_dat_mask, df_mois_per, df_mois_mask):
    # Group hourly
    group_DF = class_res.copy().resample('H', on='datetimes').sum()[1:] # hourly
    group_DF.index.rename('Date', inplace=True)
    group_cm = class_mask.copy().resample('H', on='datetimes').sum()[1:] # masked DF
    group_cm.index.rename('Date', inplace=True)

    df_mois_DF = df_mois_per.copy().resample('H', on='datetime').sum()[1:] # seism. season hourly
    df_mois_DF.index.rename('Date', inplace=True)
    df_mois_cm = df_mois_mask.copy().resample('H', on='datetime').sum()[1:] # masked DF
    df_mois_cm.index.rename('Date', inplace=True)
    # df_mois_cm = df_mois_cm.drop(['datetime'], axis=1)

    # Merge classes and precipitation data
    group_all_DF = group_DF.merge(ns_dat_per, how='inner', on='Date').merge(df_mois_DF, how='inner', on='Date') # hourly
    group_all = group_cm.merge(ns_dat_mask, how='inner', on='Date').merge(df_mois_cm, how='inner', on='Date') # masked DF

    # Group every 6 hours for clearer picture
    group_all5_DF = group_all_DF.copy().resample('6H', on='datetime').sum() # resampled 6h
    group_all5 = group_all.copy().resample('6H', on='datetime').sum() # masked DF and resampled 6h
    return(group_all_DF, group_all, group_all5_DF, group_all5)    
