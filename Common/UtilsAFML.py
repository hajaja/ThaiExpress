# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import pdb, datetime, logging, os, shutil, re
from dateutil.parser import parse
import Common.Utils as Utils
reload(Utils)
######################################################################
# benchmark
######################################################################
def generateTimeClock(df, NMinPerBar = 5):
    # split
    df['SecondsFromPrevRow'] = df.index
    df['SecondsFromPrevRow'] = df['SecondsFromPrevRow'].diff()
    df['SecondsFromPrevRow'] = df['SecondsFromPrevRow'].apply(lambda x: np.nan if pd.isnull(x) else x.total_seconds())
    
    ixPrev = df.index[0]
    listDTStart = [df.index[0]]
    for ix, row in df.iterrows():
        boolOpenOfMarketAMPM = row['SecondsFromPrevRow'] > 60 * 60
        boolMaximumMin = (ix - ixPrev).total_seconds() >= NMinPerBar * 60
        if boolOpenOfMarketAMPM or boolMaximumMin:
            listDTStart.append(ix)
            ixPrev = ix
        else:
            pass

    # assign label
    sBarStart = pd.Series(listDTStart)
    df.loc[listDTStart, 'BarLabel'] = sBarStart.values
    df['BarLabel'] = df['BarLabel'].ffill()
    
    # generate bar
    gg = df.groupby('BarLabel')
    sOpen = gg['Open'].first()
    sHigh = gg['High'].max()
    sLow = gg['Low'].min()
    sClose = gg['Close'].last()
    sVolume = gg['Volume'].sum()
    sTradeDate = gg['trade_date'].first()
    sOI = gg['OI'].first()
    dfBar = pd.concat([sOpen, sHigh, sLow, sClose, sVolume, sTradeDate, sOI], axis=1)

    # change index name
    dfBar.index.name = 'dtStart'

    return dfBar


######################################################################
# volume clock
######################################################################
def generateVolumeClock1Min(df, NMinRolling = 60 * 8, NMinPerBar = 5):
    #NMinRolling = 60 * 8
    #NMinPerBar = 5
    NMinPerBarMax = NMinPerBar * 4

    # calculate volume per bar
    sVolumeMedian = df['Volume'].rolling(NMinRolling).median()
    sVolumeMean = df['Volume'].rolling(NMinRolling).mean()
    sVolumePerBar = pd.concat([sVolumeMedian, sVolumeMean], axis=1).max(axis=1)

    # split
    df['VolumePerBar'] = sVolumePerBar
    df['SecondsFromPrevRow'] = df.index
    df['SecondsFromPrevRow'] = df['SecondsFromPrevRow'].diff()
    df['SecondsFromPrevRow'] = df['SecondsFromPrevRow'].apply(lambda x: np.nan if pd.isnull(x) else x.total_seconds())
    volumeCumulated = 0
    ixPrev = df.index[0]
    listDTStart = [df.index[0]]
    for ix, row in df.iterrows():
        boolEnoughVolume = volumeCumulated >= row['VolumePerBar'] * NMinPerBar
        boolOpenOfMarketAMPM = row['SecondsFromPrevRow'] > 60 * 60
        boolMaximumMin = (ix - ixPrev).total_seconds() > NMinPerBarMax * 60
        if boolEnoughVolume or boolOpenOfMarketAMPM or boolMaximumMin:
            volumeCumulated = row['Volume']
            listDTStart.append(ix)
            ixPrev = ix
        else:
            volumeCumulated = volumeCumulated + row['Volume']

    # assign label
    sBarStart = pd.Series(listDTStart)
    df.loc[listDTStart, 'BarLabel'] = sBarStart.values
    df['BarLabel'] = df['BarLabel'].ffill()
    
    # generate bar
    gg = df.groupby('BarLabel')
    sOpen = gg['Open'].first()
    sHigh = gg['High'].max()
    sLow = gg['Low'].min()
    sClose = gg['Close'].last()
    sVolume = gg['Volume'].sum()
    sTradeDate = gg['trade_date'].first()
    sOI = gg['OI'].first()
    dfBar = pd.concat([sOpen, sHigh, sLow, sClose, sVolume, sTradeDate, sOI], axis=1)

    # change index name
    dfBar.index.name = 'dtStart'

    return dfBar

######################################################################
# tripple barrier method
######################################################################
def generateTrippleBarrierBar(df, NMinRolling = 60 * 8, NMinPerBarMax = 60, NVol = 3):
    # parameter
    #NMinPerBarMax = 60
    #NVol = 3

    # split
    df['SecondsFromPrevRow'] = df.index
    df['SecondsFromPrevRow'] = df['SecondsFromPrevRow'].diff()
    df['SecondsFromPrevRow'] = df['SecondsFromPrevRow'].apply(lambda x: np.nan if pd.isnull(x) else x.total_seconds())
    df['vol'] = df['PCT'].rolling(NMinRolling).std()

    # initialized
    volRolling = 0.001
    railUpper = df.iloc[0]['Close'] * (1 + NVol * volRolling)
    railLower = df.iloc[0]['Close'] * (1 - NVol * volRolling)
    ixPrev = df.index[0]
    listDTStart = [{'BarLabel':df.index[0], 'Upper': railUpper, 'Lower': railLower, 'VolRolling': volRolling}]
    # loop
    for ix, row in df.iterrows():
        boolOpenOfMarketAMPM = row['SecondsFromPrevRow'] > 60 * 60
        boolHorizontal = (ix - ixPrev).total_seconds() > NMinPerBarMax * 60
        boolUpper = row['Close'] > railUpper
        boolLower = row['Close'] < railLower
        if boolOpenOfMarketAMPM or boolHorizontal or boolUpper or boolLower:
            # append datetime
            ixPrev = ix
            # re-calculate the upper and lower rail
            volRolling = row['vol']
            railUpper = row['Close'] + NVol * volRolling
            railLower = row['Close'] - NVol * volRolling
            # append result
            listDTStart.append({'BarLabel': ix, 'Upper': railUpper, 'Lower': railLower, 'VolRolling': row['vol'], 'HorizontalHit': boolHorizontal})
        else:
            pass

    # assign label
    dfBarStart = pd.DataFrame(listDTStart).set_index('BarLabel').sort_index()
    df.loc[dfBarStart.index, 'BarLabel'] = dfBarStart.index
    for strColumn in dfBarStart.columns:
        df.loc[dfBarStart.index, strColumn] = dfBarStart[strColumn]
    df = df.ffill()
    
    # generate bar
    gg = df.groupby('BarLabel')
    sOpen = gg['Open'].first()
    sHigh = gg['High'].max()
    sLow = gg['Low'].min()
    sClose = gg['Close'].last()
    sVolume = gg['Volume'].sum()
    sTradeDate = gg['trade_date'].first()
    sOI = gg['OI'].first()
    dfBar = pd.concat([sOpen, sHigh, sLow, sClose, sVolume, sTradeDate, sOI], axis=1)

    # change index name
    dfBar.index.name = 'dtStart'

    return dfBar

######################################################################
# fractionally differential features
######################################################################
def fractionalDifferentiate(df, d = 0.5, NLookBack = 6):
    # generate weight
    dd = 1
    listCumProd = []
    for k in range(0, NLookBack):
        if k == 0:
            dd = 1
        else:
            dd = dd * (d - k + 1)
        listCumProd.append(dd)
    
    listWeight = []
    for k in range(0, NLookBack):
        weight = np.power(-1, k) * listCumProd[k] / np.math.factorial(k)
        listWeight.append(weight)
    
    # filter
    from scipy.signal import lfilter
    for strColumn in ['Close', 'OI', 'Volume']:
        df['Log_%s'%strColumn] = df[strColumn].apply(lambda x: np.log(x))
        df['FD_%s'%strColumn] = lfilter(listWeight, 1, df['Log_%s'%strColumn])

    # ADF test
    # from statsmodels.tsa.stattools import adfuller
    # result_PCT = adfuller(df['Close'].pct_change().dropna()[-10000:])
    # result_FD = adfuller(df['FD_Close'].pct_change().dropna()[-10000:])

    return df

######################################################################
# feature evaluation
######################################################################

######################################################################
# structural breaks
# to detect whether a strategy fails (then stop loss)
# key word: sequential detection
######################################################################
def testCUSUM(s):
    return

if __name__ == '__main__':
    # read data
    strFile1Min = '1MinExample.pickle'
    if os.path.exists(strFile1Min):
        df1Min = pd.read_pickle(strFile1Min)
    else:
        dictDataSpec = dict(Utils.dictDataSpecTemplate)
        dictDataSpec['Secu'] = 'j.dce'
        dictDataSpec['freq'] = '1min'
        df1Min = Utils.getTradingDataPoint_Commodity(dictDataSpec).replace(np.inf, np.nan)
        df1Min.to_pickle(strFile1Min)

    #dfBM5 = generateTimeClock(df1Min.copy(), 5)
    #dfBM10 = generateTimeClock(df1Min.copy(), 10)
    #dfVC = generateVolumeClock1Min(df1Min.copy())
    #dfTB = generateTrippleBarrierBar(df1Min)
    dfBM = generateTimeClock(df1Min, 20)
    #dfVC = generateVolumeClock1Min(df1Min)
    dfTB = generateTrippleBarrierBar(df1Min)

    #dfFD = fractionalDifferentiate(df1Min)

