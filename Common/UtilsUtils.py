# 
import pandas as pd
import datetime
import numpy as np
import pdb
import scipy.io as sio
import os
import random
from scipy.signal import butter, lfilter, freqz
from scipy import signal
from dateutil.parser import parse
import gc

secondsAdvanceDayEnd = 3600 * 4
NTradingDayPerYear = 250

def useTrendStatToFilterIndicator(df, dictDataSpec):
    # define function, how to use trend to avoid trend failure
    def determineFailure(row, boolTrendType, strCriterion):
        PCTFromExtremeExtent = dictDataSpec['PCTFromExtremeExtent']
        NBarFromExtremeDuration = dictDataSpec['NBarFromExtremeDuration']

        # extent
        if boolTrendType == 'upTrend':
            boolTrendFailureExtent = row['Close'] > row['bottom'] * (1 + PCTFromExtremeExtent)
        elif boolTrendType == 'downTrend':
            boolTrendFailureExtent = row['Close'] < row['top'] * (1 - PCTFromExtremeExtent)
        
        # duration
        if boolTrendType == 'upTrend':
            nBar = row['nBar'] - row['nBarBottom']
            boolTrendFailureDuration = nBar > NBarFromExtremeDuration
        elif boolTrendType == 'downTrend':
            nBar = row['nBar'] - row['nBarTop']
            boolTrendFailureDuration = nBar > NBarFromExtremeDuration

        # return
        if strCriterion == 'extent':
            return boolTrendFailureExtent
        elif strCriterion == 'duration':
            return boolTrendFailureDuration
        elif strCriterion == 'extent&duration':
            return boolTrendFailureExtent & boolTrendFailureDuration
        elif strCriterion == 'extent|duration':
            return boolTrendFailureExtent | boolTrendFailureDuration
        else:
            return False

    # do not act against the trend
    strCriterion = dictDataSpec['strCriterion']
    alphaExtent = dictDataSpec['alphaExtent']
    alphaDurationCorrection = dictDataSpec['alphaDurationCorrection']
    
    dictDurationCorrectionQuantile = dictDataSpec['dictDurationCorrectionQuantile']
    dictExtentQuantile = dictDataSpec['dictExtentQuantile']
    dictDurationQuantile = dictDataSpec['dictDurationQuantile']

    PCTFromExtremeExtent = dictExtentQuantile[dictExtentQuantile.index >= alphaExtent-0.0001].values[0]
    NBarFromExtremeDuration = dictExtentQuantile[dictDurationQuantile.index >= alphaExtent-0.0001].values[0]
    NBarDurationCorrection = dictDurationCorrectionQuantile[dictDurationCorrectionQuantile.index >= alphaDurationCorrection-0.0001].values[0][0]
    
    dictDataSpec['PCTFromExtremeExtent'] = PCTFromExtremeExtent
    dictDataSpec['NBarFromExtremeDuration'] = NBarFromExtremeDuration
    dictDataSpec['NBarDurationCorrection'] = NBarDurationCorrection

    for ix, row in df.dropna().iterrows():
        boolUpTrendFailure = determineFailure(row, 'upTrend', strCriterion)
        boolDownTrendFailure = determineFailure(row, 'downTrend', strCriterion)
        if row['indicator'] == 1:
            NBarFromPrevExtreme = row['nBar'] - row['nBarBottom']
            #if (row['bottomConfirm'] & boolUpTrendFailure) or (row['topConfirm'] and not boolDownTrendFailure):
            if (row['bottomConfirm'] & boolUpTrendFailure):
                df.ix[ix, 'indicator'] = 0
            #'''
            elif row['topConfirm'] and (not boolDownTrendFailure):
                if row['top'] / row['bottom'] - 1 > dictDataSpec['pTrend'] * 2:
                    if NBarFromPrevExtreme < NBarDurationCorrection:
                        df.ix[ix, 'indicator'] = 0
                else:
                    df.ix[ix, 'indicator'] = 0
            #'''
        elif row['indicator'] == -1:
            NBarFromPrevExtreme = row['nBar'] - row['nBarTop']
            #if (row['topConfirm'] & boolDownTrendFailure) or (row['bottomConfirm'] and not boolUpTrendFailure):
            if (row['topConfirm'] & boolDownTrendFailure):
                df.ix[ix, 'indicator'] = 0
            #'''
            elif row['bottomConfirm'] and (not boolUpTrendFailure):
                if row['bottom'] / row['top'] - 1 < dictDataSpec['pTrend'] * 2:
                    if NBarFromPrevExtreme < NBarDurationCorrection:
                        df.ix[ix, 'indicator'] = 0
                else:
                    df.ix[ix, 'indicator'] = 0
            #'''

    # stop profit
    if dictDataSpec['boolStopProfitDynamic']:
        dictExtentQuantile = dictDataSpec['dictExtentQuantile']
        alphaStopProfit = dictDataSpec['alphaStopProfit']
        PCTFromExtremeStopProfit = dictExtentQuantile[dictExtentQuantile.index >= alphaStopProfit-0.0001].values[0]
        dfCopy = df.copy()
        dfCopy['indicator'] = dfCopy['indicator'].ffill()
        boolStopProfitLong = (df['bottomConfirm'])&(df['Close']>df['bottom']*(1+PCTFromExtremeStopProfit))
        boolStopProfitLong = boolStopProfitLong & (dfCopy['indicator']==1)
        df.ix[boolStopProfitLong, 'indicator'] = 0
        boolStopProfitShort = (df['topConfirm'])&(df['Close']<df['top']*(1-PCTFromExtremeStopProfit))
        boolStopProfitShort = boolStopProfitShort & (dfCopy['indicator']==-1)
        df.ix[boolStopProfitShort, 'indicator'] = 0

    # stop loss
    if dictDataSpec['boolStopLossDynamic']:
        dfCopy = df.copy()
        dfCopy['indicator'] = dfCopy['indicator'].ffill()
        boolStopLossLong = (df['topConfirm'])
        boolStopLossLong = boolStopLossLong & (dfCopy['indicator']==1)
        df.ix[boolStopLossLong, 'indicator'] = 0
        boolStopLossShort = (df['bottomConfirm'])
        boolStopLossShort = boolStopLossShort & (dfCopy['indicator']==-1)
        df.ix[boolStopLossShort, 'indicator'] = 0

    return df

def trendFindDecision(df, dictDataSpec):
    # use Matlab to get the trend stats
    pTrend = dictDataSpec['pTrend']
    freq = dictDataSpec['freq']
    Secu = dictDataSpec['Secu']
    strFilePrefix = dictDataSpec['strFilePrefix']
    strFileAddress = strFilePrefix + '/' + 'TrendStats' + '/' + Secu + freq + str(pTrend) + '.mat'
    if os.path.exists(strFileAddress) is False:
        from mlabwrap import mlab
        try:
            mlab.dumpStatistics(pTrend, df['vwp'].values.tolist(), strFileAddress, 0)
        except:
            mlab.dumpStatistics(pTrend, df['Close'].values.tolist(), strFileAddress, 0)

    dictMat = sio.loadmat(strFileAddress)
   
    # use statistics to mark top/bottom, and the moments when confirming top/bottom 
    yx1 = dictMat['yx1'].astype(int)
    yx2 = dictMat['yx2'].astype(int)
    yx1Confirmed = dictMat['yx1Confirmed'].astype(int)
    yx2Confirmed = dictMat['yx2Confirmed'].astype(int)
    extentQuantile = dictMat['extentQuantile']
    durationQuantile = dictMat['durationQuantile']
    durationCorrectionQuantile = dictMat['durationCorrectionQuantile']
    df['nBar'] = df.reset_index().index
    df['top'] = np.nan 
    df['bottom'] = np.nan 
    df['topConfirm'] = np.nan 
    df['bottomConfirm'] = np.nan 
    df['nBarTop'] = np.nan 
    df['nBarBottom'] = np.nan 
    
    df.ix[yx1Confirmed[:, 2]-1, 'bottom'] = df.ix[yx1[:, 2]-1, 'Close'].values
    df.ix[yx2Confirmed[:, 2]-1, 'top'] = df.ix[yx2[:, 2]-1, 'Close'].values
    df.ix[yx1Confirmed[:, 2]-1, 'bottomConfirm'] = True
    df.ix[yx2Confirmed[:, 2]-1, 'topConfirm'] = True
    
    df[['top', 'bottom']] = df[['top', 'bottom']].ffill().fillna(1)
    
    df.ix[df[df['topConfirm']==True].index, 'bottomConfirm'] = False
    df.ix[df[df['bottomConfirm']==True].index, 'topConfirm'] = False
    
    df.ix[yx2Confirmed[:, 2]-1, 'nBarTop'] = df.ix[yx2[:, 2]-1, 'nBar'].values
    df.ix[yx1Confirmed[:, 2]-1, 'nBarBottom'] = df.ix[yx1[:, 2]-1, 'nBar'].values
    
    df[['topConfirm', 'bottomConfirm']] = df[['topConfirm', 'bottomConfirm']].ffill().fillna(False)
    df[['nBarTop', 'nBarBottom']] = df[['nBarTop', 'nBarBottom']].ffill().fillna(0)
    
    
    dictExtentQuantile = pd.DataFrame(extentQuantile).set_index(0)
    dictDurationQuantile = pd.DataFrame(durationQuantile).set_index(0)
    dictDurationCorrectionQuantile = pd.DataFrame(durationCorrectionQuantile).set_index(0)
    
    return df, dictExtentQuantile, dictDurationQuantile, dictDurationCorrectionQuantile


def keepOperation(s):
    s = s.ffill().fillna(0)
    indexNonOperation = s.diff()==0
    s[indexNonOperation] = np.nan
    return s

def closePositionAtDayEnd(df, strEnd=None, boolIgnoreExistIndicator=True):
    if strEnd is None:
        return df

    if 5 in df.index.weekday:   # whether there is night market
        if 'dtForDaily' not in df.columns:
            print 'dtForDaily NA'
            df['dt'] = df.index
        else:
            df['dt'] = df['dtForDaily']
    else:
        df['dt'] = df.index

    df = df[df['dt'].apply(lambda x: type(x)!=pd.tslib.NaT)]
    '''
    for ix, row in df.iterrows():
        if type(row['dt']) != pd.tslib.NatType:
            print row
    '''
    def funcGetDT1455(seriesDT):
        try:
            dtToClosePosition = seriesDT.index[-5]
        except:
            dtToClosePosition = seriesDT.index[0]
        return dtToClosePosition

    if strEnd == 'day':
        seriesDTEnd = df.groupby(df.index.date)['dt'].apply(funcGetDT1455)
    elif strEnd == 'week':
        seriesDTEnd = df.groupby(df.index.strftime('%Y%W'))['dt'].apply(funcGetDT1455)

    if boolIgnoreExistIndicator:
        df.ix[(df.index.isin(seriesDTEnd.values))&(df['indicator'].isnull()), 'indicator'] = 0
    else:
        df.ix[df.index.isin(seriesDTEnd.values), 'indicator'] = 0

    del seriesDTEnd
    gc.collect()
    return df

def afterFixedEnter(df, dictDataSpec):
    timeEnter = dictDataSpec['timeEnter']
    seriesReturnInDayEnd = df.ix[df.index.time>=timeEnter, 'returnInDayEnd']
    def funcReturnInDayEnd(s):
        return (s+1).cumprod()[-1]-1
    def funcReturnDTStart(s):
        return s.index[0]
    seriesDTStart = seriesReturnInDayEnd.groupby(seriesReturnInDayEnd.index.date).apply(funcReturnDTStart)
    '''
    seriesReturnInDayEnd = seriesReturnInDayEnd.groupby(seriesReturnInDayEnd.index.date).apply(funcReturnInDayEnd)
    df.ix[seriesDTStart.values, 'returnInDayEnd'] = seriesReturnInDayEnd.values
    '''
    df = df.ix[seriesDTStart.values]
    return df
