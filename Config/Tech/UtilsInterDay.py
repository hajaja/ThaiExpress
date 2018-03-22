import pandas as pd
import datetime
import numpy as np
import pdb
import scipy.io as sio
import os
import gc
import random
from scipy.signal import butter, lfilter, freqz
from scipy import signal

import ThaiExpress.Common.Utils as Utils
reload(Utils)
import logging

###################
# wrapper
###################
def generateIndicatorTech(dictDataSpec):
    """
    wrapper for all indicator generator
    """
    if dictDataSpec['strModelName'] == 'MARUBOZU':
        seriesIndicator = generateIndicatorMARUBOZU(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'MA':
        seriesIndicator = generateIndicatorMA(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'HL':
        seriesIndicator = generateIndicatorHL(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'BT':
        seriesIndicator = generateIndicatorBT(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'BTBreak':
        seriesIndicator = generateIndicatorBTBreak(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'TSCRebound':
        seriesIndicator = generateIndicatorTSCRebound(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'TSCReboundA':
        seriesIndicator = generateIndicatorTSCReboundA(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'TSCReboundASL':
        seriesIndicator = generateIndicatorTSCReboundASL(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'TSC':
        seriesIndicator = generateIndicatorTSCReboundASL(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'AOGE':
        seriesIndicator = generateIndicatorAOGE(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'TALib':
        seriesIndicator = generateIndicatorTALib(dictDataSpec)

    seriesIndicator[dictDataSpec['df']['Volume'] < Utils.NThresholdVolume] = 0
    seriesIndicator = Utils.UtilsUtils.keepOperation(seriesIndicator)
    return seriesIndicator
##################
# inter day strategy
#################
def generateIndicatorMARUBOZU(dictDataSpec):
    import talib
    reload(talib)
    # param
    NDayHist = dictDataSpec['NDayHist']
    LLThreshold = dictDataSpec['LLThreshold']
    df = dictDataSpec['df']
    sIndicator = df['Close'].apply(lambda x: np.nan)

    df['PCTHLABS'] = talib.ATR(df.High.values, df.Low.values, df.Close.values, NDayHist)
    df['PCTHLABS'] = df['PCTHLABS'] / df['Close'].rolling(NDayHist).mean() * LLThreshold
    df['LL'] = df.apply(lambda row: abs(row['High'] / row['Low'] - 1) > row['PCTHLABS'], axis=1)
    df['Bullish'] = df.apply(lambda row: row['Close'] > row['Open'], axis=1)
    df['LLBullish'] = df.apply(lambda row: row['LL'] & row['Bullish'], axis=1)
    df['LLBearish'] = df.apply(lambda row: row['LL'] & (not row['Bullish']), axis=1)
    df['LongMARUBOZU'] = df.apply(lambda row: abs(row['Open']/row['Low']-1) < min(0.001, row['PCTHLABS'] * 0.1), axis=1)
    df['ShortMARUBOZU'] = df.apply(lambda row: abs(row['Open']/row['High']-1) < min(0.001, row['PCTHLABS'] * 0.1), axis=1)

    df.ix[df['LLBullish'] & df['LongMARUBOZU'], 'indicator'] = 1
    df.ix[df['LLBearish'] & df['ShortMARUBOZU'], 'indicator'] = -1

    sIndicator = df['indicator']
    
    return sIndicator

def generateIndicatorTALib(dictDataSpec):
    """
    using TA-Lib
    
    Parameters:
    ----------
    * dictDataSpec: df for data, and many parameters as defined in Common/ParamRange.py
                                            
    Returns:
    -------
    * ret: pandas.Series, indicator, including -1 for sell, 1 for buy, and 0 for closing
    """
    import UtilsTALib
    reload(UtilsTALib)
    ret = UtilsTALib.generateIndicator(dictDataSpec)
    
    return ret

def generateIndicatorMA(dictDataSpec):
    """
    long when up crossing MA(High, NDaySlow)
    sell when down crossing MA(Low, NDaySlow)
    no stoploss
    
    Parameters:
    ----------
    * dictDataSpec: df for data, and many parameters as defined in Common/ParamRange.py
                                            
    Returns:
    -------
    * ret: pandas.Series, indicator, including -1 for sell, 1 for buy, and 0 for closing
    """

    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan

    # param
    NDayFast = dictDataSpec['NDayFast']
    NDaySlow = dictDataSpec['NDaySlow']

    # indicator for enter
    strPrice = 'Close'
    sFast = df[strPrice].rolling(NDayFast).mean()

    '''
    sUpper = df['High'].rolling(NDaySlow).mean()
    sLower = df['Low'].rolling(NDaySlow).mean()
    #'''

    #'''
    sUpper = df[strPrice].rolling(NDaySlow).mean()
    sLower = sUpper
    #'''

    sDiffUpper = sFast - sUpper
    sDiffUpperPrev = sDiffUpper.shift(1)
    sDiffLower = sFast - sLower
    sDiffLowerPrev = sDiffLower.shift(1)
    df.ix[(sDiffUpper>0)&(sDiffUpperPrev<=0), 'indicator'] = 1
    df.ix[(sDiffLower<0)&(sDiffLowerPrev>=0), 'indicator'] = -1
    del sDiffUpper, sDiffUpperPrev, sDiffLower, sDiffLowerPrev
    
    ret = df['indicator'].copy()
    del df, sFast
    gc.collect()
    
    return ret

def generateIndicatorHL(dictDataSpec):
    """
    long when up crossing 0.95 percentile of past NDayHist close
    sell when dn crossing 0.05 percentile of past NDayHist close
    
    close long when dn crossing 0.05 percentile of past NDayHistSL close
    close short when up crossing 0.95 percentile of past NDayHistSL close
    
    Parameters:
    ----------
    * dictDataSpec: df for data, and many parameters as defined in Common/ParamRange.py
                                            
    Returns:
    -------
    * ret: pandas.Series, indicator, including -1 for sell, 1 for buy, and 0 for closing
    """
    
    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan

    # param
    NDayHist = dictDataSpec['NDayHist']
    NDayHistSL = dictDataSpec['NDayHistSL']

    # indicator for enter
    strPrice = 'Close'
    sClose = df[strPrice]
    def func2ndMax(s, n):
        NPoint = min(s.size/8, n)
        return np.sort(s)[-NPoint-1]
    def func2ndMin(s, n):
        NPoint = min(s.size/8, n)
        return np.sort(s)[NPoint]

    #NPoint = int(np.ceil(NDayHist * 0.1))
    NPoint = 3

    # long when reaching the 2nd highest, short when reaching the 2nd lowest
    sNDayHigh = df[strPrice].rolling(NDayHist).apply(lambda x: func2ndMax(x, NPoint)).shift(1)
    sNDayLow = df[strPrice].rolling(NDayHist).apply(lambda x: func2ndMin(x, NPoint)).shift(1)
    sDiffUpper = sClose - sNDayHigh
    sDiffUpperPrev = sDiffUpper.shift(1)
    sDiffLower = sClose - sNDayLow
    sDiffLowerPrev = sDiffLower.shift(1)
    
    df.ix[(sDiffUpper>0)&(sDiffUpperPrev<=0), 'indicator'] = 1
    df.ix[(sDiffLower<0)&(sDiffLowerPrev>=0), 'indicator'] = -1

    # close long when reaching the 2nd lowest, close short when reaching the 2nd highest
    sNDayHighSL = df[strPrice].rolling(NDayHistSL).apply(lambda x: func2ndMax(x, NPoint)).shift(1)
    sNDayLowSL = df[strPrice].rolling(NDayHistSL).apply(lambda x: func2ndMin(x, NPoint)).shift(1)
    sDiffUpperSL = sClose - sNDayHighSL
    sDiffUpperPrevSL = sDiffUpperSL.shift(1)
    sDiffLowerSL = sClose - sNDayLowSL
    sDiffLowerPrevSL = sDiffLowerSL.shift(1)
    
    sIndicator = df['indicator'].ffill()
    df.ix[(sDiffUpperSL>0)&(sDiffUpperPrevSL<=0)&(sIndicator==-1), 'indicator'] = 0
    df.ix[(sDiffLowerSL<0)&(sDiffLowerPrevSL>=0)&(sIndicator==1), 'indicator'] = 0

    ret = df['indicator'].copy()
    del df, sClose, sNDayHigh, sNDayLow, sDiffUpper, sDiffUpperPrev, sDiffLower, sDiffLowerPrev
    gc.collect()
    return ret

def generateIndicatorBTBreak(dictDataSpec):
    """
    """
    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan

    # indicator for enter
    strPrice = 'Close'
    s = df[strPrice]
    #  
    def funcVShape(array):
        if array[0] < array[1] and array[1] < array[2] and array[2] > array[3] and array[3] > array[4]:
            ret = 1
        elif array[0] > array[1] and array[1] > array[2] and array[2] < array[3] and array[3] < array[4]:
            ret = -1
        else:
            ret = 0
        return ret
    sVShape = s.rolling(5).apply(funcVShape).shift(-2)
    dfVShape = pd.concat([s, sVShape], axis=1)
    dfVShape.columns = ['Close', 'VA']
    dfVShape['V'] = dfVShape.ix[sVShape==-1, 'Close']
    dfVShape.ix[sVShape==-1,'VDT'] = dfVShape[sVShape==-1].index
    dfVShape['A'] = dfVShape.ix[sVShape==1, 'Close']
    dfVShape.ix[sVShape==1,'ADT'] = dfVShape[sVShape==1].index
    dfVShape[['A', 'V', 'ADT', 'VDT']] = dfVShape[['A', 'V', 'ADT', 'VDT']].ffill().shift(2)

    # HL
    strToLog = 'calculate HL'
    logging.log(logging.INFO, strToLog)
    #p = s.pct_change().abs().median()
    #p = s.pct_change().abs().quantile(0.5)
    p = dictDataSpec['p']
    import RoyalMountain.Tech.BTRecognizer as BTRecognizer
    reload(BTRecognizer)
    dfHL = BTRecognizer.funcBT(df, strPrice, p)
    dfHL = dfHL[dfHL['indicator'].isnull()==False]
    dictReplace = {'B':1, 'T':2,}
    dfHL['indicator'] = dfHL['indicator'].apply(lambda x: dictReplace[x])
    dfHL = dfHL.rename(columns={'Close': 'close'})

    if dfHL.empty or dfHL.index.size < 5:
        return df['indicator']
    
    # max(low, prevLow) & valid low 
    strToLog = 'max(low, prevLow) & valid low'
    logging.log(logging.INFO, strToLog)

    dfL = dfHL[dfHL['indicator']==1]
    dfL['Max_Low_PreLow'] = pd.concat([dfL['close'], dfL['close'].shift(1)], axis=1).max(1)
    ixValidLow = dfL[dfL['close']>=dfL['Max_Low_PreLow']].index
    dfL['HighValid'] = False
    dfL.ix[ixValidLow, 'HighValid'] = True
    df.ix[dfL.index, 'HighValid_Original'] = dfL['HighValid']
    df.ix[dfL.index, 'Max_Low_PreLow_Original'] = dfL['Max_Low_PreLow']

    dfH = dfHL[dfHL['indicator']==2]
    dfH['Min_High_PreHigh'] = pd.concat([dfH['close'], dfH['close'].shift(1)], axis=1).min(1)
    ixValidHigh = dfH[dfH['close']<=dfH['Min_High_PreHigh']].index
    dfH['LowValid'] = False
    dfH.ix[ixValidHigh, 'LowValid'] = True
    df.ix[dfH.index, 'LowValid_Original'] = dfH['LowValid']
    df.ix[dfH.index, 'Min_High_PreHigh_Original'] = dfH['Min_High_PreHigh']

    df.ix[dfHL.index, 'BT'] = dfHL['indicator']
    df.ix[dfL.index, 'Bot_Original'] = dfL['close']
    df.ix[dfH.index, 'Top_Original'] = dfH['close']

    df['A'] = dfVShape['A']
    df['V'] = dfVShape['V']
    df['ADT'] = dfVShape['ADT']
    df['VDT'] = dfVShape['VDT']

    # confirm bottom & top
    strToLog = 'confirm bottom & top'
    logging.log(logging.INFO, strToLog)

    df['BotConfirm'] = np.nan
    df['TopConfirm'] = np.nan
    df['BotObservable'] = np.nan
    df['TopObservable'] = np.nan
    Bot = np.nan
    Top = np.nan
    for ix, row in df.iterrows():
        if row['Close']/Bot > (1+p):
            df.ix[ix, 'BotConfirm'] = True
            df.ix[ix, 'Bot'] = Bot
            df.ix[ix, 'Max_Low_PreLow'] = Max_Low_PreLow
            df.ix[ix, 'HighValid'] = HighValid
            Bot = np.nan
        elif row['Close']/Top < (1-p):
            df.ix[ix, 'TopConfirm'] = True
            df.ix[ix, 'Top'] = Top
            df.ix[ix, 'Min_High_PreHigh'] = Min_High_PreHigh
            df.ix[ix, 'LowValid'] = LowValid
            Top = np.nan

        if pd.isnull(row['Bot_Original']) is False:
            Bot = row['Bot_Original']
            Max_Low_PreLow = row['Max_Low_PreLow_Original']
            HighValid = row['HighValid_Original']
        elif pd.isnull(row['Top_Original']) is False:
            Top = row['Top_Original']
            Min_High_PreHigh = row['Min_High_PreHigh_Original']
            LowValid = row['LowValid_Original']
        pass
    #raise Exception
    #df[['Close', 'Bot_Original', 'Bot', 'HighValid', 'Max_Low_PreLow']]

    # iterate
    strToLog = 'iterate'
    logging.log(logging.INFO, strToLog)
    #df = df.ffill()
    #df['Bot'] = df['Bot'].shift(1)
    #df['Top'] = df['Top'].shift(1)
    df['Max_Low_PreLow'] = df['Max_Low_PreLow'].shift(1)
    df['Min_High_PreHigh'] = df['Min_High_PreHigh'].shift(1)
    df = df.ffill()
    listIndicator = []
    listDictRetSL = []
    indicatorCurrent = 0
    dtEnter = df.index.max()
    for ix, row in df.iterrows():
        # enter
        if indicatorCurrent == 0:
            if row['Close'] > row['Top'] and row['HighValid']:
                indicatorNext = 1
                dtEnter = ix
            elif row['Close'] < row['Bot'] and row['LowValid']:
                indicatorNext = -1
                dtEnter = ix
            else:
                indicatorNext = 0
        # close
        elif indicatorCurrent == 1:
            priceToCloseLong = row['Low']
            if priceToCloseLong < row['Max_Low_PreLow']:
                indicatorNext = 0
                listDictRetSL.append({'TradingDay':ix, 'PCT':row['Max_Low_PreLow']/rowPrev['Close']-1, 'direction': indicatorCurrent})
                if priceToCloseLong < row['Bot']:
                    #indicatorNext = -1
                    #dtEnter = ix
                    pass
            elif priceToCloseLong < row['V'] and dtEnter < row['VDT']:
                indicatorNext = 0
                listDictRetSL.append({'TradingDay':ix, 'PCT':row['V']/rowPrev['Close']-1, 'direction': indicatorCurrent})
            elif row['TopConfirm'] and row['LowValid']:
                indicatorNext = 0
                pass
            else:
                indicatorNext = 1
        elif indicatorCurrent == -1:
            priceToCloseShort = row['High']
            if priceToCloseShort > row['Min_High_PreHigh']:
                indicatorNext = 0
                listDictRetSL.append({'TradingDay':ix, 'PCT':row['Min_High_PreHigh']/rowPrev['Close']-1, 'direction': indicatorCurrent})
                if priceToCloseShort > row['Top']:
                    #indicatorNext = 1
                    #dtEnter = ix
                    pass
            elif priceToCloseShort > row['A'] and dtEnter < row['ADT']:
                if ix == datetime.datetime(2016,12,13):
                    #raise Exception
                    pass
                indicatorNext = 0
                listDictRetSL.append({'TradingDay':ix, 'PCT':row['A']/rowPrev['Close']-1, 'direction': indicatorCurrent})
            elif row['BotConfirm'] and row['HighValid']:
                indicatorNext = 0
                pass
            else:
                indicatorNext = -1
        else:
            print 'error, incorrect indicatorCurrent: %d'%indicatorCurrent

        listIndicator.append(indicatorNext)
        indicatorCurrent = indicatorNext
        rowPrev = row

    df['indicator'] = listIndicator
    df.ix[dfHL.index, 'BTOriginal'] = dfHL['indicator']

    '''
    # stoploss
    dfPCTSL = pd.DataFrame(listDictRetSL).set_index('TradingDay')
    dfPCTSL['PCTHold'] = dfPCTSL['PCT'] * dfPCTSL['direction'] - 0.001

    # PCTHold
    df['PCTHold'] = df['Close'].pct_change() * df['indicator'].shift(1)
    df.ix[dfPCTSL.index, 'SL'] = True
    df.ix[dfPCTSL.index, 'PCTHold'] = dfPCTSL['PCTHold']

    # enter & close
    sIndicator = df['indicator']
    sIndicator = Utils.UtilsUtils.keepOperation(sIndicator)
    df['Value'] = (1+df['PCTHold']).cumprod()
    '''
    #df.to_pickle(dictDataSpec['Secu'] + '.pickle')

    ret = df['indicator'].copy()
    del df, dfHL
    gc.collect()
    
    return ret

def generateIndicatorBT(dictDataSpec):
    """
    long when up crossing the previous top
    sell when dn crossing the previous bottom
    
    stoploss: no
    
    Note:
    ----------
    the top and bottom are calculated by the HLScript, and the only parameter is p, which is the threshold of a trend.
    p = Np * sClose.pct_change().std()
    it is better to use sClose.pct_change().rolling(240).std() to determine p
    
    Parameters:
    ----------
    * dictDataSpec: df for data, and many parameters as defined in Common/ParamRange.py
                                            
    Returns:
    ----------
    * ret: pandas.Series, indicator, including -1 for sell, 1 for buy, and 0 for closing
    """
    import RoyalMountain.Tech.BTRecognizer as BTRecognizer
    reload(BTRecognizer)

    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan

    # param
    p = dictDataSpec['p']

    # indicator for enter
    strPrice = 'Close'
    s = df[strPrice]
    p = s.pct_change().median()
    dfHL = BTRecognizer.funcBT(df, strPrice, p)
    if dfHL.empty:
        return df['indicator']
    
    dfH = dfHL[dfHL['indicator']==2]
    for ix, row in dfH.iterrows():
        H = row['close']
        sCandidate = s[(s.index > ix)&(s > H)]
        if sCandidate.empty is False:
            ixBreak = sCandidate.index[0]
            df.ix[ixBreak, 'indicator'] = 1

    dfL = dfHL[dfHL['indicator']==1]
    for ix, row in dfL.iterrows():
        L = row['close']
        sCandidate = s[(s.index > ix)&(s < L)]
        if sCandidate.empty is False:
            ixBreak = sCandidate.index[0]
            df.ix[ixBreak, 'indicator'] = -1

    ret = df['indicator'].copy()
    del df, dfHL, dfH, dfL
    gc.collect()
    
    return ret

def generateIndicatorAOGE(dictDataSpec):
    """
    this is similar with BT, the difference is at the stoploss.

    long when up crossing the previous top
    sell when dn crossing the previous bottom
    close long/short position when the average turnovervalue of current trend is lower then the previous trend (or previous similar trend). 
    
    Note:
    ----------
    the top and bottom are calculated by the HLScript, and the only parameter is p, which is the threshold of a trend.
    p = Np * sClose.pct_change().std()
    it is better to use sClose.pct_change().rolling(240).std() to determine p
    
    Parameters:
    ----------
    * dictDataSpec: df for data, and many parameters as defined in Common/ParamRange.py
    * ratioAOGE: df.ix[(sIndicator==1) & (df['Close'] < (1+p*0.5)*df['PrevLow']) & (df['VolumeAvg'] < df[strPastVolume]*ratioAOGE), 'indicator'] = 0
                                            
    Returns:
    ----------
    * ret: pandas.Series, indicator, including -1 for sell, 1 for buy, and 0 for closing
    """
    
    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan

    # param
    Np = dictDataSpec['Np']
    ratioAOGE = dictDataSpec['ratioAOGE']
    strPastVolume = dictDataSpec['strPastVolume']

    # find bottom & top points
    strPrice = 'Close'
    s = df[strPrice]
    p = Np * s.pct_change().std()
    dfHL = HLScript.funcHL(s, p)
    if dfHL.empty:
        return df['indicator']

    # when to confirm the bottom and top points
    ixPrev = dfHL.index[0]
    rowPrev = dfHL.ix[ixPrev]
    dfHL['dtConfirm'] = np.nan
    dfHL['indicator'] = dfHL['indicator'].astype(int)
    for ix, row in dfHL.ix[1:].iterrows():
        sCloseInterval = s[(s.index>ixPrev)&(s.index<=ix)]
        if int(rowPrev['indicator']) == 1:
            sCloseCandidate = sCloseInterval[sCloseInterval/rowPrev['close'] > (1+p)]
        elif int(rowPrev['indicator']) == 2:
            sCloseCandidate = sCloseInterval[sCloseInterval/rowPrev['close'] < (1-p)]
        else:
            strToLog = "UNKNOWN row['indicator']=%d"%rowPrev['indicator']
            logging.log(logging.ERRROR, strToLog)
            sCloseCandidate = None

        if sCloseCandidate is not None and sCloseCandidate.empty is False:
            ixConfirm = sCloseCandidate.index[0]
            dfHL.ix[ixPrev, 'dtConfirm'] = ixConfirm
        else:
            strToLog = "empty dtConirm:\n Secu=%s, ixHLPrev=%s, ixHL=%s"%(dictDataSpec['Secu'], ixPrev, ix)
            logging.log(logging.ERROR, strToLog)
            dfHL.ix[ixPrev, 'dtConfirm'] = ix
        
        ixPrev = ix
        rowPrev = row

    # split the close
    df['VolumeAvg'] = np.nan
    dfHL['VolumeAvg'] = np.nan
    ixPrev = dfHL.index[0]
    dtConfirmPrev = dfHL.ix[0, 'dtConfirm']
    for ix, row in dfHL.ix[1:].iterrows():
        dtConfirm = row['dtConfirm']
        dfHL.ix[ixPrev, 'VolumeAvg'] = df.ix[ixPrev:ix, 'Volume'].mean()
        if type(dtConfirm) is pd.tslib.Timestamp:
            df.ix[dtConfirmPrev: dtConfirm, 'VolumeAvg'] = df.ix[ixPrev: dtConfirm, 'Volume'].expanding().mean().ix[dtConfirmPrev: dtConfirm]
        ixPrev = ix
        dtConfirmPrev = dtConfirm

    # drop the last for now
    dfHL = dfHL.ix[:-1]
    
    # add VolumeAvgPast1 to df
    dfHL['VolumeAvgPast1'] = dfHL['VolumeAvg'].shift(1)
    dfHL['VolumeAvgPast2'] = dfHL['VolumeAvg'].shift(2)
    #df.ix[dfHL.index, 'VolumeAvgPast1'] = dfHL['VolumeAvgPast1']
    #df.ix[dfHL.index, 'VolumeAvgPast2'] = dfHL['VolumeAvgPast2']

    df.ix[dfHL['dtConfirm'], 'VolumeAvgPast2'] = dfHL['VolumeAvgPast2']
    df.ix[dfHL['dtConfirm'], 'VolumeAvgPast1'] = dfHL['VolumeAvgPast1']
    
    #raise Exception

    # drop the first 2 rows
    dfHL = dfHL.ix[2:]

    df[['VolumeAvgPast1', 'VolumeAvgPast2']] = df[['VolumeAvgPast1', 'VolumeAvgPast2']].ffill()
    df['VolumeAvg'] = df['VolumeAvg'].fillna(300e4)

    # indicator, enter
    for ix, row in dfHL.iterrows():
        if type(row['dtConfirm']) is not pd.tslib.Timestamp:
            continue
        if int(row['indicator']) == 1:
            df.ix[row['dtConfirm'], 'indicator'] = 1
            df.ix[row['dtConfirm'], 'PrevLow'] = row['close']
        elif int(row['indicator']) == 2:
            df.ix[row['dtConfirm'], 'indicator'] = -1
            df.ix[row['dtConfirm'], 'PrevHigh'] = row['close']
    df[['PrevHigh', 'PrevLow']] = df[['PrevHigh', 'PrevLow']].ffill()

    # indicator, exit
    sIndicator = df['indicator'].ffill().fillna(0)
    df.ix[(sIndicator==1) & (df['Close'] < (1+p*0.5)*df['PrevLow']) & (df['VolumeAvg'] < df[strPastVolume]*ratioAOGE), 'indicator'] = 0
    df.ix[(sIndicator==-1) & (df['Close'] > (1-p*0.5)*df['PrevHigh']) & (df['VolumeAvg'] < df[strPastVolume]*ratioAOGE), 'indicator'] = 0

    ret = df['indicator'].copy()
    del df, dfHL
    gc.collect()
    
    return ret

def generateIndicatorTSCReboundASL(dictDataSpec):
    """
    core idea: future price is dragged by the stock price
    difference with TSCReboundA: stoploss. 
    TSCReboundA and TSCRebound: using moving average
    TSCReboundASL: considering carry rate changing. 
    ixLongClose = sTS[(sIndicator==1)&((sTS < sTS.quantile(1-QuantileTS*3))&(sClose < sClosePrevMax * ratioReboundDn))].index

    carry rate: we define the carry rate as the annualized return of holding future contract is the stock price does not change and future contract converges to the stock price. 
    when to enter long position: 
    * the carry rate is higher than a given threshold (RetTS)
    * the carry rate is shrinking
    * the shrinking is caused by the rising of future contract. sClosePrevMin is the min value of past NDayHist
    code: ixLong = sTS[(sTS > RetTSUpper)&(sTS < sTSPrevMax * ratioExtreme)&(sClose > sClosePrevMin * ratioReboundUp)].index

    for comparison, the code from TSCRebound is listed below:
    code: ixLong = sTS[(sTS > RetTS)&(sTS < sTSPrevMax * ratioExtreme)&(sClose > sClosePrevMin * ratioReboundUp)].index

    when to close long position:
    * the future price is dn crossing the MA(Close, NDayHistSL)
    code: df.ix[(sDiffLowerSL<0)&(sDiffLowerPrevSL>=0)&(sIndicator==1), 'indicator'] = 0

    Note:
    ----------
    the sTS is calculated in Config/TS/UtilsTS.py
    code snippet:
    # determine the trend TS2
    strMethodTrend = 'TS2'
    strMethodTrendIndicator = 'indicator' + strMethodTrend
    dfTS[strMethodTrendIndicator] = np.nan
    listDict = []
    for dt in dfTS.index.get_level_values('TradingDay').unique():
        print dt
        series = dfTS.ix[dt][strMethodTrend]
        listSecuCodeLong = series[series >= max(0.1, series.quantile(0.8))].index
        listSecuCodeShort = series[series <= min(-0.1, series.quantile(0.2))].index
        for SecuCode in listSecuCodeLong:
            listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': 1.0})
        for SecuCode in listSecuCodeShort:
            listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': -1.0})
    
    dfTrend = pd.DataFrame(listDict)
    dfTrend = dfTrend.set_index('TradingDay')
    dfTrend = dfTrend.sort_index()
    dfTrend.to_pickle('dfTrend%s.pickle'%strMethodTrend)
    
    Parameters:
    ----------
    * dictDataSpec: df for data, and many parameters as defined in Common/ParamRange.py
    * NDayHist: used for enter position
    * NDayHistSL: used for close position
    * QuantileTS: to determine the threshold of RetTS, actually it is using future data. and for each product, there is a individual threshold
                                            
    Returns:
    ----------
    * ret: pandas.Series, indicator, including -1 for sell, 1 for buy, and 0 for closing
    """
    
    # param
    ratioExtreme = dictDataSpec['ratioExtreme']
    NRebound = dictDataSpec['NRebound']
    QuantileTS = dictDataSpec['QuantileTS']
    NDayHist = dictDataSpec['NDayHist']

    strPrice = 'Close'
    df = dictDataSpec['df'].copy()
    dfTS = dictDataSpec['dfTS']
    sTS = dfTS.xs(dictDataSpec['Secu'], level='SecuCode')[dictDataSpec['strMethodTrend']]
    RetTSUpper = sTS.quantile(1-QuantileTS)
    RetTSLower = sTS.quantile(QuantileTS)

    df['indicator'] = np.nan
    sClose = df[strPrice]
    sPCT = sClose.pct_change()
    std = sPCT.std()

    sTSPrevMax = sTS.rolling(NDayHist).max()
    sTSPrevMin = sTS.rolling(NDayHist).min()
    sClosePrevMax = sClose.rolling(NDayHist).max()
    sClosePrevMin = sClose.rolling(NDayHist).min()

    ratioReboundUp = 1 + NRebound * std
    ratioReboundDn = 1 - NRebound * std 
    ixLong = sTS[(sTS > RetTSUpper)&(sTS < sTSPrevMax * ratioExtreme)&(sClose > sClosePrevMin * ratioReboundUp)].index
    ixShort = sTS[(sTS < RetTSLower)&(sTS > sTSPrevMin * ratioExtreme)&(sClose < sClosePrevMax * ratioReboundDn)].index

    df.ix[ixLong, 'indicator'] = 1
    df.ix[ixShort, 'indicator'] = -1
    del ixLong, ixShort

    sIndicator = df['indicator'].ffill()
    #ratioReboundUp = 1 + NRebound/2. * std
    #ratioReboundDn = 1 - NRebound/2. * std 
    #ixLongClose = sTS[(sIndicator==1)&((sTS < sTS.quantile(1-QuantileTS*2))|(sClose < sClosePrevMax * ratioReboundDn))].index
    #ixShortClose = sTS[(sIndicator==-1)&((sTS > sTS.quantile(QuantileTS*2))|(sClose > sClosePrevMin * ratioReboundUp))].index
    ixLongClose = sTS[(sIndicator==1)&((sTS < sTS.quantile(1-QuantileTS*3))&(sClose < sClosePrevMax * ratioReboundDn))].index
    ixShortClose = sTS[(sIndicator==-1)&((sTS > sTS.quantile(QuantileTS*3))&(sClose > sClosePrevMin * ratioReboundUp))].index
    df.ix[ixLongClose, 'indicator'] = 0 
    df.ix[ixShortClose, 'indicator'] = 0
    
    del sIndicator, ixLongClose, ixShortClose
    
    ret = df['indicator'].copy()

    del df, sClose, sTS
    gc.collect()
    
    return ret

def generateIndicatorTSCReboundA(dictDataSpec):
    """
    core idea: future price is dragged by the stock price
    difference with TSCRebound: there is individual RetTS (to determine the carry rate is large enough) for each product

    carry rate: we define the carry rate as the annualized return of holding future contract is the stock price does not change and future contract converges to the stock price. 
    when to enter long position: 
    * the carry rate is higher than a given threshold (RetTS)
    * the carry rate is shrinking
    * the shrinking is caused by the rising of future contract. sClosePrevMin is the min value of past NDayHist
    code: ixLong = sTS[(sTS > RetTSUpper)&(sTS < sTSPrevMax * ratioExtreme)&(sClose > sClosePrevMin * ratioReboundUp)].index

    for comparison, the code from TSCRebound is listed below:
    code: ixLong = sTS[(sTS > RetTS)&(sTS < sTSPrevMax * ratioExtreme)&(sClose > sClosePrevMin * ratioReboundUp)].index

    when to close long position:
    * the future price is dn crossing the MA(Close, NDayHistSL)
    code: df.ix[(sDiffLowerSL<0)&(sDiffLowerPrevSL>=0)&(sIndicator==1), 'indicator'] = 0

    Note:
    ----------
    the sTS is calculated in Config/TS/UtilsTS.py
    code snippet:
    # determine the trend TS2
    strMethodTrend = 'TS2'
    strMethodTrendIndicator = 'indicator' + strMethodTrend
    dfTS[strMethodTrendIndicator] = np.nan
    listDict = []
    for dt in dfTS.index.get_level_values('TradingDay').unique():
        print dt
        series = dfTS.ix[dt][strMethodTrend]
        listSecuCodeLong = series[series >= max(0.1, series.quantile(0.8))].index
        listSecuCodeShort = series[series <= min(-0.1, series.quantile(0.2))].index
        for SecuCode in listSecuCodeLong:
            listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': 1.0})
        for SecuCode in listSecuCodeShort:
            listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': -1.0})
    
    dfTrend = pd.DataFrame(listDict)
    dfTrend = dfTrend.set_index('TradingDay')
    dfTrend = dfTrend.sort_index()
    dfTrend.to_pickle('dfTrend%s.pickle'%strMethodTrend)
    
    Parameters:
    ----------
    * dictDataSpec: df for data, and many parameters as defined in Common/ParamRange.py
    * NDayHist: used for enter position
    * NDayHistSL: used for close position
    * QuantileTS: to determine the threshold of RetTS, actually it is using future data. and for each product, there is a individual threshold
                                            
    Returns:
    ----------
    * ret: pandas.Series, indicator, including -1 for sell, 1 for buy, and 0 for closing
    """
    # param
    ratioExtreme = dictDataSpec['ratioExtreme']
    NRebound = dictDataSpec['NRebound']
    QuantileTS = dictDataSpec['QuantileTS']
    NDayHist = dictDataSpec['NDayHist']
    NDayHistSL = dictDataSpec['NDayHistSL']

    strPrice = 'Close'
    df = dictDataSpec['df'].copy()
    dfTS = dictDataSpec['dfTS']
    sTS = dfTS.xs(dictDataSpec['Secu'], level='SecuCode')[dictDataSpec['strMethodTrend']]
    RetTSUpper = sTS.quantile(1-QuantileTS)
    RetTSLower = sTS.quantile(QuantileTS)

    df['indicator'] = np.nan
    sClose = df[strPrice]
    sPCT = sClose.pct_change()
    std = sPCT.std()

    sTSPrevMax = sTS.rolling(NDayHist).max()
    sTSPrevMin = sTS.rolling(NDayHist).min()
    sClosePrevMax = sClose.rolling(NDayHist).max()
    sClosePrevMin = sClose.rolling(NDayHist).min()

    ratioReboundUp = 1 + NRebound * std
    ratioReboundDn = 1 - NRebound * std 
    ixLong = sTS[(sTS > RetTSUpper)&(sTS < sTSPrevMax * ratioExtreme)&(sClose > sClosePrevMin * ratioReboundUp)].index
    ixShort = sTS[(sTS < RetTSLower)&(sTS > sTSPrevMin * ratioExtreme)&(sClose < sClosePrevMax * ratioReboundDn)].index

    df.ix[ixLong, 'indicator'] = 1
    df.ix[ixShort, 'indicator'] = -1
    del sTS, ixLong, ixShort
    
    sNDayHighSL = sClose.rolling(NDayHistSL).max().shift(1)
    sNDayLowSL = sClose.rolling(NDayHistSL).min().shift(1)
    sDiffUpperSL = sClose - sNDayHighSL
    sDiffUpperPrevSL = sDiffUpperSL.shift(1)
    sDiffLowerSL = sClose - sNDayLowSL
    sDiffLowerPrevSL = sDiffLowerSL.shift(1)

    sIndicator = df['indicator'].ffill()
    df.ix[(sDiffUpperSL>0)&(sDiffUpperPrevSL<=0)&(sIndicator==-1), 'indicator'] = 0
    df.ix[(sDiffLowerSL<0)&(sDiffLowerPrevSL>=0)&(sIndicator==1), 'indicator'] = 0

    del sNDayHighSL, sNDayLowSL, sDiffUpperSL, sDiffUpperPrevSL, sDiffLowerSL, sDiffLowerPrevSL

    ret = df['indicator'].copy()

    del df, sClose
    gc.collect()
    
    return ret

def generateIndicatorTSCRebound(dictDataSpec):
    """
    core idea: future price is dragged by the stock price

    carry rate: we define the carry rate as the annualized return of holding future contract is the stock price does not change and future contract converges to the stock price. 
    when to enter long position: 
    * the carry rate is higher than a given threshold (RetTS)
    * the carry rate is shrinking
    * the shrinking is caused by the rising of future contract. sClosePrevMin is the min value of past NDayHist
    code: ixLong = sTS[(sTS > RetTS)&(sTS < sTSPrevMax * ratioExtreme)&(sClose > sClosePrevMin * ratioReboundUp)].index

    when to close long position:
    * the future price is dn crossing the MA(Close, NDayHistSL)
    code: df.ix[(sDiffLowerSL<0)&(sDiffLowerPrevSL>=0)&(sIndicator==1), 'indicator'] = 0

    Note:
    ----------
    the sTS is calculated in Config/TS/UtilsTS.py
    code snippet:
    # determine the trend TS2
    strMethodTrend = 'TS2'
    strMethodTrendIndicator = 'indicator' + strMethodTrend
    dfTS[strMethodTrendIndicator] = np.nan
    listDict = []
    for dt in dfTS.index.get_level_values('TradingDay').unique():
        print dt
        series = dfTS.ix[dt][strMethodTrend]
        listSecuCodeLong = series[series >= max(0.1, series.quantile(0.8))].index
        listSecuCodeShort = series[series <= min(-0.1, series.quantile(0.2))].index
        for SecuCode in listSecuCodeLong:
            listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': 1.0})
        for SecuCode in listSecuCodeShort:
            listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': -1.0})
    
    dfTrend = pd.DataFrame(listDict)
    dfTrend = dfTrend.set_index('TradingDay')
    dfTrend = dfTrend.sort_index()
    dfTrend.to_pickle('dfTrend%s.pickle'%strMethodTrend)
    
    Parameters:
    ----------
    * dictDataSpec: df for data, and many parameters as defined in Common/ParamRange.py
    * NDayHist: used for enter position
    * NDayHistSL: used for close position
                                            
    Returns:
    ----------
    * ret: pandas.Series, indicator, including -1 for sell, 1 for buy, and 0 for closing
    """
    # param
    ratioExtreme = dictDataSpec['ratioExtreme']
    ratioRebound = dictDataSpec['ratioRebound']
    strPrice = 'Close'

    df = dictDataSpec['df'].copy()
    dfTS = dictDataSpec['dfTS']
    sTS = dfTS.xs(dictDataSpec['Secu'], level='SecuCode')[dictDataSpec['strMethodTrend']]

    df['indicator'] = np.nan
    sClose = df[strPrice]

    RetTS = dictDataSpec['RetTS']
    NDayHist = dictDataSpec['NDayHist']
    NDayHistSL = dictDataSpec['NDayHistSL']

    #ixLong = sTS[(sTS > RetTS)&(sTS < sTS.shift(NDayHist) * 0.8)&(sClose > sClose.shift(NDayHist) * 1.05)].index
    #ixShort = sTS[(sTS < -RetTS)&(sTS > sTS.shift(NDayHist) * 0.8)&(sClose < sClose.shift(NDayHist) * 0.95)].index

    sTSPrevMax = sTS.rolling(NDayHist).max()
    sTSPrevMin = sTS.rolling(NDayHist).min()
    sClosePrevMax = sClose.rolling(NDayHist).max()
    sClosePrevMin = sClose.rolling(NDayHist).min()

    ratioReboundUp = 1 + ratioRebound
    ratioReboundDn = 1 - ratioRebound
    ixLong = sTS[(sTS > RetTS)&(sTS < sTSPrevMax * ratioExtreme)&(sClose > sClosePrevMin * ratioReboundUp)].index
    ixShort = sTS[(sTS < -RetTS)&(sTS > sTSPrevMin * ratioExtreme)&(sClose < sClosePrevMax * ratioReboundDn)].index

    df.ix[ixLong, 'indicator'] = 1
    df.ix[ixShort, 'indicator'] = -1
    del sTS, ixLong, ixShort
    
    sNDayHighSL = sClose.rolling(NDayHistSL).max().shift(1)
    sNDayLowSL = sClose.rolling(NDayHistSL).min().shift(1)
    sDiffUpperSL = sClose - sNDayHighSL
    sDiffUpperPrevSL = sDiffUpperSL.shift(1)
    sDiffLowerSL = sClose - sNDayLowSL
    sDiffLowerPrevSL = sDiffLowerSL.shift(1)

    sIndicator = df['indicator'].ffill()
    df.ix[(sDiffUpperSL>0)&(sDiffUpperPrevSL<=0)&(sIndicator==-1), 'indicator'] = 0
    df.ix[(sDiffLowerSL<0)&(sDiffLowerPrevSL>=0)&(sIndicator==1), 'indicator'] = 0

    del sNDayHighSL, sNDayLowSL, sDiffUpperSL, sDiffUpperPrevSL, sDiffLowerSL, sDiffLowerPrevSL

    ret = df['indicator'].copy()
    del df, sClose
    gc.collect()
    
    return ret

