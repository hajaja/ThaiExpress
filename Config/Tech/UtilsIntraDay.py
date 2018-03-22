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
    if dictDataSpec['strModelName'] == 'FD':
        seriesIndicator = generateIndicatorFD(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'Feiali':
        seriesIndicator = generateIndicatorFeiali(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'Aberration':
        seriesIndicator = generateIndicatorAberration(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'RBreak':
        seriesIndicator = generateIndicatorRBreak(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'Hans123':
        seriesIndicator = generateIndicatorHans123(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'Dochian':
        seriesIndicator = generateIndicatorDochian(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'Keltner':
        seriesIndicator = generateIndicatorKeltner(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'BBANDS':
        seriesIndicator = generateIndicatorBBANDS(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'DBOII':
        seriesIndicator = generateIndicatorDynamicBreakOutII(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'DualThrust':
        seriesIndicator = generateIndicatorDualThrust(dictDataSpec)

    seriesIndicator = Utils.UtilsUtils.keepOperation(seriesIndicator)
    return seriesIndicator

##################
# intra day strategy
#################
def getDataDaily(dfBar, secondsAdvanceDayEnd=Utils.UtilsUtils.secondsAdvanceDayEnd):
    # rename AdjHigh
    if 'AdjHigh' in dfBar.columns:
        dictRename = {
                'AdjOpen': 'Open',
                'AdjHigh': 'High',
                'AdjLow': 'Low',
                'AdjClose': 'Close',
                'volumeSum': 'Volume',
                }
        dfBar = dfBar.rename(columns=dictRename)
    # insert daily data 
    dfBar['dtForDaily'] = dfBar.index + datetime.timedelta(0, secondsAdvanceDayEnd)
    dfBar = dfBar.reset_index().set_index('dtForDaily')
    gg = dfBar.groupby(dfBar.index.date)
    seriesDT = gg['dtEnd'].first()
    if 'vwp' in dfBar.columns:
        seriesOpen = gg['vwp'].first()
        seriesHigh = gg['vwp'].max()
        seriesLow = gg['vwp'].min()
        seriesClose = gg['vwp'].last()
    else:
        seriesOpen = gg['Open'].first()
        seriesHigh = gg['High'].max()
        seriesLow = gg['Low'].min()
        seriesClose = gg['Close'].last()
    
    seriesVolume = gg['Volume'].sum()
    listSeries = [seriesDT, seriesOpen, seriesHigh, seriesLow, seriesClose, seriesVolume]
    listName = ['dtEnd', 'Open', 'High', 'Low', 'Close', 'Volume']
    dfDaily = pd.concat(listSeries, axis=1)
    dfDaily.columns = listName
    dfDaily.index.name = 'TradingDay'

    # for weekend
    rowPrev = None
    ixPrev = None
    listRowToDrop = []
    for ix, row in dfDaily.iterrows():
        if rowPrev is None:
            rowPrev = row
            ixPrev = ix
            continue
        if row['dtEnd'].time() in [datetime.time(21, 0), datetime.time(0, 0)]:
            dfDaily.ix[ix, 'Open'] = rowPrev['Open']
            dfDaily.ix[ix, 'High'] = max(row['High'], rowPrev['High'])
            dfDaily.ix[ix, 'Low'] = max(row['Low'], rowPrev['Low'])
            dfDaily.ix[ix, 'Close'] = row['Close']
            dfDaily.ix[ix, 'Volume'] = row['Volume'] + rowPrev['Volume']
            dfDaily.ix[ix, 'dtEnd'] = rowPrev['dtEnd']
            listRowToDrop.append(ixPrev)
        rowPrev = row
        ixPrev = ix
    dfDaily = dfDaily[dfDaily.index.isin(listRowToDrop)==False]

    # shift
    dfDaily = dfDaily.reset_index().set_index('dtEnd')
    dfDaily['PreOpen'] = dfDaily['Open'].shift(1)
    dfDaily['PreHigh'] = dfDaily['High'].shift(1)
    dfDaily['PreLow'] = dfDaily['Low'].shift(1)
    dfDaily['PreClose'] = dfDaily['Close'].shift(1)

    return dfDaily

def generateIndicatorAberration(dictDataSpec):
    import talib
    reload(talib)
    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan

    NPeriod = 240 * 6
    NStd = 4
    
    #NPeriod = 30 * 6
    #NStd = 6
    
    df['Middle'] = talib.MA(df['Close'].values, NPeriod)
    df['Upper'] = df['Middle'] + NStd * df['Close'].rolling(NPeriod).std()
    df['Lower'] = df['Middle'] - NStd * df['Close'].rolling(NPeriod).std()

    sFast = df['Close'].rolling(1).mean()
    df.ix[sFast > df['Upper'], 'indicator'] = 1
    df.ix[sFast < df['Lower'], 'indicator'] = -1

    sDiff = sFast - df['Middle']
    sDiffPrev = sDiff.shift()
    df.ix[(sDiff>0)&(sDiffPrev<=0)&(df['indicator'].ffill()==-1), 'indicator'] = 0
    df.ix[(sDiff<0)&(sDiffPrev>=0)&(df['indicator'].ffill()==1), 'indicator'] = 0
    
    #raise Exception

    return df['indicator'].copy()

def generateIndicatorHans123(dictDataSpec):
    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan

    def funcHans123(dfDay):
        HighFirst30Min = dfDay.ix[dfDay.index.time <= datetime.time(9, 45), 'High'].max()
        LowFirst30Min = dfDay.ix[dfDay.index.time <= datetime.time(9, 45), 'Low'].min()
        dfDay.ix[dfDay['Close'] > HighFirst30Min, 'indicator'] = 1
        dfDay.ix[dfDay['Close'] < LowFirst30Min, 'indicator'] = -1
        return dfDay
    df = df.groupby(df.index.date).apply(funcHans123)
    df = df.reset_index().set_index('dtEnd')

    return df['indicator'].copy()

def generateIndicatorDochian(dictDataSpec):
    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan
    #NPeriod = 240 * 10
    NPeriod = dictDataSpec['NPeriod']

    df['HighRolling'] = df['High'].rolling(NPeriod).max()
    df['LowRolling'] = df['Low'].rolling(NPeriod).min()
    
    df.ix[df['Close'] > df['HighRolling'].shift(1), 'indicator'] = 1
    df.ix[df['Close'] < df['LowRolling'].shift(1), 'indicator'] = -1

    return df['indicator'].copy()

def generateIndicatorKeltner(dictDataSpec):
    import talib
    reload(talib)
    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan

    #NPeriod = 240 * 5
    #width = 5
    NPeriod = dictDataSpec['NPeriod']
    width = dictDataSpec['width']

    df['MA'] = talib.MA(df['Close'].values, NPeriod)
    df['ATR'] = talib.ATR(df['High'].values, df['Low'].values, df['Close'].values, timeperiod=NPeriod)
    df['Upper'] = df['MA'] + width * df['ATR']
    df['Lower'] = df['MA'] - width * df['ATR']

    df.ix[df['Close'] > df['Upper'], 'indicator'] = 1
    df.ix[df['Close'] < df['Lower'], 'indicator'] = -1

    return df['indicator'].copy()

def generateIndicatorBBANDS(dictDataSpec):
    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan
    NPeriod = 240 * 5
    width = 4
    aUpper, aMiddle, aLower = talib.BBANDS(df['Close'].values, timeperiod=NPeriod, nbdevup=width, nbdevdn=width)
    df['Upper'] = aUpper
    df['Middle'] = aMiddle
    df['Lower'] = aLower
    df['Fast'] = df['Close'].rolling(5).mean()

    NShift = 1
    sDiffUpper = df['Fast'] - df['Upper']
    sDiffPrevUpper = sDiffUpper.shift(NShift)
    indexReverseToUpper = (sDiffUpper < 0) & (sDiffPrevUpper >= 0)
    indexBreakUpper = (sDiffUpper > 0) & (sDiffPrevUpper <= 0)
    df.ix[indexBreakUpper, 'indicator'] = 1

    sDiffLower = df['Fast'] - df['Lower']
    sDiffPrevLower = sDiffLower.shift(NShift)
    indexReverseToLower = (sDiffLower > 0) & (sDiffPrevLower <= 0)
    indexBreakLower = (sDiffLower < 0) & (sDiffPrevLower >= 0)
    df.ix[indexBreakLower, 'indicator'] = -1

    sDiffMiddle = df['Fast'] - df['Middle']
    sDiffPrevMiddle = sDiffMiddle.shift(NShift)
    indexUpCrossMiddle = (sDiffMiddle>0)&(sDiffPrevMiddle<0)
    indexDnCrossMiddle = (sDiffMiddle<0)&(sDiffPrevMiddle>0)
    df.ix[indexUpCrossMiddle & df['indicator'].ffill()==-1, 'indicator'] = 0
    df.ix[indexReverseToUpper & df['indicator'].ffill()==1, 'indicator'] = 0

    #raise Exception
    return df['indicator'].copy()

def generateIndicatorDynamicBreakOutII(dictDataSpec):
    import talib
    reload(talib)
    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan
    
    # param
    NPeriodBBANDS = dictDataSpec['NPeriodBBANDS']
    WidthBBANDS = dictDataSpec['WidthBBANDS']
    NPeriodRollingSlow = dictDataSpec['NPeriodRollingSlow']
    NPeriodRollingFast = dictDataSpec['NPeriodRollingFast']
    strCloseAtDayEnd = dictDataSpec['strCloseAtDayEnd']

    # extract daily data
    dfDaily = getDataDaily(df)
    dfDaily['PreClose'] = dfDaily['Close'].shift(1)
    aUpper, aMiddle, aLower = talib.BBANDS(dfDaily['PreClose'].values,
    timeperiod=NPeriodBBANDS, nbdevup=WidthBBANDS, nbdevdn=WidthBBANDS)
    dfDaily['Upper'] = aUpper
    dfDaily['Middle'] = aMiddle
    dfDaily['Lower'] = aLower
    listColDaily = ['Upper', 'Middle', 'Lower', 'PreClose']
    df[listColDaily] = dfDaily[listColDaily]
    df[listColDaily] = df[listColDaily].ffill()

    # rolling bar
    df['HighRolling'] = df['High'].rolling(NPeriodRollingSlow).max().shift(1)
    df['LowRolling'] = df['Low'].rolling(NPeriodRollingSlow).min().shift(1)
    df['MA'] = df['Close'].rolling(NPeriodRollingSlow).mean()

    df.ix[(df['Close']>df['HighRolling'])&(df['PreClose'] > df['Upper']), 'indicator'] = 1
    df.ix[(df['Close']<df['LowRolling'])&(df['PreClose'] < df['Lower']), 'indicator'] = -1
    #df.ix[(df['Close']>df['HighRolling']), 'indicator'] = 1
    #df.ix[(df['Close']<df['LowRolling']), 'indicator'] = -1

    '''
    sDiff = df['Close'].rolling(NPeriodRollingFast).mean() - df['MA']
    sDiffPrev = sDiff.shift(1)
    df.ix[(sDiff > 0) & (sDiffPrev <= 0) & (df['indicator'].ffill()==-1), 'indicator'] = 0
    df.ix[(sDiff < 0) & (sDiffPrev >= 0) & (df['indicator'].ffill()==1), 'indicator'] = 0
    '''
    # close at day end 
    df['indicator'] = Utils.UtilsUtils.keepOperation(df['indicator'])
    df = Utils.UtilsUtils.closePositionAtDayEnd(df, strCloseAtDayEnd)
    
    ret = df['indicator'].copy()
    del df, dfDaily, aUpper, aMiddle, aLower
    gc.collect()

    return ret

def generateIndicatorDualThrust(dictDataSpec):
    """
    use filter to determine the low-frequency trend, replace MA with the low-frequency trend
    long when up crossing upper rail
    sell when down crossing lower rail
    
    Parameters:
    ----------
    * dictDataSpec: df for data, and many parameters as defined in Common/ParamRange.py
    * NDay, K: to use the 1day bar to determine the interval
    * upper: Open + K * interval
                                            
    Returns:
    -------
    * ret: pandas.Series, indicator, including -1 for sell, 1 for buy, and 0 for closing
    """

    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan

    # param
    NDay = dictDataSpec['NDay']
    K1 = dictDataSpec['K']
    K2 = dictDataSpec['K']
    NPeriodRollingFast = dictDataSpec['NPeriodRollingFast']
    strCloseAtDayEnd = dictDataSpec['strCloseAtDayEnd']

    # extract daily data
    dfDaily = getDataDaily(df)
    dfDaily['HH'] = dfDaily['PreHigh'].rolling(NDay).max()
    dfDaily['HC'] = dfDaily['PreClose'].rolling(NDay).max()
    dfDaily['LC'] = dfDaily['PreClose'].rolling(NDay).min()
    dfDaily['LL'] = dfDaily['PreLow'].rolling(NDay).min()
    dfDaily['HH_LC'] = dfDaily['HH'] - dfDaily['LC']
    dfDaily['HC_LL'] = dfDaily['HC'] - dfDaily['LL']
    dfDaily['interval'] = dfDaily[['HH_LC', 'HC_LL']].max(1)
    dfDaily['Upper'] = dfDaily['Open'] + K1 * dfDaily['interval']
    dfDaily['Lower'] = dfDaily['Open'] - K2 * dfDaily['interval']
    
    # add daily to tick
    listDaily = ['Upper', 'Lower', 'PreClose', 'Open']
    for strColumn in listDaily:
        df[strColumn] = np.nan
    df.ix[dfDaily.index, listDaily] = dfDaily[listDaily]
    df[listDaily] = df[listDaily].ffill()

    # indicator for enter
    if 'vwp' in df.columns:
        strPrice = 'vwp'
    else:
        strPrice = 'Close'
    sDiffUpper = df[strPrice].rolling(NPeriodRollingFast).mean() - df['Upper']
    sDiffUpperPrev = sDiffUpper.shift()
    sDiffLower = df[strPrice].rolling(NPeriodRollingFast).mean() - df['Lower']
    sDiffLowerPrev = sDiffLower.shift()
    df.ix[(sDiffUpper>0)&(sDiffUpperPrev<=0), 'indicator'] = 1
    df.ix[(sDiffLower<0)&(sDiffLowerPrev>=0), 'indicator'] = -1

    # trend statistics
    if dictDataSpec.has_key('boolTrend') and dictDataSpec['boolTrend'] is True:
        df = Utils.UtilsUtils.useTrendStatToFilterIndicator(df, dictDataSpec)
    
    # day end
    df['indicator'] = Utils.UtilsUtils.keepOperation(df['indicator'])
    df = Utils.UtilsUtils.closePositionAtDayEnd(df, strCloseAtDayEnd)

    ret = df['indicator'].copy()
    del df, dfDaily, sDiffUpper, sDiffUpperPrev, sDiffLower, sDiffLowerPrev
    gc.collect()
    
    return ret

def generateIndicatorRBreak(dictDataSpec):
    df = dictDataSpec['df'].copy()
    df['indicator'] = np.nan
    # param
    KSetup = dictDataSpec['KSetup']    #0.35
    KBreak = dictDataSpec['KBreak']    #0.25
    KEnter = dictDataSpec['KEnter']    #0.07
    NPeriodRollingFast = dictDataSpec['NPeriodRollingFast']
    strCloseAtDayEnd = dictDataSpec['strCloseAtDayEnd']
    
    # get daily data
    dfDaily = getDataDaily(df)
    listColDaily = ['PreHigh', 'PreLow', 'PreClose', 'TradingDay']
    df[listColDaily] = dfDaily[listColDaily]
    df[listColDaily] = df[listColDaily].ffill()

    # high/low today
    df['HighToday'] = np.nan
    df['LowToday'] = np.nan
    def funcHighLowToday(df):
        df['HighToday'] = df['High'].expanding(min_periods=1).max()
        df['LowToday'] = df['Low'].expanding(min_periods=1).min()
        df = df.drop('TradingDay', axis=1)
        return df
    df = df.groupby('TradingDay').apply(funcHighLowToday)

    # logic of strategy
    df['SSetup'] = df['PreHigh'] + KSetup * (df['PreHigh'] - df['PreClose'])
    df['BSetup'] = df['PreLow'] - KSetup * (df['PreClose'] - df['PreLow'])
    df['SEnter'] = df['SSetup'] - KEnter * (df['SSetup'] - df['BSetup'])
    df['BEnter'] = df['BSetup'] + KEnter * (df['SSetup'] - df['BSetup'])
    df['BBreak'] = df['SSetup'] + KBreak * (df['SSetup'] - df['BSetup'])
    df['SBreak'] = df['BSetup'] - KBreak * (df['SSetup'] - df['BSetup'])

    df['Fast'] = df['Close'].rolling(NPeriodRollingFast).mean()
    df.ix[df['Fast'] > df['BBreak'], 'indicator'] = 1
    df.ix[df['Fast'] < df['SBreak'], 'indicator'] = -1

    df.ix[(df['HighToday'] > df['SSetup'])&(df['Fast'] < df['SEnter']), 'indicator'] = -1
    df.ix[(df['LowToday'] < df['BSetup'])&(df['Fast'] > df['BEnter']), 'indicator'] = 1

    # close position at day end
    df['indicator'] = Utils.UtilsUtils.keepOperation(df['indicator'])
    df = Utils.UtilsUtils.closePositionAtDayEnd(df, strCloseAtDayEnd)

    # keep operation
    df['indicator'] = df['indicator'].ffill().fillna(0)

    return df['indicator'].copy()

def butter_lowpass(lLowerUpper, order, boolLP=False):
    if boolLP:
        b = signal.firwin(order, lLowerUpper[1])
    else:
        if lLowerUpper[0] == 0:
            b = signal.firwin(order, lLowerUpper[1])
        else:
            b = signal.firwin(order, lLowerUpper, pass_zero=False)
    b = b / sum(b)
    a = 1
    return b, a

def generateIndicatorFD(dictDataSpec):
    """
    use filter to determine the low-frequency trend, replace MA with the low-frequency trend
    long when up crossing LF trend
    sell when down crossing LF trend
    
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
    strNameX = 'Value'
    NPeriodRollingFast = dictDataSpec['NPeriodRollingFast']
    fLower = dictDataSpec['FLower']
    fUpper = dictDataSpec['FUpper']
    order = dictDataSpec['Order']
    strCloseAtDayEnd = dictDataSpec['strCloseAtDayEnd']
    
    # indicator for enter
    lLowerUpper = [fLower, fUpper]
    if fLower == 0:
        boolLP = True
    else:
        boolLP = False
    b, a = butter_lowpass(lLowerUpper, order, boolLP)
    df['TrendFD'] = lfilter(b, a, df[strNameX])
    
    sDiffUpper = df[strNameX].rolling(NPeriodRollingFast).mean() - df['TrendFD'] * 1.01
    sDiffUpperPrev = sDiffUpper.shift()
    sDiffLower = df[strNameX].rolling(NPeriodRollingFast).mean() - df['TrendFD'] * 0.99
    sDiffLowerPrev = sDiffLower.shift()
    df.ix[(sDiffUpper>0)&(sDiffUpperPrev<=0), 'indicator'] = 1
    df.ix[(sDiffLower<0)&(sDiffLowerPrev>=0), 'indicator'] = -1

    #df.ix[df.index.time!=datetime.time(10, 0), 'indicator'] = np.nan
    
    # day end
    df['indicator'] = Utils.UtilsUtils.keepOperation(df['indicator'])
    df = Utils.UtilsUtils.closePositionAtDayEnd(df, strCloseAtDayEnd)

    ret = df['indicator'].copy()
    del df, sDiffUpper, sDiffLower
    gc.collect()
    
    return ret

