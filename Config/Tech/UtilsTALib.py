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
import talib

import ThaiExpress.Common.Utils as Utils
reload(Utils)

def generateIndicator(dictDataSpec):
    if dictDataSpec['TAModelName'].find('CDLMORNINGSTAR') >= 0:
        sIndicator = generateIndicatorCDLMORNINGSTAR(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('CDL3LINESTRIKE') >= 0:
        sIndicator = generateIndicatorCDL3LINESTRIKE(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('CDL3BLACKCROWS') >= 0:
        sIndicator = generateIndicatorCDL3BLACKCROWS(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('CDL3INSIDE') >= 0:
        sIndicator = generateIndicatorCDL3INSIDE(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('CDLABANDONEDBABY') >= 0:
        sIndicator = generateIndicatorCDLABANDONEDBABY(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('CDLADVANCEBLOCK') >= 0:
        sIndicator = generateIndicatorCDLADVANCEBLOCK(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('CDL') >= 0:
        sIndicator = generateIndicatorCDL(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('ADXR') >= 0:
        sIndicator = generateIndicatorADXR(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('ADX') >= 0:
        sIndicator = generateIndicatorADX(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('APO') >= 0:
        sIndicator = generateIndicatorAPO(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('BOP') >= 0:
        sIndicator = generateIndicatorBOP(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('V') >= 0:
        sIndicator = generateIndicatorV(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('MARUBOZU') >= 0:
        sIndicator = generateIndicatorMARUBOZU(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('GAP') >= 0:
        sIndicator = generateIndicatorGAP(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('DOJI') >= 0:
        sIndicator = generateIndicatorDOJI(dictDataSpec)
    elif dictDataSpec['TAModelName'].find('BELTHOLD') >= 0:
        sIndicator = generateIndicatorBELTHOLD(dictDataSpec)

    sIndicator = sIndicator.replace(0, np.nan).replace(100, 1).replace(-100, -1)
    return sIndicator

def generateIndicatorCDLMORNINGSTAR(dictDataSpec):
    df = dictDataSpec['df']
    p = dictDataSpec['p']
    sIndicatorL = pd.Series(talib.CDLMORNINGDOJISTAR(df.Open.values, df.High.values, df.Low.values, df.Close.values, p), index=df.index)
    sIndicatorS = pd.Series(talib.CDLEVENINGDOJISTAR(df.Open.values, df.High.values, df.Low.values, df.Close.values, p), index=df.index)
    sIndicator = df['Close'].apply(lambda x: np.nan)
    sIndicator.ix[sIndicatorL[sIndicatorL!=0].index] = 1
    sIndicator.ix[sIndicatorS[sIndicatorS!=0].index] = -1
    return sIndicator

def generateIndicatorCDL3BLACKCROWS(dictDataSpec):
    df = dictDataSpec['df']
    sIndicatorS = pd.Series(talib.CDL3BLACKCROWS(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    sIndicatorL = pd.Series(talib.CDL3WHITESOLDIERS(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    sIndicator = df['Close'].apply(lambda x: np.nan)
    sIndicator.ix[sIndicatorL[sIndicatorL!=0].index] = 1
    sIndicator.ix[sIndicatorS[sIndicatorS!=0].index] = -1
    return sIndicator

def generateIndicatorCDL3LINESTRIKE(dictDataSpec):
    df = dictDataSpec['df']
    sIndicator = pd.Series(talib.CDL3LINESTRIKE(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    return sIndicator

def generateIndicatorCDL3INSIDE(dictDataSpec):
    df = dictDataSpec['df']
    sIndicator = pd.Series(talib.CDL3OUTSIDE(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    return sIndicator

def generateIndicatorCDLABANDONEDBABY(dictDataSpec):
    df = dictDataSpec['df']
    p = dictDataSpec['p']
    sIndicator = pd.Series(talib.CDLABANDONEDBABY(df.Open.values, df.High.values, df.Low.values, df.Close.values, p), index=df.index)
    return sIndicator

def generateIndicatorCDL(dictDataSpec):
    df = dictDataSpec['df']
    p = dictDataSpec['p']
    sIndicator = pd.Series(talib.CDLBELTHOLD(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLLONGLINE(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)

    #sIndicator = pd.Series(talib.CDLADVANCEBLOCK(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDL3STARSINSOUTH(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLBREAKAWAY(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLBREAKAWAY(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLCLOSINGMARUBOZU(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLCONCEALBABYSWALL(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLCOUNTERATTACK(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLDARKCLOUDCOVER(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLDOJI(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLDOJISTAR(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLDRAGONFLYDOJI(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLENGULFING(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLGRAVESTONEDOJI(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLHAMMER(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLHANGINGMAN(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLHARAMI(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLHIGHWAVE(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLHIKKAKE(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index).replace(200, 1).replace(-200, -1)
    #sIndicator = pd.Series(talib.CDLHIKKAKEMOD(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index).replace(200, 1).replace(-200, -1)
    #sIndicator = pd.Series(talib.CDLHOMINGPIGEON(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLIDENTICAL3CROWS(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLINNECK(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLINVERTEDHAMMER(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLKICKING(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLKICKINGBYLENGTH(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLLADDERBOTTOM(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLLONGLEGGEDDOJI(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLMARUBOZU(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLMATCHINGLOW(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLMATHOLD(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLONNECK(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLPIERCING(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLRICKSHAWMAN(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLRISEFALL3METHODS(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLSEPARATINGLINES(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLSHOOTINGSTAR(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLSHORTLINE(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLSPINNINGTOP(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLSTALLEDPATTERN(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLTAKURI(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLTHRUSTING(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLTRISTAR(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLUPSIDEGAP2CROWS(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    #sIndicator = pd.Series(talib.CDLXSIDEGAP3METHODS(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    return sIndicator

def generateIndicatorADX(dictDataSpec):
    """
    https://www.investopedia.com/articles/technical/02/041002.asp
    """
    df = dictDataSpec['df']
    sReal = pd.Series(talib.ADX(df.High.values, df.Low.values, df.Close.values, 14), index=df.index)
    sIndicator = sReal.apply(lambda x: np.nan)

    ADXThresholdUpper = 30
    ADXThresholdLower = 20
    NDayHist = 60
    NDayHistSL = 20
    sIndicator[(sReal > ADXThresholdUpper)&(sReal.shift(1) < ADXThresholdUpper)&(df['Close'] / df['Close'].rolling(NDayHist).min() > 1.1)] = 1
    sIndicator[(sReal < ADXThresholdLower)&(sReal.shift(1) > ADXThresholdLower)] = 0
    sIndicator[(df['Close']<df['Close'].shift(1).rolling(NDayHistSL).quantile(0.1))] = -1
    return sIndicator

def generateIndicatorADXR(dictDataSpec):
    """
    https://www.investopedia.com/articles/technical/02/041002.asp
    """
    df = dictDataSpec['df']
    sReal = pd.Series(talib.ADXR(df.High.values, df.Low.values, df.Close.values, 14), index=df.index)
    sIndicator = sReal.apply(lambda x: np.nan)
    print sReal.describe()

    ADXThresholdUpper = 25
    ADXThresholdLower = 32
    NDayHist = 60
    NDayHistSL = 20
    sIndicator[(sReal > ADXThresholdUpper)&(sReal.shift(1) < ADXThresholdUpper)&(df['Close'] / df['Close'].rolling(NDayHist).min() > 1.1)] = 1
    sIndicator[(sReal < ADXThresholdLower)&(sReal.shift(1) > ADXThresholdLower)] = 0
    sIndicator[(df['Close']<df['Close'].shift(1).rolling(NDayHistSL).quantile(0.1))] = -1
    return sIndicator

def generateIndicatorAPO(dictDataSpec):
    """
    https://www.investopedia.com/articles/technical/02/041002.asp
    """
    df = dictDataSpec['df']
    sReal = pd.Series(talib.APO(df.Close.values, 12, 26), index=df.index)
    sIndicator = sReal.apply(lambda x: np.nan)
    print sReal.describe()

    ADXThresholdUpper = 50
    ADXThresholdLower = 50
    NDayHist = 60
    NDayHistSL = 20
    sIndicator[(sReal > ADXThresholdUpper)&(sReal.shift(1) < ADXThresholdUpper)&(df['Close'] / df['Close'].rolling(NDayHist).min() > 1.1)] = 1
    sIndicator[(sReal < ADXThresholdLower)&(sReal.shift(1) > ADXThresholdLower)] = 0
    sIndicator[(df['Close']<df['Close'].shift(1).rolling(NDayHistSL).quantile(0.1))] = -1
    return sIndicator

def generateIndicatorBOP(dictDataSpec):
    """
    https://www.investopedia.com/articles/technical/02/041002.asp
    """
    df = dictDataSpec['df']
    sReal = pd.Series(talib.BOP(df.Open.values, df.High.values, df.Low.values, df.Close.values), index=df.index)
    sIndicator = sReal.apply(lambda x: np.nan)
    print sReal.describe()

    ThresholdUpper = 0.8
    ThresholdLower = -0.8
    sIndicator[(sReal > ThresholdUpper)&(sReal.shift(1) < ThresholdUpper)] = 1
    sIndicator[(sReal < ThresholdLower)&(sReal.shift(1) > ThresholdLower)] = -1

    return sIndicator

def generateIndicatorMARUBOZU_GOOD(dictDataSpec):
    df = dictDataSpec['df']
    sIndicator = df['Close'].apply(lambda x: np.nan)

    NDayHist = 240
    df['PCTHLABS'] = talib.ATR(df.High.values, df.Low.values, df.Close.values, NDayHist)
    df['PCTHLABS'] = df['PCTHLABS'] / df['Close'].rolling(NDayHist).mean() * 0.4
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

def generateIndicatorMARUBOZU(dictDataSpec):
    df = dictDataSpec['df']
    sIndicator = df['Close'].apply(lambda x: np.nan)

    NDayHist = 240
    df['MA'] = df['Close'].rolling(20).mean()
    df['LengthBody'] = df.apply(lambda row: abs(row['Close']/row['Open']-1), axis=1)
    df['PCTHLABS'] = talib.ATR(df.High.values, df.Low.values, df.Close.values, NDayHist)
    df['PCTHLABS'] = df['PCTHLABS'] / df['Close'].rolling(NDayHist).mean() * 0.2
    df['LL'] = df.apply(lambda row: abs(row['High'] / row['Low'] - 1) > row['PCTHLABS'], axis=1)
    df['Bullish'] = df.apply(lambda row: row['Close'] > row['Open'], axis=1)
    df['LLBullish'] = df.apply(lambda row: row['LL'] & row['Bullish'], axis=1)
    df['LLBearish'] = df.apply(lambda row: row['LL'] & (not row['Bullish']), axis=1)
    df['LongMARUBOZU'] = df.apply(lambda row: abs(row['Open']/row['Low']-1) < min(0.001, row['PCTHLABS'] * 0.2), axis=1)
    df['ShortMARUBOZU'] = df.apply(lambda row: abs(row['Open']/row['High']-1) < min(0.001, row['PCTHLABS'] * 0.2), axis=1)

    #df['LongMARUBOZU'] = df.apply(lambda row: abs(row['Close']/row['High']-1) < min(0.001, row['PCTHLABS'] * 0.2), axis=1)
    #df['ShortMARUBOZU'] = df.apply(lambda row: abs(row['Close']/row['Low']-1) < min(0.001, row['PCTHLABS'] * 0.2), axis=1)
    #df['LongMARUBOZU'] = df.apply(lambda row: (abs(row['Open']/row['Low']-1) < min(0.001, row['PCTHLABS'] * 0.2)) & (abs(row['Close']/row['High']-1) < row['PCTHLABS'] * 0.9), axis=1)
    #df['ShortMARUBOZU'] = df.apply(lambda row: (abs(row['Open']/row['High']-1) < min(0.001, row['PCTHLABS'] * 0.2)) & (abs(row['Close']/row['Low']-1) < row['PCTHLABS'] * 0.9), axis=1)
    #df['LongMARUBOZU'] = df.apply(lambda row: abs(row['Low']/row['Open']-1) < 0.1 * row['LengthBody'], axis=1)
    #df['ShortMARUBOZU'] = df.apply(lambda row: abs(row['High']/row['Open']-1) < 0.1 * row['LengthBody'], axis=1)
    #df['LongMARUBOZU'] = df.apply(lambda row: abs(row['Open']/row['Low']-1) < min(0.001, max(row['LengthBody'], row['PCTHLABS']) * 0.2), axis=1)
    #df['ShortMARUBOZU'] = df.apply(lambda row: abs(row['Open']/row['High']-1) < min(0.001, max(row['LengthBody'], row['PCTHLABS']) * 0.2), axis=1)

    ixLong = df['LLBullish'] & df['LongMARUBOZU']
    ixShort = df['LLBearish'] & df['ShortMARUBOZU']

    #ixLong = ixLong & (df['Close']>df['MA']) & (df['Close'].shift(1) < df['MA'].shift(1))
    #ixShort = ixShort & (df['Close']<df['MA']) & (df['Close'].shift(1) > df['MA'].shift(1))
    
    df.ix[ixLong, 'indicator'] = 1
    df.ix[ixShort, 'indicator'] = -1
    
    #Utils.plotCandleStick(df)
    #raise Exception

    sIndicator = df['indicator']
    return sIndicator

def generateIndicatorDOJI(dictDataSpec):
    df = dictDataSpec['df']
    sIndicator = df['Close'].apply(lambda x: np.nan)

    NDayHist = 240
    PCTDOJI = 0.2
    NShadow = 2.
    TOLERANCE = 0.0000
    PCTShadow = 0.008

    df['PrevClose'] = df['Close'].shift(1)
    df['Bullish'] = df.apply(lambda row: row['Close'] > (1-TOLERANCE) * row['PrevClose'], axis=1)
    df['Bearish'] = df.apply(lambda row: row['Close'] < (1+TOLERANCE) * row['PrevClose'], axis=1)
    
    df['Mean'] = df[['Open', 'Close']].mean(1)
    ixDOJI = abs(df['Close'] / df['Open'] - 1) < abs(df['High']/df['Low'] - 1) * PCTDOJI
    ixDOJILong = ixDOJI & (df['Mean'] / df['Low'] - 1 > PCTShadow)
    ixDOJIShort = ixDOJI & (df['Mean'] / df['High'] - 1 < -PCTShadow)
    df.ix[ixDOJILong, 'DOJILong'] = True
    df.ix[ixDOJIShort, 'DOJIShort'] = True
    
    ixLong = df['DOJILong'] & df['Bearish'].shift(1) & df['Bullish'].shift(-1)
    ixShort = df['DOJIShort'] & df['Bullish'].shift(1) & df['Bearish'].shift(-1)
    #df['HighIsMax'] = (df['High'] > df['High'].shift(1)) & (df['High'] > df['High'].shift(-1))
    #df['LowIsMax'] = (df['Low'] > df['Low'].shift(1)) & (df['Low'] > df['Low'].shift(-1))
    #df['HighIsMin'] = (df['High'] < df['High'].shift(1)) & (df['High'] < df['High'].shift(-1))
    #df['LowIsMin'] = (df['Low'] < df['Low'].shift(1)) & (df['Low'] < df['Low'].shift(-1))
    #ixLong = ixLong & df['HighIsMin']
    #ixShort = ixShort & df['LowIsMax'] 
    #ixLong = ixLong & df['LowIsMin']
    #ixShort = ixShort & df['HighIsMax']
    
    df.ix[ixLong, 'indicator'] = 1
    df.ix[ixShort, 'indicator'] = -1

    df['indicator'] = df['indicator'].shift(1)

    '''
    sMAMax = df['Close'].shift(1).rolling(20).mean()
    sMAMin = df['Close'].shift(1).rolling(20).mean()
    sUpCross = df['Close'] - sMAMax
    sDnCross = df['Close'] - sMAMin
    df.ix[df['indicator'].ffill()==-1 & (sUpCross > 0) & (sUpCross.shift(1)<0)] = 0
    df.ix[df['indicator'].ffill()==1 & (sDnCross < 0) & (sDnCross.shift(1)>0)] = 0
    '''
    #raise Exception
    
    sIndicator = df['indicator']
    return sIndicator

def generateIndicatorGAP(dictDataSpec):
    df = dictDataSpec['df']

    df['indicator'] = np.nan
    df.ix[(df['Open'] > df['High'].shift(1)) & (df['Close'] > df['Open']) & (df['Close'] > df['Close'].shift(1)), 'indicator'] = 1
    df.ix[(df['Open'] < df['Low'].shift(1)) & (df['Close'] < df['Open']) &(df['Close'] < df['Close'].shift(1)), 'indicator'] = -1
    
    sIndicator = df['indicator']
    return sIndicator

