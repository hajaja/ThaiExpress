import pandas as pd
import datetime
import numpy as np
import pdb
import scipy.io as sio
import os
import random
from scipy.signal import butter, lfilter, freqz
from scipy import signal

import ThaiExpress.Common.Utils as Utils
reload(Utils)
import ThaiExpress.Common.UtilsUtils as UtilsUtils
reload(UtilsUtils)

############
# parameters
############

############################################################
# trend
############################################################
def determineTrendSimple(dictParam):
    df = dictParam['df']
    ret = df.ix[-1, 'Close'].mean() / df.ix[0, 'Close'].mean() - 1
    #print 'Check determineTrendSimple', df.index[0], df.ix[0, 'Close'], df.index[-1], df.ix[-1, 'Close'], ret
    if ret > 0:
        return 1
    else:
        return -1

def determineTrendSMT(dictParam):
    tThreshold = 2
    RThreshold = 0.65

    import statsmodels.formula.api as sm
    df = dictParam['df']
    
    dfOLS = pd.DataFrame(df['Close'])
    dfOLS['i'] = range(0, dfOLS.index.size)
    dfOLS = dfOLS.dropna()
    for NInterval in range(4, 10):
        NPointPerInterval = int(np.ceil(dfOLS.index.size / NInterval))
        dfOLS['nInterval'] = dfOLS['i'] - dfOLS['i'].max() % NPointPerInterval
        dfOLS['nInterval'] = np.ceil(dfOLS['nInterval'] / NPointPerInterval).astype(int)
        sPriceIntervalMean = dfOLS.groupby('nInterval')['Close'].mean()
        sPriceIntervalMean.name = 'priceMeanOfInterval'
        dfPriceI = sPriceIntervalMean.reset_index()
    
        ols = sm.ols(formula='priceMeanOfInterval ~ nInterval', data=dfPriceI).fit(cov_type='HAC',cov_kwds={'maxlags':1})
        t = ols.tvalues['nInterval']
        RSquared = ols.rsquared

        if t > tThreshold and RSquared > RThreshold:
            return 1
        elif t < -tThreshold and RSquared > RThreshold:
            return -1
    
    return 0

def determineTrendTREND_Variant(dictParam):
    import statsmodels.formula.api as sm
    df = dictParam['df']
    
    dfOLS = pd.DataFrame(df['Close'])
    dfOLS['i'] = range(0, dfOLS.index.size)
    dfOLS = dfOLS.dropna()
        
    ols = sm.ols(formula='Close ~ i', data=dfOLS).fit(cov_type='HAC',cov_kwds={'maxlags':1})
    t = ols.tvalues['i']
    
    import scipy
    alphaThreshold = 0.05
    alpha = scipy.stats.t.cdf(t, ols.df_resid)

    if alpha > 1-alphaThreshold:
        return 1
    elif alpha < alphaThreshold:
        return -1
    else:
        return 0

def determineTrendWLS(dictParam):
    NLag = dictParam['NLag']
    import statsmodels.formula.api as sm
    df = dictParam['df']
    
    dfOLS = pd.DataFrame(df['Close'])
    dfOLS['i'] = range(0, dfOLS.index.size)
    dfOLS['weight'] = dfOLS['i']
    dfOLS['weight'] = dfOLS['weight'] - dfOLS['i'].mean()
    dfOLS['weight'] = dfOLS['weight'].apply(lambda x: np.power(abs(x), 2))
    dfOLS['weight'] = dfOLS['weight'] / dfOLS['weight'].sum()
    dfOLS = dfOLS.dropna()
    
    #wls = sm.wls(formula='Close ~ i', data=dfOLS, weights=dfOLS.weight.values).fit()
    wls = sm.wls(formula='Close ~ i', data=dfOLS, weights=dfOLS.weight).fit(cov_type='HAC', cov_kwds={'maxlags':NLag})
    #wls = sm.wls(formula='Close ~ i', data=dfOLS, weights=dfOLS.weight).fit(cov_type='HC0')
    t = wls.tvalues['i']

    tThreshold = 2
    if t > tThreshold:
        return 1
    elif t < -tThreshold:
        return -1
    else:
        return 0

def determineTrendTREND(dictParam):
    import statsmodels.formula.api as sm
    df = dictParam['df']
    
    dfOLS = pd.DataFrame(df['Close'])
    dfOLS['i'] = range(0, dfOLS.index.size)
    dfOLS = dfOLS.dropna()
        
    ols = sm.ols(formula='Close ~ i', data=dfOLS).fit(cov_type='HAC',cov_kwds={'maxlags':1})
    t = ols.tvalues['i']
    
    tThreshold = 2
    if t > tThreshold:
        return 1
    elif t < -tThreshold:
        return -1
    else:
        return 0

def determineTrendEEMD(dictParam):
    df = dictParam['df']
    
    import pyeemd
    imfs = pyeemd.emd(df['Close'])
    residual = imfs[-1, :]
    residual_diff = np.diff(residual)
    
    if residual_diff[-1] > 0:
        return 1
    else:
        return -1
        
    raise Exception

def determineTrend(dictParam):
    strMethod = dictParam['strMethodTrend']
    if strMethod == 'Simple':
        return determineTrendSimple(dictParam)
    elif strMethod == 'SMT':
        return determineTrendSMT(dictParam)
    elif strMethod == 'EEMD':
        return determineTrendEEMD(dictParam)
    elif strMethod == 'TREND':
        return determineTrendTREND(dictParam)
    elif strMethod.startswith('WLS'):
        dictParam['NLag'] = int(strMethod[-1])
        return determineTrendWLS(dictParam)

############################################################
# volatility
############################################################
def calculateVolatilitySimple(dictParam):
    df = dictParam['df']
    ret = df['Close'].pct_change().std()
    ret = ret * np.sqrt(UtilsUtils.NTradingDayPerYear)
    return ret

def calculateVolatilityEWMA(dictParam):
    df = dictParam['df']
    vol = df['Close'].pct_change().std()
    seriesPCT = df['Close'].pct_change() / vol
    D = seriesPCT.size / 2
    D = np.float(D)
    delta = D / (D+1.)
    seriesCoef = pd.Series(range(0, seriesPCT.size))
    seriesCoef = seriesCoef.apply(lambda x: (1-delta)*np.power(delta, x))
    seriesCoef = seriesCoef.iloc[::-1]
    seriesCoef.index = seriesPCT.index
    seriesPCTDiff = seriesPCT - seriesPCT.mean()

    seriesTemp = seriesCoef * seriesPCTDiff * seriesPCTDiff
    ret = np.sqrt(seriesTemp.sum()) 

    if df.index[0] > datetime.datetime(2015,1,1):
        #raise Exception
        pass

    return ret

def calculateVolatilityEWMAN(dictParam):
    df = dictParam['df']
    seriesPCT = df['Close'].pct_change()
    D = seriesPCT.size / dictParam['NDivide']
    D = np.float(D)
    delta = D / (D+1.)
    seriesCoef = pd.Series(range(0, seriesPCT.size))
    seriesCoef = seriesCoef.apply(lambda x: (1-delta)*np.power(delta, x))
    seriesCoef = seriesCoef.iloc[::-1]
    seriesCoef.index = seriesPCT.index
    seriesPCTDiff = seriesPCT - seriesPCT.mean()

    seriesTemp = seriesCoef * seriesPCTDiff * seriesPCTDiff
    ret = np.sqrt(seriesTemp.sum()) 
    
    ret = ret * np.sqrt(UtilsUtils.NTradingDayPerYear)

    if df.index[0] > datetime.datetime(2015,1,1):
        #raise Exception
        pass

    return ret

def calculateVolatilityARCH(dictParam):
    df = dictParam['df']
    seriesPCT = df['Close'].pct_change().dropna()

    p = dictParam['p']
    o = dictParam['o']
    q = dictParam['q']

    import warnings
    warnings.simplefilter('ignore')
        
    from arch import arch_model
    am = arch_model(seriesPCT, vol='Garch', p=p, o=o, q=q)

    #res = am.fit(seriesPCT, vol='Garch', p=1, o=0, q=1, dist='Normal')
    res = am.fit()
    ret = res.conditional_volatility.dropna().mean()
    ret = ret * np.sqrt(UtilsUtils.NTradingDayPerYear)

    return ret

def calculateVolatilityGK(dictParam):
    df = dictParam['df']
    std = 0.5*np.power(df['h'] - df['c'], 2) - (2*np.log10(2)-1) * np.power(df['c'], 2)
    std = np.sqrt(std.sum() / std.index.size)
    ret = std
    ret = ret * np.sqrt(UtilsUtils.NTradingDayPerYear)

    if np.isnan(ret):
        raise Exception

    return ret

def calculateVolatilityYZ(dictParam):
    df = dictParam['df']
    stdOpen = df['o'].std()
    stdClose = df['Close'].pct_change().std()
    stdRS = df['h']*(df['h'] - df['c']) + df['l']*(df['l'] - df['c'])
    stdRS = np.sqrt(stdRS.sum() / stdRS.index.size)

    k = 0.34 / (1.34 + 1)
    ret = np.power(stdOpen,2) + k * np.power(stdClose,2) + (1-k) * np.power(stdRS,2)
    ret = np.sqrt(ret)
    ret = ret * np.sqrt(UtilsUtils.NTradingDayPerYear)

    return ret

def calculateVolatility(dictParam):
    strMethod = dictParam['strMethodVolatility']
    if strMethod == 'Simple':
        ret = calculateVolatilitySimple(dictParam)
    elif strMethod == 'YZ':
        ret = calculateVolatilityYZ(dictParam)
    elif strMethod == 'GK':
        ret = calculateVolatilityGK(dictParam)
    elif strMethod == 'EWMA':
        ret = calculateVolatilityEWMA(dictParam)
    elif strMethod.startswith('EWMAN'):
        NDivide = int(strMethod[-1])
        dictParam['NDivide'] = NDivide
        ret = calculateVolatilityEWMAN(dictParam)
    elif strMethod.startswith('ARCH'):
        dictParam['p'] = int(strMethod[-3])
        dictParam['o'] = int(strMethod[-2])
        dictParam['q'] = int(strMethod[-1])
        ret = calculateVolatilityARCH(dictParam)

    return ret

# aggregate
def funcCheckHK(dictDataSpec, listNDayTrain, listNDayTest, listSecu, listMethodTrend, listMethodVolatility):
    Secu = dictDataSpec['Secu']
    NDayTrain = dictDataSpec['NDayTrain']
    NDayTest = dictDataSpec['NDayTest']
    strMethodTrend = dictDataSpec['strMethodTrend']
    strMethodVolatility = dictDataSpec['strMethodVolatility']
    if listNDayTrain is None or int(NDayTrain) in listNDayTrain:
        if listNDayTest is None or int(NDayTest) in listNDayTest:
            if listSecu is None or Secu in listSecu:
                if listMethodTrend is None or strMethodTrend in listMethodTrend:
                    if listMethodVolatility is None or strMethodVolatility in listMethodVolatility:
                        return True
    
    return False

def funcShowStrategyPort(listDictDataSpec, listNDayTrain, listNDayTest, listSecu, listMethodTrend, listMethodVolatility, seriesDTRebalance):
    # extract the selected data
    listSeriesReturn = []
    listSeriesPosition = []
    for dictDataSpec in listDictDataSpec:
        if funcCheckHK(dictDataSpec, listNDayTrain, listNDayTest, listSecu, listMethodTrend, listMethodVolatility):
            strategy = dictDataSpec['strategy']
            
            if dictDataSpec['strMethodVolatility'] == 'EWMA':
                seriesPosition = (0.4 / np.sqrt(len(listSecu))) / strategy.seriesVolatility.resample('1D', how='last').dropna()
            else:
                seriesPosition = (0.1 / np.sqrt(len(listSecu))) / strategy.seriesVolatility.resample('1D', how='last').dropna()

            seriesPosition = seriesPosition.replace(np.inf, 0).replace(-np.inf, 0)
            seriesPosition.name = dictDataSpec['Secu']
            listSeriesPosition.append(seriesPosition)
            
            seriesReturn = strategy.seriesReturnPCTHoldDaily
            seriesReturn.name = dictDataSpec['Secu']
            listSeriesReturn.append(seriesReturn)

    # calculate the position information, considering the rebalance
    dfPosition = pd.concat(listSeriesPosition, axis=1).ffill()
    seriesDailyPosition = dfPosition.sum(axis=1)
    seriesDailyPosition = seriesDailyPosition[seriesDailyPosition.index > datetime.datetime(2014, 1, 1)]
    seriesDailyPosition.name = 'Position'
    
    # calculate the daily value, considering the rebalance
    dfReturn = pd.concat(listSeriesReturn, axis=1).fillna(0)
    dfReturn.index = dfReturn.index.to_datetime()
    dfReturn.index.name = 'dtEnd'
    dfReturn = dfReturn[dfReturn.index > datetime.datetime(2014, 1, 1)]

    #def funcGetFirstDT(df):
    #    return df.index[0]
    #seriesDTRebalance = dfReturn.groupby(dfReturn.index.strftime('%Y%m%d')).apply(funcGetFirstDT)

    seriesDTRebalance = seriesDTRebalance.reset_index().set_index(0)
    dfReturn['nDTRebalance'] = np.nan
    ix = (seriesDTRebalance.index & dfReturn.index)
    dfReturn.ix[ix, 'nDTRebalance'] = seriesDTRebalance.ix[ix].values[:, 0]
    dfReturn['nDTRebalance'] = dfReturn['nDTRebalance'].ffill().fillna(0)
    def funcDailyValueRebalance(df, dfPosition=dfPosition):
        listColumnSecu = df.columns.tolist()
        listColumnSecu.remove('nDTRebalance')
        sumPosition = dfPosition.ix[df.index[0], listColumnSecu].sum()
        dfValuePerSecu = (df[listColumnSecu] + 1).cumprod()
        dfWeightedValuePerSecu = dfValuePerSecu * dfPosition.ix[df.index[0], listColumnSecu]
        if sumPosition < 1:
            dfWeightedValuePerSecu['cash'] = (1-sumPosition)
        seriesWeightedDailyValue = dfWeightedValuePerSecu.sum(axis=1)
        retFirstDay = seriesWeightedDailyValue[0] / max(sumPosition, 1) - 1
        ret = seriesWeightedDailyValue.pct_change()
        ret[0] = retFirstDay
        if df.index[0] > datetime.datetime(2015, 10, 1):
            #raise Exception
            pass
        ret.name = 'ReturnDaily'
        ret = ret * max(sumPosition, 1)
        return ret
    seriesDailyReturn = dfReturn.groupby(dfReturn['nDTRebalance']).apply(funcDailyValueRebalance)
    seriesDailyReturn = seriesDailyReturn.reset_index().set_index('dtEnd')['ReturnDaily']
    seriesDailyLeveragedReturn = seriesDailyReturn * seriesDailyPosition
    seriesDailyReturn = seriesDailyLeveragedReturn


    # calculate the portfolio performance
    seriesDailyValue = (1 + seriesDailyReturn).cumprod()
    seriesDailyValue.name = 'Cum Return'
    std = seriesDailyReturn.std() * np.sqrt(252)
    ret = seriesDailyValue[-1]
    ret = np.power(ret, 252./seriesDailyValue.size) - 1
    SharpeRatio = (ret - 0.015)/ std
    seriesMaxValue = seriesDailyValue.expanding().max()
    seriesMaxDD = (seriesMaxValue - seriesDailyValue) / seriesMaxValue
    seriesMaxDD.name = 'Max DD'
    maxDD = seriesMaxDD.max()

    dfOut = pd.concat([seriesDailyValue-1, seriesMaxDD, seriesDailyPosition], axis=1)

#    raise Exception
    
    dictResult = {}
    if seriesDailyReturn.min() <= -1:
        dictResult['retAnnualized'] = np.nan
        dictResult['stdAnnualized'] = np.nan
        dictResult['SharpeRatio'] = np.nan
        dictResult['maxDD'] = np.nan
    else:
        dictResult['retAnnualized'] = ret
        dictResult['stdAnnualized'] = std
        dictResult['SharpeRatio'] = SharpeRatio
        dictResult['maxDD'] = maxDD

    if np.isinf(-ret) or np.isnan(ret):
        #raise Exception
        pass

    return dfOut, dictResult

#####################
# utils functions
#####################
def generateDTTestStart(dfAll):
    listTimeStart = [
        datetime.time(0, 0),
        datetime.time(0, 30),
        datetime.time(1, 0),
        datetime.time(1, 30),
        datetime.time(2, 0),

        datetime.time(9, 0),
        datetime.time(9, 30),
        datetime.time(10, 0),
        datetime.time(10, 30),
        datetime.time(11, 0),

        datetime.time(13, 0),
        datetime.time(13, 30),
        datetime.time(14, 0),
        datetime.time(14, 30),

        datetime.time(21, 0),
        datetime.time(21, 30),
        datetime.time(22, 0),
        datetime.time(22, 30),
        datetime.time(23, 0),
        datetime.time(23, 30),
    ]

    listDate = set(dfAll[(dfAll.index.time < datetime.time(20, 0)) & (dfAll.index.time > datetime.time(8, 0))].index.date)
    listDate = list(listDate)
    listDate.sort()

    listDT = []
    for date in listDate:
        for time in listTimeStart:
            weekday = date.weekday()
            if weekday in [0,1,2,3,4]:
                dt = datetime.datetime.combine(date, time)               
            else:
                continue
            
            if weekday in [1, 2, 3, 4] and time > datetime.time(20, 0):
                dt = dt - datetime.timedelta(1, 0)
            
            if weekday == 0 and time > datetime.time(20, 0):
                dt = datetime.datetime.combine(date, time) - datetime.timedelta(3, 0)
            if weekday == 0 and time < datetime.time(5, 0):
                dt = datetime.datetime.combine(date, time) - datetime.timedelta(2, 0)

            listDT.append(dt)
    listDT.sort()
    return listDT

