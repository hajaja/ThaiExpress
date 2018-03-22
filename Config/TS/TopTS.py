import pandas as pd
import datetime
import numpy as np
import os
import re
import sys
from dateutil.parser import parse

import ThaiExpress.Common.Strategy as Strategy
reload(Strategy)
import ThaiExpress.Common.Utils as Utils
reload(Utils)
import UtilsTS
reload(UtilsTS)

if __name__ == '__main__':
    strParamSweep = sys.argv[1]
    if Utils.boolClearData:
        Utils.funcClearData(strParamSweep)

#####################
# configuration & data
#####################
dictParam = dict(Utils.dictParamTemplate)

########################################################
# train and test (generate indicator) in rolling way, only give indicator
########################################################
dictTrendTS = {}
dictTrendTS['TS2'] = UtilsTS.getTSXSMIndicator(Utils.dtBackTestStart, {'strMethodTrend':'TS2'})
#dictTrendTS['TS3'] = UtilsTS.getTSXSMIndicator(Utils.dtBackTestStart, {'strMethodTrend':'TS3'})

########################################################
# generate listDictDataSpec for backtest
########################################################
dictDataSpec = dict(Utils.dictDataSpecTemplate)
dictDataSpec['strModelName'] = 'TS'
dictDataSpec['freq'] = '1day'
listDictDataSpec = Utils.sweepParam(dictDataSpec, Utils.dictStrategyParamRange[strParamSweep])

########################################################
# back test each dictDataSpec
########################################################
for nSpec, dictDataSpec in enumerate(listDictDataSpec):
    print nSpec, len(listDictDataSpec), dictDataSpec['strCase']
    strFileAddress = Utils.dirResultPerCase + dictDataSpec['strCase'] + '.pickle'
    if os.path.exists(strFileAddress):
        continue

    #dfAll = Utils.getTradingDataPoint_Commodity(dictDataSpec).replace(np.inf, np.nan).dropna() # Error was here 20171012, fb.dce last none-NA data is 20170929
    dfAll = Utils.getTradingDataPoint_Commodity(dictDataSpec).replace(np.inf, np.nan)
    dtLastObservation = dfAll.index[-1]
    #dtEnter = dtLastObservation + datetime.timedelta(1, 0)
    #if dtEnter.weekday() >= 5:
    #    dtEnter = dtEnter + datetime.timedelta(3, 0)
    dtEnter = Utils.UtilsDB.dtTomorrow
    rowLast = dfAll.ix[dtLastObservation]
    rowLast.name = dtEnter
    dfAll = dfAll.append(rowLast)
    #dfAll = dfAll[dfAll['Volume']!=0]
    
    dictDataSpec['df'] = dfAll
    
    # stoploss
    dictParam['stoploss'] = dictDataSpec['decimalStoploss']
    if dictParam['stoploss'] == 0.99:
        dictParam['boolStoploss'] = False
    else:
        dictParam['boolStoploss'] = True

    # set time start, end, step
    dtDataStart = dfAll.index[0]
    dtDataEnd = dfAll.index[-1]
    NDayTrain = dictDataSpec['NDayTrain']
    NDayTest = dictDataSpec['NDayTest']
    NthFriday = dictDataSpec['NthFriday']
    NDayShift = dictDataSpec['NDayShift']
    NMonthStart = dictDataSpec['NMonthStart']

    listDTTestStart = Utils.generateNthFriday(NthFriday, NMonthStart, NDayShift)
    listDTTestStart = [x for x in listDTTestStart if x <= Utils.dtLastData]
    
    dtDataStart = datetime.datetime.combine(dfAll.index[0], dtDataStart.time())
    dtTrainStart = dtDataStart
    seriesIndicatorAll = pd.Series()
    seriesYAll = pd.Series()

    seriesIndicatorAll = dfAll['Close'].copy()
    seriesIndicatorAll = seriesIndicatorAll.apply(lambda x: np.nan)

    # back test this data spec
    if dtEnter > listDTTestStart[-1]:
        listDTTestStart.append(dtEnter)
    for nWindow, dtTestStart in enumerate(listDTTestStart):
        # the start & end of test window
        dtTestStart = listDTTestStart[nWindow]
        if nWindow < len(listDTTestStart)-1:
            dtTestEnd = listDTTestStart[nWindow + 1]
        else:
            dtTestEnd = dtEnter
            if dtTestEnd < dtTestStart:
                break
        # the start & end of train window
        dtTrainEnd = dtTestStart
        if dfAll[dfAll.index < dtTestStart].index.size < NDayTrain:
            continue
        else:
            dtTrainStart = dfAll[dfAll.index < dtTestStart].index[-NDayTrain]
        
        # get the train window and test window
        dfTrain = dfAll[(dfAll.index>=dtTrainStart)&(dfAll.index<dtTrainEnd)].copy()
        dfTest = dfAll[(dfAll.index>=dtTestStart)&(dfAll.index<dtTestEnd)].copy()
        
        if dfTrain.empty:
            continue
            
        # determine the trend 
        dfTrend = dictTrendTS[dictDataSpec['strMethodTrend']]
        seriesIndicator = dfTrend[dfTrend['SecuCode']==dictDataSpec['Secu']]
        dtTrainLast = dfTrain.index[-1]
        if dtTrainLast in seriesIndicator.index:
            trend = seriesIndicator.ix[dtTrainLast, 'indicator']
        else:
            trend = 0

        if NDayTrain <= 5:
            PCTTrendRecognition = 0.008
        elif NDayTrain <= 10:
            PCTTrendRecognition = 0.015
        elif NDayTrain <= 15:
            PCTTrendRecognition = 0.020
        elif NDayTrain <= 20:
            PCTTrendRecognition = 0.025
        else:
            PCTTrendRecognition = 0.025

        if dfTrain['Volume'].mean() < Utils.NThresholdVolume:
            trend = 0
        elif abs(dfTrain.ix[-1, 'Close'] / dfTrain.ix[0, 'Open'] - 1) < PCTTrendRecognition:
            trend = 0
            pass

        '''
        retPast = dfTrain.ix[-1, 'Close'] / dfTrain.ix[0, 'Open'] - 1 
        if trend < 0 and retPast > PCTTrendRecognition:
            trend = 0
        elif trend > 0 and retPast < -PCTTrendRecognition:
            trend = 0
            pass
        #'''
            
        #seriesIndicatorAll.ix[dfTest.index[0]] = trend
        seriesIndicatorAll.ix[dfTrain.index[-1]] = trend    # in Strategy.py, there will be 1 bar shift

        print dtTrainStart, dtTrainEnd, dtTestStart, dtTestEnd, dtTrainLast, trend
    
    ########################################################
    # evaluate the strategy (the indicator generated)
    ########################################################
    # evaluate
    #seriesIndicatorAll = seriesIndicatorAll.ffill()
    if dictDataSpec['Secu'] == 'fb.dce':
        # raise Excetion
        pass
    reload(Strategy)
    strategy = Strategy.Strategy(dfAll.ix[seriesIndicatorAll.index], dictParam)
    strategy.seriesYAll = seriesYAll
    strategy.df['indicator'] = seriesIndicatorAll
    dictResultS = strategy.evaluateLongShortSimplified()

    # change dominant contract
    indexDominantChange = dfAll[dfAll['boolDominantChange']==True].index
    indexDominantChange = indexDominantChange & strategy.seriesReturnPCTHoldDaily[strategy.seriesReturnPCTHoldDaily!=0].index
    strategy.seriesReturnPCTHoldDaily.ix[indexDominantChange] = strategy.seriesReturnPCTHoldDaily[indexDominantChange] - dictParam['commission']

    # for memory
    if dictParam['boolStoploss']:
        strategy.df = strategy.df[['Close', 'indicator', 'returnPCTHold', 'Stoploss', 'returnStoploss', 'StoplossPrice', 'StoplossPriceTomorrow']]
    else:
        strategy.df = strategy.df[['Close', 'indicator', 'returnPCTHold']]
    
    # add to dict
    dictDataSpec['strategy'] = strategy

    # show result
    print strategy.dictEvaluation

    # save data
    sDataSpec = pd.Series(dictDataSpec)
    strFileAddress = Utils.dirResultPerCase + dictDataSpec['strCase'] + '.pickle'
    sDataSpec.to_pickle(strFileAddress)

