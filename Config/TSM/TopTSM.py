import pandas as pd
import datetime
import numpy as np
import os
import re
import sys

import ThaiExpress.Common.Strategy as Strategy
reload(Strategy)
import ThaiExpress.Common.Utils as Utils
reload(Utils)
import UtilsTSM
reload(UtilsTSM)

if __name__ == '__main__':
    strParamSweep = sys.argv[1]
    if Utils.boolClearData:
        Utils.funcClearData(strParamSweep)

#####################
# configuration & data
#####################
dictParam = dict(Utils.dictParamTemplate)

########################################################
# generate listDictDataSpec for backtest
########################################################
dictDataSpec = dict(Utils.dictDataSpecTemplate)
dictDataSpec['strModelName'] = 'TSM'
dictDataSpec['freq'] = '1day'
listDictDataSpec = Utils.sweepParam(dictDataSpec, Utils.dictStrategyParamRange[strParamSweep])

########################################################
# back test each data spec
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

    # set time start, end, step
    dtDataStart = dfAll.index[0]
    dtDataEnd = dfAll.index[-1]
    NDayTrain = dictDataSpec['NDayTrain']
    NDayTest = dictDataSpec['NDayTest']
    NDayShift = dictDataSpec['NDayShift']
    NWeekStart = dictDataSpec['NWeekStart']
    
    listDTTestStart = Utils.generateDTTestStartCalendarDay(NDayTest, NDayShift, NWeekStart)
    listDTTestStart = [x for x in listDTTestStart if x <= Utils.dtLastData]
    
    dtDataStart = datetime.datetime.combine(dfAll.index[0], dtDataStart.time())
    dtTrainStart = dtDataStart
    seriesIndicatorAll = pd.Series()
    seriesYAll = pd.Series()

    seriesIndicatorAll = dfAll['Close'].copy()
    seriesIndicatorAll = seriesIndicatorAll.apply(lambda x: np.nan)

    # back test this data spec
    #for nWindow, dtTestStart in enumerate(listDTTestStart[:-1]):
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
        dictParamTSM = {
                'df': dfTrain,
                'strMethodTrend': dictDataSpec['strMethodTrend'],
                'NDayTrain': dictDataSpec['NDayTrain'],
                }
        trend = UtilsTSM.determineTrend(dictParamTSM)
        
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

        if dtTestStart == datetime.datetime(2017, 4, 14):
            #raise Exception
            pass
        if dfTrain['Volume'].mean() < Utils.NThresholdVolume:
            trend = 0
        elif dfTrain.empty is False and dfTrain.ix[dfTrain.index[-1], 'Volume'] < 100:
            trend = 0
        elif abs(dfTrain.ix[-1, 'Close'] / dfTrain.ix[0, 'Open'] - 1) < PCTTrendRecognition:
            #print PCTTrendRecognition
            trend = 0
            pass

        '''
        retPast = dfTrain.ix[-1, 'Close'] / dfTrain.ix[0, 'Open'] - 1 
        if trend < 0 and retPast > -PCTTrendRecognition:
            trend = 0
        elif trend > 0 and retPast < PCTTrendRecognition:
            trend = 0
        #'''    
        #seriesIndicatorAll.ix[dfTest.index[0]] = trend
        seriesIndicatorAll.ix[dfTrain.index[-1]] = trend    # in Strategy.py, there will be 1 bar shift

        print dtTrainStart, dtTrainEnd, dtTestStart, dtTestEnd, trend
    
    ########################################################
    # evaluate the strategy (the indicator generated)
    ########################################################
    # evaluate
    reload(Strategy)
    dictParam['boolStoploss'] = dictDataSpec['boolStoploss']
    strategy = Strategy.Strategy(dfAll.ix[seriesIndicatorAll.index], dictParam)
    strategy.seriesYAll = seriesYAll
    strategy.df['indicator'] = seriesIndicatorAll
    dictResultS = strategy.evaluateLongShortSimplified()
    
    # dominant contract
    indexDominantChange = dfAll[dfAll['boolDominantChange']==True].index
    indexDominantChange = indexDominantChange & strategy.seriesReturnPCTHoldDaily[strategy.seriesReturnPCTHoldDaily!=0].index
    strategy.seriesReturnPCTHoldDaily.ix[indexDominantChange] = strategy.seriesReturnPCTHoldDaily[indexDominantChange] - dictParam['commission']

    # for memory
    if dictDataSpec['boolStoploss']:
        strategy.df = strategy.df[['Close', 'indicator', 'returnPCTHold', 'indicatorOfDecision', 'Stoploss', 'returnStoploss', 'StoplossPrice', 'StoplossPriceTomorrow']]
    else:
        strategy.df = strategy.df[['Close', 'indicator', 'returnPCTHold', 'indicatorOfDecision']]

    # add to dict
    dictDataSpec['strategy'] = strategy

    # save data
    sDataSpec = pd.Series(dictDataSpec)
    strFileAddress = Utils.dirResultPerCase + dictDataSpec['strCase'] + '.pickle'
    sDataSpec.to_pickle(strFileAddress)

