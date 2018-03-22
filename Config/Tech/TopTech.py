import pandas as pd
import datetime
import gc
import numpy as np
import os, sys

import ThaiExpress.Common.Strategy as Strategy
reload(Strategy)
import ThaiExpress.Common.Utils as Utils
reload(Utils)
import ThaiExpress.Config.TS.UtilsTS as UtilsTS
reload(UtilsTS)
import UtilsTech
reload(UtilsTech)
import logging

if __name__ == '__main__':
    strParamSweep = sys.argv[1]
    if Utils.boolClearData:
        Utils.funcClearData(strParamSweep)

#########################################
# logging
#########################################
import logging
logging.basicConfig(
        level = logging.INFO,
        format = '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt = '%m-%d %H:%M:%S',
        filename = 'Run.log',
        filemode = 'a'
        )
if len(logging.getLogger().handlers) == 1:
    ch = logging.StreamHandler()
    logging.getLogger().addHandler(ch)
    logging.log(logging.DEBUG, 'Run starts')

#####################
# params
#####################
dictParam = dict(Utils.dictParamTemplate)
    
########################################################
# generate listDictDataSpec for backtest
########################################################
dictDataSpec = dict(Utils.dictDataSpecTemplate)
dictDataSpec['strModelName'] = strParamSweep

#if dictDataSpec['strModelName'].startswith('TSCRebound'):
#    dfTS = UtilsTS.getTS(Utils.dtBackTestStart-datetime.timedelta(365, 0), {'strMethodTrend':'TS2'})
#    dictDataSpec['dfTS'] = dfTS

dfTS = UtilsTS.getTS(Utils.dtBackTestStart-datetime.timedelta(365, 0), {'strMethodTrend':'TS2'})
dictDataSpec['dfTS'] = dfTS
listDictDataSpec = Utils.sweepParam(dictDataSpec, Utils.dictStrategyParamRange[strParamSweep])

########################################################
# for each data spec, evaluate
########################################################
for nSpec, dictDataSpec in enumerate(listDictDataSpec):
    # does data exists
    print nSpec, len(listDictDataSpec), dictDataSpec['strCase']
    strFileAddress = Utils.dirResultPerCase + '/' + dictDataSpec['strCase'] + '.pickle'

    # read data & append a pseudo tomorrow data, this is for generating indicator tomorrow
    dfAll = Utils.getTradingDataPoint_Commodity(dictDataSpec).replace(np.inf, np.nan)
    dtLastObservation = dfAll.index[-1]
    dtEnter = dtLastObservation + datetime.timedelta(1, 0)
    if dtEnter.weekday() >= 5:
        dtEnter = dtEnter + datetime.timedelta(3, 0)
    rowLast = dfAll.ix[dtLastObservation]
    rowLast.name = dtEnter
    dfAll = dfAll.append(rowLast)
    
    dictDataSpec['df'] = dfAll
    seriesIndicatorAll = UtilsTech.generateIndicator(dictDataSpec)
    
    # evaluate 
    dictParam['switchPlot'] = False
    dictParam['boolStoploss'] = False
    dictParam['boolStopProfit'] = False
    strategy = Strategy.Strategy(dfAll.ix[seriesIndicatorAll.index], dictParam)
    strategy.df['indicator'] = seriesIndicatorAll
    dictResultS = strategy.evaluateLongShortSimplified()

    # add to dict
    strategy.df = strategy.df[['indicator']]    # for memory
    dictDataSpec['strategy'] = strategy

    # save data
    del dictDataSpec['df']
    sDataSpec = pd.Series(dictDataSpec)
    strFileAddress = Utils.dirResultPerCase + dictDataSpec['strCase'] + '.pickle'
    sDataSpec.to_pickle(strFileAddress)

    # clear memory
    strategy.df = None
    strategy = None
    gc.collect()
    seriesIndicatorAll = None
    dictResultS = None
    sDataSpec = None
    dictDataSpec = None
    gc.collect()
    del dfAll
    dfAll = None
    gc.collect()
    
