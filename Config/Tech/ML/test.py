import pandas as pd
import datetime
import gc
import numpy as np
import os, sys
from memory_profiler import profile
import memory_profiler

import ThaiExpress.Common.Strategy as Strategy
reload(Strategy)
import ThaiExpress.Common.Utils as Utils
reload(Utils)

import Dumpling
reload(Dumpling)
import Dumpling.Seq.libTF as libTF
reload(libTF)

# parameter
strModelName = 'CNN'
strParamSweep = 'ML_Test0_Daily'

# read data
dictDataSpec = dict(Utils.dictDataSpecTemplate)
dictDataSpec['strModelName'] = strParamSweep
dictDataSpec['Secu'] = 'j.dce'
dictDataSpec['freq'] = '1min'
listDictDataSpec = Utils.sweepParam(dictDataSpec, Utils.dictStrategyParamRange[strParamSweep])
dictDataSpec = listDictDataSpec[0]
dfAll = Utils.getTradingDataPoint_Commodity(dictDataSpec).replace(np.inf, np.nan)
dfAll = dfAll.rename(columns={'OpenRaw':'Open', 'HighRaw':'High', 'LowRaw':'Low', 'CloseRaw':'Close'})

dfAll['StateObserved'] = dfAll['PCT'].shift(-1).apply(np.sign)
#listFactorX = ['Open', 'High', 'Low', 'Close', 'Volume']
listFactorX = ['Close', 'Volume']
dfAll[listFactorX] = dfAll[listFactorX] / dfAll[listFactorX].shift(10) - 1
df = dfAll[listFactorX + ['StateObserved']]
df = df.ix[-10000:]

# prepare data
dictParam = {}
NLength = 28
X, Y = Dumpling.Seq.Utils.getXYSingleSecu(df, listFactorX, NLength)
dictParam['strModelName'] = strModelName
dictParam['X'] = X
dictParam['Y'] = Y

# initialize model
strFilePrefix = '/home/dongcs/workspace/python/ThaiExpress/Config/Tech/'
dictModel = {
    'strFilePrefix': strFilePrefix, 
    'learning_rate': 0.02,
    'n_length': NLength,
    'n_dimension': len(listFactorX),
    'n_classes': 2,
    'dropout': 1.0, # Dropout, probability to keep units
    'thresholdBoundary': 0.0005,
    'NLength': NLength,
    'NPred': 5,
    'NBatchSize': 256,
    'NBatchToTrain': 25000,
    'strModelName': strModelName,
    'SEED': 0,
    }
model = libTF.NNModel(dictModel)

# train
model.train_np(dictParam)




