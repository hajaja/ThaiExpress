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

import Dumpling.Point.GeneralModel as Model
reload(Model)

#########################################
# X and Y
#########################################
def generateXY(df, NDayTest):
    listFactor = []

    boolMomentum = True
    boolMomentumCarry = False
    boolCarry = False
    
    # assign dfTS
    if boolMomentumCarry or boolCarry:
        dfTS = dictDataSpec['dfTS']
        ixCommon = dfRaw.index.intersection(dfTS.index.get_level_values('TradingDay'))
        dfRaw.ix[ixCommon, 'TS2'] = dfTS.xs(dictDataSpec['Secu'], level='SecuCode').ix[ixCommon, 'TS2']

    # momentum
    if boolMomentum:
        listNDayMOM = [5, 10, 20, 60]
        strColumn = 'Close'
        for nDayMOM in listNDayMOM:
            strFactor = 'MOM%d'%nDayMOM
            listFactor.append(strFactor)
            df[strFactor] = df[strColumn] / df[strColumn].shift(nDayMOM) - 1
    
    # carry rate Momentum
    if boolMomentumCarry:
        listNDayMOMCarry = [20, 40]
        strColumn = 'TS2'
        for nDayMOMCarry in listNDayMOMCarry:
            strFactor = 'MOMCarry%d'%nDayMOMCarry
            listFactor.append(strFactor)
            df[strFactor] = df[strColumn] - df[strColumn].shift(nDayMOMCarry)

    # carry rate
    if boolCarry:
        #listFactor.append('TS2')
        dfRaw.ix[ixCommon, 'pRank'] = dfTS.xs(dictDataSpec['Secu'], level='SecuCode').ix[ixCommon, 'pRank']
        listFactor.append('pRank')

    # drop na
    df = df[df[listFactor].isnull().sum(axis=1)==0]

    # Y
    strColumnY = 'Close'
    df['ReturnFuture'] = df[strColumnY] / df[strColumnY].shift(NDayTest) - 1
    df['ReturnFuture'] = df['ReturnFuture'].shift(-NDayTest)
    ixNotNull = df[df['ReturnFuture'].isnull()==False].index
    df.ix[ixNotNull, 'Y'] = df.ix[ixNotNull, 'ReturnFuture'].apply(lambda x: -1 if x < 0 else 1)

    return df, listFactor

#########################################
# X and Y
#########################################
def prepareDictModelPoint(dictDataSpec):
    dictModel = {}
    if dictDataSpec['strModelName'] == 'SVM':
        dictModel['name'] = 'SVM'
        dictModel['kernel'] = 'linear'
        #dictModel['kernel'] = 'rbf'
        #dictModel['C'] = 1.
        #dictModel['gamma'] = 0.1
    elif dictDataSpec['strModelName'] == 'DecisionTreeClassifier':
        dictModel['name'] = 'DecisionTreeClassifier'
        dictModel['min_samples_leaf'] = 20
    elif dictDataSpec['strModelName'] == 'LogisticRegression':
        dictModel['name'] = 'LogisticRegression'
    elif dictDataSpec['strModelName'] == 'MultinomialNB':
        dictModel['name'] = 'MultinomialNB'
    elif dictDataSpec['strModelName'] == 'MultinomialNBSymbol':
        dictModel['name'] = 'MultinomialNBSymbol'
    elif dictDataSpec['strModelName'] == 'GaussianHMM':
        dictModel['name'] = 'GaussianHMM'
        dictModel['SEED_INSIDE_MODEL'] = 0

    return dictModel

def generateIndicatorPoint(dictDataSpec):
    dfRaw = dictDataSpec['df'].copy()
    dfRaw['indicator'] = np.nan

    # param
    NDayTrain = dictDataSpec['NDayTrain']
    NDayTest = dictDataSpec['NDayTest']

    # prepare dictModel
    dictModel = prepareDictModelPoint(dictDataSpec)

    # generate XY
    dfFactor, listFactor = generateXY(dfRaw, NDayTest)
    dfFactor = dfFactor[listFactor + ['Y', 'ReturnFuture']].dropna()

    # train & test
    listNIndex = range(NDayTrain, dfRaw.index.size, NDayTest)
    seriesIndicatorAll = dfRaw['Close'].apply(lambda x: np.nan)
    model = None
    for nTrainEnd in listNIndex:
        # calculate factor
        nTrainStart = nTrainEnd - NDayTrain

        dfTrain = dfFactor.iloc[nTrainStart:nTrainEnd]  # dfFactor.iloc[nTrainEnd-NDayTest:nTrainEnd]['Y'] is future information
        if dfTrain.index.size < 5:
            continue

        # train 
        dictParam = {}
        dictParam['X'] = dfTrain.ix[:-NDayTest, listFactor].values
        dictParam['Y'] = dfTrain.ix[:-NDayTest, 'Y'].values
        if dfTrain.ix[:-NDayTest, 'Y'].value_counts().size <= 1:
            continue
        else:
            model = Model.GeneralModel(dictModel)
            model.fit(dictParam)
        
        # determine direction for unsupervised learning, not necessary for supervised learning
        YValidated = model.predict(dictParam)
        dfTrain.ix[:-NDayTest, 'YValidated'] = YValidated
        dfReturnFutureMean = dfTrain.ix[:-NDayTest].groupby('YValidated')['ReturnFuture'].mean()
        YBullish = dfReturnFutureMean.argmax()

        # make prediction
        dictParam = {}
        dictParam['X'] = dfTrain[listFactor].values
        dictParam['Y'] = np.zeros(dfTrain['Y'].values.shape)
        YPredicted = model.predict(dictParam)

        # append output
        if YPredicted[-1] == YBullish:
            indicator = 1
        else:
            indicator = -1
        seriesIndicatorAll.ix[dfTrain.index[-1]] = indicator

        # correct or not
        print dfRaw.index[nTrainEnd], indicator

    # 
    return seriesIndicatorAll

#########################################
# Sequence
#########################################
def prepareDictModelSequence(dictDataSpec):
    if dictDataSpec['strModelName'] == 'CNN':
        dictModel = {
            'strFilePrefix': './', 
            'learning_rate': 0.001,
            'n_length': 28,
            'n_dimension': len(dictDataSpec['listFactorX']),
            'n_classes': 2,
            'dropout': 1.0, # Dropout, probability to keep units
            'thresholdBoundary': 0.0005,
            'NPred': 5,
            'NBatchSize': 128,
            'NBatchToTrain': 250,
            'strModelName': dictDataSpec['strModelName'],
            'SEED': 0,
        }

    return dictModel

def generateIndicatorSequence(dictDataSpec):
    dfRaw = dictDataSpec['df'].copy()
    dfRaw['indicator'] = np.nan
    dfRaw['Close'] = (dfRaw['PCT'] + 1).cumprod()
    dfRaw['StateObserved'] = (dfRaw['Close'].shift(-dictDataSpec['NPred']) / dfRaw['Close'] - 1).apply(np.sign)

    # prepare dictModel
    #listFactorX = ['Close', 'Volume', 'OI']
    listFactorX = ['Close']
    dictDataSpec['listFactorX'] = listFactorX
    dictModel = prepareDictModelSequence(dictDataSpec)
    import Dumpling.Seq.libTF as libTF
    reload(libTF)
    model = libTF.NNModel(dictModel)

    # prepare data 
    dictParam = {}
    dictParam['strModelName'] = dictDataSpec['strModelName']

    # train & test

    # train
    import Dumpling
    reload(Dumpling)
    X, Y = Dumpling.Seq.Utils.getXYSingleSecu(dfRaw.ix[:dfRaw.index.size/2], listFactorX, dictModel['n_length'])
    dictParam['X'] = X
    dictParam['Y'] = Y
    model.train_np(dictParam)
    
    # test
    X, Y = Dumpling.Seq.Utils.getXYSingleSecu(dfRaw.ix[dfRaw.index.size/2:], listFactorX, dictModel['n_length'])
    dictParam['X'] = X
    dictParam['Y'] = None
    pred = model.predict_np(dictParam)

    # 
    return seriesIndicatorAll

