# -*- coding = utf8 -*-
import pandas as pd
import datetime, re, gc, os, sys, logging
import numpy as np

import ThaiExpress.Common.Strategy as Strategy
reload(Strategy)
import ThaiExpress.Common.Utils as Utils
reload(Utils)
import ThaiExpress.Common.ParamRange as ParamRange
reload(ParamRange)
import ThaiExpress.Config.TS.UtilsTS as UtilsTS
reload(UtilsTS)

#####################
# logging
#####################
import RoyalMountain as RM
strFileAddress = 'Run.log'
RM.MISC.Logger.initLogging(strFileAddress)

#####################
# prepare data
#####################
import UtilsCSA
reload(UtilsCSA)
#UtilsCSA.prepareTrend(Utils.dtBackTestStart - datetime.timedelta(365,1))
#UtilsCSA.concatContractDiff()

#########################################
# param
#########################################
NDayBeforeSettle = 10

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
# read data
#####################
strFileAddress = 'dfTS.pickle'
dfTSAll = pd.read_pickle(strFileAddress)
#dfTSAll = UtilsTS.getTS(Utils.dtBackTestStart, {'strMethodTrend':'TS2'})
dfTSAll = dfTSAll.reset_index().set_index(['SecuCode', 'TradingDay']).sort_index()

strFileAddress = Utils.dirDataSource + '/1day/' + 'CommodityFuture_CSA.pickle' 
dfCSAAll = pd.read_pickle(strFileAddress)

#####################
# buy nike 
#####################
#dfCSAAll = dfCSAAll[dfCSAAll['SecuCodeRaw'].isin(ParamRange.listSecuInactive)==False]
dfCSAAll = dfCSAAll[dfCSAAll['SecuCodeRaw'].isin(ParamRange.listSecuHEISE)]
#dfCSAAll = dfCSAAll[dfCSAAll['SecuCodeRaw'].isin(ParamRange.listSecuNONG)]
#dfCSAAll = dfCSAAll[dfCSAAll['SecuCodeRaw'].isin(ParamRange.listSecuHUA)]
listSecuCode = dfCSAAll['SecuCode'].unique()
sSecuCode_SecuCodeRaw = dfCSAAll.groupby('SecuCode')['SecuCodeRaw'].first()
listSDollarReturn = []
for strPair in listSecuCode[:]:
#for strPair in ['HC15']:
    strToLog = 'strPair {0}'.format(strPair)
    logging.log(logging.INFO, strToLog)

    # TS
    SecuCodeRaw = sSecuCode_SecuCodeRaw[strPair]
    if SecuCodeRaw in ParamRange.listSecuInactive:
        strToLog = 'SecuCodeRaw: {0} is not suitable fr CSA'.format(SecuCodeRaw)
        logging.log(logging.INFO, strToLog)
        continue
    dfTS = dfTSAll.ix[SecuCodeRaw]

    # CSA
    df = dfCSAAll[dfCSAAll['SecuCode']==strPair]
    month = int(strPair[-2])
    strTSDelta = 'TSDelta_S%d'%month
    df[strTSDelta] = dfTS.ix[df.index, strTSDelta]
    ThresholdTSRate = df[strTSDelta].quantile(0.9)
    #ThresholdTSRate = 100.
    boolDelta = df[strTSDelta] > ThresholdTSRate
    print strPair, 

    #strTSRet = 'TSRet_S%d'%month
    #df[strTSRet] = dfTS.ix[df.index, strTSRet]
    #boolDelta = df[strTSRet] > df[strTSRet].quantile(0.85)
    #strTSDeltaSettle = 'TSDeltaSettle_S%d'%month
    #df[strTSDeltaSettle] = dfTS.ix[df.index, strTSDeltaSettle]
    #boolDelta = df[strTSDeltaSettle] > df[strTSDeltaSettle].quantile(0.7)
    strNDayToSettle = 'NDayToSettle_S%d'%month
    df[strNDayToSettle] = dfTS.ix[df.index, strNDayToSettle]

    boolDate = (df[strNDayToSettle]<90)&(df[strNDayToSettle]>20)
    boolCondition = boolDelta
    #boolCondition = boolCondition & boolDate

    dfNike = df[boolCondition]
    if dfNike.empty:
        strToLog = 'strPair {0}, no qualified enter point'.format(strPair)
        logging.log(logging.INFO, strToLog)
        continue
    
    # determine indicator
    listContractCode = dfNike['ContractCode'].unique()
    listSDollarReturnYear = []
    for ContractCode in listContractCode:
        dfNikePart = dfNike[dfNike['ContractCode']==ContractCode]
        dfPart = df[df['ContractCode']==ContractCode]
        if dfNikePart.empty:
            strToLog = 'strPair {0}, ContractCode {1}, no qualified enter point dfNikePart'.format(strPair, ContractCode)
            logging.log(logging.INFO, strToLog)
            continue
        dfPartEnter = dfPart[dfPart.index > dfNikePart.index[0]]
        if dfPartEnter.empty:
            strToLog = 'strPair {0}, ContractCode {1}, no qualified enter point dfPartEnter'.format(strPair, ContractCode)
            logging.log(logging.INFO, strToLog)
            continue
        ixEnter = dfPartEnter.index[0]
        rowEnter = dfPart.ix[ixEnter]
        sDollarReturn = dfPart.ix[dfPart.index>=rowEnter.name, 'DiffSettle'] - rowEnter['DiffOpen']
        sDollarReturn = sDollarReturn.diff() / rowEnter['MinSettle']
        if sDollarReturn.size < NDayBeforeSettle:
            strToLog = 'strPair {0}, ContractCode {1}, too late to enter'.format(strPair, ContractCode)
            logging.log(logging.INFO, strToLog)
            continue
        dfPartLowVolume = dfPartEnter[dfPartEnter['TurnoverVolume'] < Utils.NThresholdVolume]
        if dfPartLowVolume.empty:
            ixLast = dfPartEnter.index[-1]
        else:
            ixLast = dfPartLowVolume.index[0]
        '''
        sDollarReturnStopProfit = sDollarReturn[sDollarReturn > 1.1]
        if sDollarReturnStopProfit.empty is False:
            ixStopProfit = sDollarReturnStopProfit.index[0]
            ixLast = min(ixBeforeSettle, ixStopProfit)
        #'''
        sDollarReturn = sDollarReturn[sDollarReturn.index<ixLast]
        listSDollarReturnYear.append(sDollarReturn)
    if len(listSDollarReturnYear) == 0:
        continue
    sDollarReturn = pd.concat(listSDollarReturnYear, axis=0)
    sDollarReturn.name = strPair

    # duplicates?
    sDollarReturn = sDollarReturn.reset_index().drop_duplicates(subset='TradingDay').set_index('TradingDay').sort_index()
    listSDollarReturn.append(sDollarReturn)

dfResult = pd.concat(listSDollarReturn, axis=1)
sPCT = dfResult.fillna(0).mean(1)
sPCT = sPCT[sPCT.index > datetime.datetime(2005,1,1)]
sPCT0 = sPCT

sPCT = dfResult.mean(1)
sPCT = sPCT[sPCT.index > datetime.datetime(2005,1,1)]
dictMetric = Utils.funcMetric(sPCT)
print dictMetric
