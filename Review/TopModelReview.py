#coding=utf8
#######################################################
'''
# What is Model Review
Any strategy is dependent on parameters. Constant parameters are preferred. However, the parameters are usually changing. We define the process to tune the parameters as "Model Review". 

Although this process is to tune parameters, it needs new parameters itself:
1. the lookback period to calculate what is the best parameter, NMonthLookBack
2. the period to conduct model review, NMonthModelReview

Example: for Time Series Momentum, we have parameters NDayTrain, NDayTest, NDayShift, NWeekStart to review. The best set of parameters is determined on its performance in the past NMonthLookBack, and the model review process will be conducted NMonthModelReview later. 

# How to determine NMonthModelReview, NMonthLookBack
For each (NMonthLookBack, NMonthModelReview), we have 10 value series with different review start date. Among them, the one with mininmum CR (worst) is selected as the score of this (NMonthLookBack, NMonthModelReview), which means we care the worst performance of (NMonthLookBack, NMonthModelReview). Of course, the (NMonthLookBack, NMonthModelReview) with the best worst-case performance is used, and its worst-case value series is as the Model Review version performance of the strategy. 

# Known problems
## The path dependency
The path dependency is only partially solved by use 10 different review start dates. Actually, a more reasonable method to determine the review start dates is also necessary

## How to tune (NMonthLookBack, NMonthModelReview)
The process of tuning parameter is always relying on new parmeter, and this is infinite loop. To solve this, we review the NMonthModelReview and NMonthLookBack every year, and this is fixed. 

'''
#######################################################
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

if __name__ == '__main__':
    strParamSweep = sys.argv[1]

# param
strColumnToSelect = 'CR'
listCriterion = Utils.dictStrategyParamRange[strParamSweep].keys()
listCriterion = list(set(listCriterion).difference(set(['Secu', 'strModelName', 'strCloseAtDayEnd', 'NWeekStart', 'NMonthStart'])))
print listCriterion

# utils
def funcCalcMetricRolling(sDailyReturn):
    sPart = sDailyReturn
    sValue = (1+sPart).cumprod()
    sValueMonthly = sValue.resample('1m').last()
    listDict = []
    for nIndex in range(12, sValueMonthly.index.size):
        sValueMonthlyPart = sValueMonthly.ix[nIndex-12:nIndex]
        sValueMonthlyPart = sValueMonthlyPart / sValueMonthlyPart[0]
        sValueMonthlyPartMax = sValueMonthlyPart.expanding().max()
        sValueMonthlyPartDD = (sValueMonthlyPart / sValueMonthlyPartMax) - 1
        sValueMonthlyPartDD = -sValueMonthlyPartDD
        Ret = sValueMonthlyPart[-1] - 1
        DD = sValueMonthlyPartDD.max()
        DD = max(DD, 0.001)
        Vol = sValueMonthlyPart.pct_change().std() * np.sqrt(12)
        dictOne = {'Ret': Ret, 'DD': DD, 'Vol': Vol, 'CR': Ret/DD, 'SR': Ret/Vol}
        listDict.append(dictOne)
    dfMetric = pd.DataFrame(listDict, index=sValueMonthly.index[12:])

    dictMetric = {
    'Ret': dfMetric['Ret'].min(),
    'Vol': dfMetric['Vol'].max(),
    'DD': dfMetric['DD'].max(),
    'SR': dfMetric['SR'].min(),
    'CR': dfMetric['CR'].min(),
    }

    return dictMetric

# read all pickle
strDirModelReview = '%s/%s/'%(Utils.dirResultPerCase, strParamSweep)
listDailyReturn = []
listPortConfig = []
listMetric = []
for root, dirs, files in os.walk(strDirModelReview):
    if root.find('NDayShift4') < 0 or root.find('strMethodTrendTS3') > 0:
        #continue
        pass 
    for nFile, name in enumerate(np.sort(files)):
        if name == 'dfOut.pickle':
            strFileAddress = root + '/' + name
            dfOut = pd.read_pickle(strFileAddress)
            sDailyReturn = (dfOut['Cum Return'] + 1).pct_change()
            sDailyReturn.name = root
            listDailyReturn.append(sDailyReturn)
            dictMetric = Utils.funcMetric(sDailyReturn.ix[:-1])
            dictMetric['name'] = root
            listMetric.append(dictMetric)
        if name == 'portconfig.pickle':
            strFileAddress = root + '/' + name
            sPortConfig = pd.read_pickle(strFileAddress)
            dictOne = sPortConfig.to_dict()
            dictOne['name'] = root
            listPortConfig.append(dictOne)

dfAllCase = pd.concat(listDailyReturn, axis=1)
dfPortConfig = pd.DataFrame(listPortConfig).set_index('name')
dfMetric = pd.DataFrame(listMetric).set_index('name')
dfMetric = pd.concat([dfPortConfig, dfMetric], axis=1)
strFileAddress = '%s_AllCase.xlsx'%strParamSweep
dfMetric.to_excel(strFileAddress, index=False)

# Model Review
dtReviewStart = datetime.datetime(2010,1,1)
dtDataEnd = Utils.dfExe.index.get_level_values('TradingDay').max()
dfResultAll = pd.DataFrame()
for NDayShiftModelReview in range(0, 100, 10):
    listResult = []
    for NMonthModelReview in [3, 6, 12]:
        for NMonthLookBack in [12+12, 24+12, 60+12]:
            print NDayShiftModelReview, NMonthLookBack, NMonthModelReview
            sDateReview = pd.date_range(dtReviewStart - datetime.timedelta(NMonthModelReview * 31), datetime.datetime.now(), freq='%dM'%NMonthModelReview)
            sDateReview = sDateReview + datetime.timedelta(NDayShiftModelReview, 0)
            sDateReview = sDateReview.tolist()
            sDateReview.append(Utils.dfExe.index.get_level_values('TradingDay').max())
            # iterate all case and determine which case to use at dtModelReview
            sDailyConcat = pd.Series()
            listTuple = []
            for dtModelReview in sDateReview:
                dtModelReviewEnd = min(dtModelReview, dtDataEnd)
                dtModelReviewStart = dtModelReview - datetime.timedelta(30 * NMonthLookBack)
                # concat
                if dtModelReviewEnd == sDateReview[0]:
                    pass
                else:
                    listCase = dfPerf.ix[dfPerf[strColumnToSelect].argmax(), 'listCase']
                    NSubStrategy = len(listCase)
                    sDailyPart = dfAllCase[listCase].mean(1)
                    sDailyPart = sDailyPart[(sDailyPart.index>=dtModelReviewEndPrev) & (sDailyPart.index<dtModelReviewEnd)]
                    tupleParam = dfPerf[strColumnToSelect].argmax()
                    tupleParam = tupleParam + (NSubStrategy, )
                    listTuple.append(tupleParam)

                    sDailyConcat = sDailyConcat.append(sDailyPart)
    
                # calculate performance in this period
                listCasePerformance = []
                for strCase in dfAllCase.columns:
                    dictOne = dfPortConfig.ix[strCase].to_dict()
                    # calculate metric
                    sCase = dfAllCase[strCase]
                    sPart = sCase[(sCase.index>=dtModelReviewStart) & (sCase.index<dtModelReviewEnd)]
                    dictMetric = Utils.funcMetric(sPart)
                    dictOne.update(dictMetric)
                    dictOne['name'] = strCase
                    dictOne['sPCTDaily'] = sPart
                    
                    listCasePerformance.append(dictOne)
                dfCasePerformance = pd.DataFrame(listCasePerformance).set_index('name')

                # which is the best param set
                def funcPerfOfSeverPort(df):
                    listSeries = []
                    listCase = []
                    for ix, row in df.iterrows():
                        sDailyReturn = row['sPCTDaily']
                        listSeries.append(sDailyReturn)
                        listCase.append(row.name)
                    sDailyReturn = pd.concat(listSeries, axis=1).mean(1)
                    dictOne = funcCalcMetricRolling(sDailyReturn)
                    dictOne['listCase'] = listCase
                    return pd.Series(dictOne)
                dfPerf = dfCasePerformance.groupby(listCriterion).apply(funcPerfOfSeverPort)

                # update dtModelReviewEndPrev
                dtModelReviewEndPrev = dtModelReviewEnd
            # 
            sDailyConcat = sDailyConcat[sDailyConcat.index > dtReviewStart]
            sDailyConcat = sDailyConcat[:-1]
            
            dictMetric = Utils.funcMetric(sDailyConcat)
            dictMetric['NMonthModelReview'] = NMonthModelReview
            dictMetric['NMonthLookBack'] = NMonthLookBack
            dictMetric['sDailyReturn'] = sDailyConcat
            dictMetric['dfTuple'] = pd.DataFrame(listTuple, columns=listCriterion+['NSubStrategy'], index=sDateReview[:-1])
            listResult.append(dictMetric)

    dfResult = pd.DataFrame(listResult)
    dfResult['NDayShiftModelReview'] = NDayShiftModelReview
    dfResultAll = dfResultAll.append(dfResult)

# find the best param set
strReviewSetOption = 'WorstInBestReviewSet'
#strReviewSetOption = 'BestInSample'
dfResultAll = dfResultAll.reset_index()
if strReviewSetOption == 'BestInSample':
    listDict = []
    for strColumn in dfAllCase.columns:
        sPCTDaily = dfAllCase[strColumn]
        sPCTDaily = sPCTDaily[sPCTDaily.index > dtReviewStart]
        dictMetric = Utils.funcMetric(sPCTDaily)
        dictOne = dictMetric
        dictOne['sPCTDaily'] = sPCTDaily
        listDict.append(dictOne)
    dfAllCaseAfterReviewStart = pd.DataFrame(listDict, index=dfAllCase.columns)
    sDailyReturnBest = dfAllCaseAfterReviewStart.ix[dfAllCaseAfterReviewStart[strColumnToSelect].argmax(), 'sPCTDaily']
    dfTuple = pd.DataFrame()
elif strReviewSetOption == 'BestInBestReviewSet':
    sDailyReturnBest = dfResultAll.ix[dfResultAll[strColumnToSelect].argmax(), 'sDailyReturn']
    dfTuple = dfResultAll.ix[dfResultAll[strColumnToSelect].argmax(), 'dfTuple']
elif strReviewSetOption == 'WorstInBestReviewSet':
    sCRAllSet = dfResultAll.groupby(['NMonthLookBack', 'NMonthModelReview'])[strColumnToSelect].min()
    ixReviewSet = sCRAllSet.argmax()
    dfResultBestSet = dfResultAll.set_index(['NMonthLookBack', 'NMonthModelReview']).ix[ixReviewSet]
    dfResultBestSet = dfResultBestSet.reset_index()
    sDailyReturnBest = dfResultBestSet.ix[dfResultBestSet[strColumnToSelect].argmin(), 'sDailyReturn']
    dfTuple = dfResultBestSet.ix[dfResultBestSet[strColumnToSelect].argmin(), 'dfTuple']

sDailyReturnBest = sDailyReturnBest[sDailyReturnBest.index > dtReviewStart]
sMetricPerYear = sDailyReturnBest.groupby(sDailyReturnBest.index.year).apply(Utils.funcMetric)
sMetricPerYear.name = strColumnToSelect
dictMetricBest = Utils.funcMetric(sDailyReturnBest)
print dictMetricBest

# is the best param set (NMonthLookBack, NMonthModelReview) stable or not ?
dfResultAll = dfResultAll.reset_index().set_index(['NMonthLookBack', 'NMonthModelReview', 'NDayShiftModelReview'])
dfResultAllAllYear = pd.DataFrame()
for year in range(2010, 2017+1):
    listDict = []
    for ix, row in dfResultAll.iterrows():
        sPCTDaily = row['sDailyReturn']
        sPCTDaily = sPCTDaily[sPCTDaily.index.year==year]
        dictMetric = Utils.funcMetric(sPCTDaily)
        dictOne = dictMetric
        listDict.append(dictOne)
    dfResultAllOneYear = pd.DataFrame(listDict, index=dfResultAll.index)
    dfResultAllOneYear['year'] = year
    dfResultAllAllYear = dfResultAllAllYear.append(dfResultAllOneYear)

dfResultAllAllYear.reset_index().groupby(['year', 'NMonthLookBack', 'NMonthModelReview'])[strColumnToSelect].min()
dfPerfByYear = pd.DataFrame(dfResultAllAllYear.reset_index().groupby(['year', 'NMonthLookBack', 'NMonthModelReview'])[strColumnToSelect].min())

# out
strFileAddress = '%s_%s.xlsx'%(strParamSweep, strColumnToSelect)
excel_writer = pd.ExcelWriter(strFileAddress, date_format='YYYYMMDD')
Utils.funcWriteExcel(dfPerfByYear, excel_writer, sheet_name='PerfOfReviewSet')
Utils.funcWriteExcel(pd.DataFrame(pd.Series(dictMetricBest)), excel_writer, sheet_name='MetricOf%s'%strReviewSetOption)
Utils.funcWriteExcel(dfTuple, excel_writer, sheet_name='BestParamAtEachReview')
Utils.funcWriteExcel(pd.DataFrame(sDailyReturnBest), excel_writer, sheet_name='DailyReturn%s'%strReviewSetOption)
excel_writer.close()

