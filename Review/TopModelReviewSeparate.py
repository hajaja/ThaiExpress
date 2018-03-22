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
listCriterion = list(set(listCriterion).difference(set(['Secu', 'strModelName', 'strCloseAtDayEnd', 'NWeekStart', 'NMonthStart', 
'strMethodTrend', 'decimalStoploss'])))
print listCriterion

# read all pickle
strDirModelReview = '%s/%s/'%(Utils.dirResultPerCase, strParamSweep)
listDailyReturn = []
listPortConfig = []
listMetric = []
listDictCriterion = []
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
#for NDayShiftModelReview in range(0, 100, 10):
#for NDayShiftModelReview in range(0, 20, 10):
for NDayShiftModelReview in range(-20, 20+10, 10):
#for NDayShiftModelReview in [0, 10]:
    listResult = []
    #for NMonthModelReview in [3, 6, 12]:
    for NMonthModelReview in [6, 12]:
    #for NMonthModelReview in [3]:
        for NMonthLookBack in [12+12, 24+12, 36+12, 48+12, 60+12]:
        #for NMonthLookBack in [6, 12]:
        #for NMonthLookBack in [36+12, 60+12]:
            print NDayShiftModelReview, NMonthLookBack, NMonthModelReview
            sDateReview = pd.date_range(dtReviewStart - datetime.timedelta(NMonthModelReview * 31), datetime.datetime.now(), freq='%dM'%NMonthModelReview)
            sDateReview = sDateReview + datetime.timedelta(NDayShiftModelReview, 0)
            sDateReview = sDateReview[sDateReview < dtDataEnd]
            sDateReview = sDateReview.tolist()
            sDateReview.append(dtDataEnd)
            # iterate all case and determine which case to use at dtModelReview
            sDailyConcat = pd.Series()
            #listTuple = []
            for dtModelReview in sDateReview:
                dtModelReviewEnd = min(dtModelReview, dtDataEnd)
                dtModelReviewStart = dtModelReview - datetime.timedelta(30 * NMonthLookBack)
                # concat
                if dtModelReviewEnd == sDateReview[0]:
                    pass
                else:
                    #listCase = dfPerf.ix[dfPerf[strColumnToSelect].argmax(), 'listCase']
                    dfQualified = dfCasePerformance.copy()
                    for k in listCriterion:
                        v = dictCriterion[k]
                        dfQualified = dfQualified[dfQualified[k]==v]
                    listCase = dfQualified['listStrCase'].values.tolist()[0]
                        
                    NSubStrategy = len(listCase)
                    sDailyPart = dfAllCase[listCase].mean(1)
                    sDailyPart = sDailyPart[(sDailyPart.index>=dtModelReviewEndPrev) & (sDailyPart.index<dtModelReviewEnd)]
                    #tupleParam = dfPerf[strColumnToSelect].argmax()
                    #tupleParam = tupleParam + (NSubStrategy, )
                    #listTuple.append(tupleParam)

                    sDailyConcat = sDailyConcat.append(sDailyPart)
    
                # calculate performance in this period
                listCasePerformance = []
                NDayMean = 5
                dfPortConfig = dfPortConfig.reset_index().set_index(listCriterion).sort_index()
                for ix in dfPortConfig.index.unique():
                    dfPortConfigOneCase = dfPortConfig.loc[ix]
                    if type(dfPortConfigOneCase) is pd.Series:
                        listStrCase = [dfPortConfigOneCase['name']]
                    else:
                        listStrCase = dfPortConfigOneCase['name'].values.tolist()
                    sCase=dfAllCase[listStrCase].mean(1)
                    sPart = sCase[(sCase.index>=dtModelReviewStart) & (sCase.index<dtModelReviewEnd)]
                    dictMetric = Utils.funcMetric(sPart)
                    #if sPart.index.size > 2*NDayMean:
                    #    sPartValue = (1+sPart).cumprod()
                    #    dictMetric['Ret'] = sPartValue.ix[-NDayMean:].mean() / sPartValue.ix[:NDayMean].mean() - 1
                    #    dictMetric['CR'] = dictMetric['Ret'] / dictMetric['DD']
                    #    dictMetric['SR'] = dictMetric['Ret'] / dictMetric['Vol']
                    dictOne = dict(dictMetric)
                    dictOne['listStrCase'] = listStrCase
                    dictOne['name'] = str(ix)
                    dictOne['sPCTDaily'] = sPart
                    for nCriterion in range(0, len(listCriterion)):
                        criterion = listCriterion[nCriterion]
                        dictOne[criterion] = ix[nCriterion]
                    
                    listCasePerformance.append(dictOne)
                dfCasePerformance = pd.DataFrame(listCasePerformance).set_index('name')
                #dfCasePerformance = dfCasePerformance[(dfCasePerformance['NDayTrain']==40)&(dfCasePerformance['NDayTest']==40)]

                '''
                for strCase in dfAllCase.columns:
                    dictOne = dfPortConfig.ix[strCase].to_dict()
                    # calculate metric
                    sCase = dfAllCase[strCase]
                    sPart = sCase[(sCase.index>=dtModelReviewStart) & (sCase.index<dtModelReviewEnd)]
                    dictMetric = Utils.funcMetric(sPart)
                    #if sPart.index.size > 2*NDayMean:
                    #    sPartValue = (1+sPart).cumprod()
                    #    dictMetric['Ret'] = sPartValue.ix[-NDayMean:].mean() / sPartValue.ix[:NDayMean].mean() - 1
                    #    dictMetric['CR'] = dictMetric['Ret'] / dictMetric['DD']
                    #    dictMetric['SR'] = dictMetric['Ret'] / dictMetric['Vol']
                    dictOne.update(dictMetric)
                    dictOne['name'] = strCase
                    dictOne['sPCTDaily'] = sPart
                    
                    listCasePerformance.append(dictOne)
                dfCasePerformance = pd.DataFrame(listCasePerformance).set_index('name')
                '''

                # which is the best param set
                dictCriterion = {}
                for criterion in listCriterion:
                    sCR = dfCasePerformance.groupby(criterion)[strColumnToSelect].median()
                    dictCriterion[criterion] = sCR.argmax()
                dictCriterion['NDayShiftModelReview'] = NDayShiftModelReview
                dictCriterion['NMonthModelReview'] = NMonthModelReview
                dictCriterion['NMonthLookBack'] = NMonthLookBack
                dictCriterion['dtModelReviewEnd'] = dtModelReviewEnd
                listDictCriterion.append(dictCriterion)

                # update dtModelReviewEndPrev
                dtModelReviewEndPrev = dtModelReviewEnd
            # 
            sDailyConcat = sDailyConcat[sDailyConcat.index > dtReviewStart]
            sDailyConcat = sDailyConcat[:-1]
            
            dictMetric = Utils.funcMetric(sDailyConcat)
            dictMetric['NMonthModelReview'] = NMonthModelReview
            dictMetric['NMonthLookBack'] = NMonthLookBack
            dictMetric['sDailyReturn'] = sDailyConcat
            listResult.append(dictMetric)

    dfResult = pd.DataFrame(listResult)
    dfResult['NDayShiftModelReview'] = NDayShiftModelReview
    dfResultAll = dfResultAll.append(dfResult)
dfResultAll = dfResultAll.reset_index()

# find the best param set
strReviewSetOption = 'WorstInBestReviewSet'
#strReviewSetOption = 'BestInSample'
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
elif strReviewSetOption == 'BestInBestReviewSet':
    sDailyReturnBest = dfResultAll.ix[dfResultAll[strColumnToSelect].argmax(), 'sDailyReturn']
elif strReviewSetOption == 'WorstInBestReviewSet':
    sCRAllSet = dfResultAll.groupby(['NMonthLookBack', 'NMonthModelReview'])[strColumnToSelect].median()
    ixReviewSet = sCRAllSet.argmax()
    dfResultBestSet = dfResultAll.set_index(['NMonthLookBack', 'NMonthModelReview']).ix[ixReviewSet]
    dfResultBestSet = dfResultBestSet.reset_index()
    sDailyReturnBest = dfResultBestSet.ix[dfResultBestSet[strColumnToSelect].argmin(), 'sDailyReturn']

sDailyReturnBest = sDailyReturnBest[sDailyReturnBest.index > dtReviewStart]
sMetricPerYear = sDailyReturnBest.groupby(sDailyReturnBest.index.year).apply(Utils.funcMetric)
sMetricPerYear.name = strColumnToSelect
dictMetricBest = Utils.funcMetric(sDailyReturnBest)
print dictMetricBest

# is the best param set (NMonthLookBack, NMonthModelReview) stable or not ?
dfResultAll = dfResultAll.reset_index().set_index(['NMonthModelReview', 'NMonthLookBack', 'NDayShiftModelReview'])
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

# show how to select NMonthModelReview, NMonthLookBack
listReviewParameter = ['NMonthModelReview', 'NMonthLookBack']
gg = dfResultAll.groupby(listReviewParameter)['CR']
ixMin = gg.idxmin()
ixMax = gg.idxmax()
listSMin = []
listSMax = []
for strFactor in ['Ret', 'Vol', 'DD', 'SR', 'CR']:
    sMin = dfResultAll.ix[ixMin, strFactor].reset_index().set_index(listReviewParameter)[strFactor].sort_index()
    sMax = dfResultAll.ix[ixMax, strFactor].reset_index().set_index(listReviewParameter)[strFactor].sort_index()
    sMin.name = strFactor
    sMax.name = strFactor
    listSMin.append(sMin)
    listSMax.append(sMax)
dfReviewParamMin = pd.concat(listSMin, axis=1)
dfReviewParamMax = pd.concat(listSMax, axis=1)

# which (NDayTrain, NDayTest) are selected at each model review point
dfCriterion = pd.DataFrame(listDictCriterion).set_index(['NMonthModelReview', 'NMonthLookBack', 'NDayShiftModelReview']).sort_index()
dfCriterionMin = dfCriterion.ix[ixMin].reset_index().set_index(listReviewParameter + ['dtModelReviewEnd']).sort_index()
dfCriterionMax = dfCriterion.ix[ixMax].reset_index().set_index(listReviewParameter + ['dtModelReviewEnd']).sort_index()

# out
strFileAddress = '%s_%s.xlsx'%(strParamSweep, strColumnToSelect)
excel_writer = pd.ExcelWriter(strFileAddress, date_format='YYYYMMDD')
Utils.funcWriteExcel(pd.DataFrame(pd.Series(dictMetricBest)), excel_writer, sheet_name='MetricOf%s'%strReviewSetOption)
Utils.funcWriteExcel(pd.DataFrame(sDailyReturnBest), excel_writer, sheet_name='DailyReturn%s'%strReviewSetOption)

Utils.funcWriteExcel(dfReviewParamMin, excel_writer, sheet_name='ReviewParam_MinCR')
Utils.funcWriteExcel(dfReviewParamMax, excel_writer, sheet_name='ReviewParam_MaxCR')
Utils.funcWriteExcel(dfCriterionMin, excel_writer, sheet_name='ParamAtEachReview_MinCR')
Utils.funcWriteExcel(dfCriterionMax, excel_writer, sheet_name='ParamAtEachReview_MaxCR')

# groupby NDayShift, NthFriday
df = dfMetric.groupby(np.sort(listCriterion).tolist())[['CR']].mean()
Utils.funcWriteExcel(df, excel_writer, sheet_name=strParamSweep)

excel_writer.close()


