import pandas as pd
import datetime
import numpy as np
import pdb
import scipy.io as sio
import os, re, pdb
import random
from scipy.signal import butter, lfilter, freqz
from scipy import signal
from dateutil.parser import parse

import ThaiExpress.Common.Utils as Utils
reload(Utils)

############################################################
# calculate weighted port performance
############################################################
def funcShowStrategyPortSum(
        dictPerTopPort,
        listDictDataSpec, 
        seriesDTRebalance, 
        strMethodVolatility, 
        NDayVolatilityLookBack, 
        dtBackTestStart, 
        #dtBackTestEnd, 
        strFileAddress
    ):
    
    # extract the selected data
    print 'read data'
    listSeriesClose = []
    listSeriesReturn = []
    listSeriesPosition = []
    listSeriesIndicator = []
    listSeriesStoploss = []
    listSeriesReturnStoploss = []
    listStoplossPrice = []
    listStoplossPriceTomorrow = []
    for dictDataSpec in listDictDataSpec:
        strategy = dictDataSpec['strategy']
        if dictDataSpec['Secu'] not in Utils.listSecuAll:
            continue
        print dictDataSpec['strCase']
        
        seriesVolatility = strategy.df['indicator'].apply(lambda x: np.nan)
        seriesPosition = strategy.df['indicator'].apply(lambda x: np.nan)
        dfAll = strategy.df
        listDTTestStart = seriesDTRebalance

        seriesReturn = strategy.seriesReturnPCTHoldDaily
        seriesReturn.name = dictDataSpec['Secu']
        listSeriesReturn.append(seriesReturn)

        seriesClose = dfAll['Close']
        seriesClose.name = dictDataSpec['Secu']
        listSeriesClose.append(dfAll['Close'].copy())
        
        seriesIndicator = strategy.df['indicator'].copy()
        seriesIndicator.name = dictDataSpec['Secu']
        listSeriesIndicator.append(seriesIndicator)

        # stoploss price
        if 'Stoploss' in strategy.df.columns:
            series = strategy.df['Stoploss']
            series.name = dictDataSpec['Secu']
            listSeriesStoploss.append(series)
            series = strategy.df['returnStoploss']
            series.name = dictDataSpec['Secu']
            listSeriesReturnStoploss.append(series)
            seriesStoplossPrice = strategy.df['StoplossPrice'].copy()
            seriesStoplossPrice.name = dictDataSpec['Secu']
            listStoplossPrice.append(seriesStoplossPrice)
            series = strategy.df['StoplossPriceTomorrow']
            series.name = dictDataSpec['Secu']
            listStoplossPriceTomorrow.append(series)
            boolStoploss = True
        else:
            boolStoploss = False

    # concat the extracted data
    print 'concat data'
    dfClose = pd.concat(listSeriesClose, axis=1).fillna(0)
    dfClose.index = dfClose.index.to_datetime()
    dfClose.index.name = 'dtEnd'
    
    dfReturn = pd.concat(listSeriesReturn, axis=1).fillna(0)
    dfReturn.index = dfReturn.index.to_datetime()
    dfReturn.index.name = 'dtEnd'
    dfReturn.ix[dfReturn.index[-1], dfReturn.columns] = 0

    dfIndicator = pd.concat(listSeriesIndicator, axis=1)    # indicator everyday
    
    dfIndicatorAtAction = dfIndicator.ix[seriesDTRebalance] # when to change direction

    # calculate volatility, and replace 0 return in dfReturn with np.nan
    print 'calculate volatility data'
    listDTTestStart = seriesDTRebalance.tolist()
    listDictVolatility = []
    dfReturnValid = pd.DataFrame()
    for nWindow in range(0, len(listDTTestStart)) :
        listReturn = []
        for SecuCode in dfReturn.columns:
            sDailyReturnAll = dfReturn[SecuCode]
            sDailyCloseAll = dfClose[SecuCode]
            
            # get the train window and test window
            dtTestStart = listDTTestStart[nWindow]
            dtTrainEnd = dtTestStart
            if nWindow == 0:
                continue
            else:
                dtTrainStart = listDTTestStart[nWindow-1]

            # is there any trade 
            sDailyReturn = sDailyReturnAll[(sDailyReturnAll.index>=dtTrainStart) & (sDailyReturnAll.index<dtTrainEnd)].copy()
            sDailyClose = sDailyCloseAll[(sDailyCloseAll.index>=dtTrainStart) & (sDailyCloseAll.index<dtTrainEnd)].copy()
            if sDailyReturn.empty:
                continue

            # calculate volatility & weight
            volatility = sDailyClose.pct_change().std() * np.sqrt(Utils.NTradingDayPerYear)
            weight = 0.1 / volatility
            #weight = 0.03 / volatility
            #weight = 0.1
            
            if sDailyReturn.sum() == 0:
                # not in the top 20% of XSM
                sDailyReturn = sDailyReturn.apply(lambda x: np.nan)

            listReturn.append(sDailyReturn)
            listDictVolatility.append({'dtTestStart': dtTestStart, 'SecuCode':SecuCode, 'volatility': volatility, 'weight': weight})

        if len(listReturn) == 0:
            continue
        else:
            dfReturnValid = dfReturnValid.append(pd.concat(listReturn, axis=1))
    
    # append tomorrow index to ReturnValid 
    ######
    # diff between dfReturnValid and dfReturn
    # when there is no position for a secu, dfReturnValid is 0 * dfReturn
    ######
    print 'calculate daily weight'
    dfReturnValid = dfReturnValid.append(dfIndicator.ix[-1].replace(0, np.nan))

    # position of every day
    dfVolatility = pd.DataFrame(listDictVolatility)
    dfVolatility = dfVolatility.set_index('dtTestStart').sort_index()
    dfWeightOriginal = dfVolatility.reset_index().pivot(index='dtTestStart', columns='SecuCode', values='weight')
    dfWeight = dfWeightOriginal.ix[dfReturnValid.index].ffill()
    dfWeight.index.name = dfReturnValid.index.name
    
    # adjust weight to meet leverage limit
    dfSelected = dfReturnValid.applymap(lambda x: ~np.isnan(x)).astype(np.int)
    dfWeightValid = dfWeight * dfSelected
    positionUpper = Utils.LEVERAGE
    sTotalWeight = dfWeightValid.sum(1)
    sTotalWeight.to_csv('TotalWeight.csv')
    for Secu in dfWeightValid.columns:
        dfWeightValid[Secu] = dfWeightValid[Secu] / sTotalWeight * positionUpper 
        #dfWeightValid[Secu] = dfWeightValid[Secu] / sTotalWeight * sTotalWeight.apply(lambda x: min(x, 5))
        dfWeightValid[Secu] = dfWeightValid[Secu].apply(lambda x: min(x, Utils.UpperPositionSingleContract))
        pass
    
    # if some secu's weight is smaller than one contract, then do not trade
    TotalMoney = Utils.TOTALMONEY 
    dfPositionDollar = dfWeightValid * TotalMoney
    listPositionContract = []
    listPositionDecimal = []
    for SecuCode in dfPositionDollar.columns:
        listColumn = ['CloseRaw', 'SettleRaw']
        dfSecu = Utils.getDFOneProduct(SecuCode, listColumn)
        sPositionDollar = dfPositionDollar[SecuCode]
        #sSettleRaw = Utils.dfExe.ix[SecuCode].ix[sPositionDollar.index, 'SettleRaw'].shift(1)
        sSettleRaw = dfSecu.ix[sPositionDollar.index, 'SettleRaw'].shift(1)
        sSettleRaw.name = SecuCode
        sNominalContract = sSettleRaw * Utils.dfDetail.ix[SecuCode, 'multiplier']
        sMarginContract = sSettleRaw * Utils.dfDetail.ix[SecuCode, 'multiplier'] * Utils.dfDetail.ix[SecuCode, 'margin']
        sPositionContract = sPositionDollar / sNominalContract
        #sPositionContract = sPositionContract.apply(lambda x: np.round(x))
        def funcRound(x):
            if x < 0.3:
                return 0
            elif x >= 0.3 and x <= 0.5:
                return 1
            else:
                return np.round(x)
        sPositionContract = sPositionContract.apply(lambda x: np.round(x))
        #sPositionContract = sPositionContract.apply(lambda x: funcRound(x))
        sPositionDecimal = sPositionContract * sNominalContract / TotalMoney
        sPositionContract.name = SecuCode
        sPositionDecimal.name = SecuCode
        listPositionContract.append(sPositionContract)
        listPositionDecimal.append(sPositionDecimal)
    dfPositionContract = pd.concat(listPositionContract, axis=1)
    dfPositionDirection = (dfIndicatorAtAction*dfPositionContract).ffill()
    dfPosition = pd.concat(listPositionDecimal, axis=1)
    dfWeightValid = dfPosition

    # daily position
    seriesDailyPosition = dfWeightValid.sum(axis=1)
    seriesDailyPosition = seriesDailyPosition[seriesDailyPosition.index >= dtBackTestStart]
    seriesDailyPosition.name = 'Position'
    
    # calculate daily dollar return, daily margin
    '''
    print 'calculate daily return'
    TotalMoney = Utils.TOTALMONEY 
    dfPositionDollar = dfWeightValid * TotalMoney
    listDollarReturnDaily = []
    listDollarMarginDaily = []
    for SecuCode in dfPositionDirection.columns:
        listColumn = ['SettleRaw', 'Delta']
        dfSecu = Utils.getDFOneProduct(SecuCode, listColumn)
        sPositionContract = dfPositionDirection[SecuCode]
        #sSettleRaw = Utils.dfExe.ix[SecuCode].ix[sPositionContract.index, 'SettleRaw']
        sSettleRaw = dfSecu.ix[sPositionContract.index, 'SettleRaw']
        sSettleRaw.name = SecuCode
        #sCloseDelta = Utils.dfExe.ix[SecuCode].ix[sPositionContract.index, 'Delta']
        sCloseDelta = dfSecu.ix[sPositionContract.index, 'Delta']
        sCloseDelta.name = SecuCode
        sDollarReturnDaily = sPositionContract * Utils.dfDetail.ix[SecuCode, 'multiplier'] * sCloseDelta
        sDollarMarginDaily = sPositionContract.abs() * Utils.dfDetail.ix[SecuCode, 'multiplier'] * sSettleRaw * Utils.dfDetail.ix[SecuCode, 'margin']
        listDollarReturnDaily.append(sDollarReturnDaily)
        listDollarMarginDaily.append(sDollarMarginDaily)
    dfDollarReturnDaily = pd.concat(listDollarReturnDaily, axis=1)
    dfDollarMarginDaily = pd.concat(listDollarMarginDaily, axis=1)
    dfDollarReturnDaily['Total'] = dfDollarReturnDaily.sum(1)
    dfDollarMarginDaily['Total'] = dfDollarMarginDaily.sum(1)
    
    # consider the commission fee for position adjustment
    dfAdjustPosition = (dfIndicatorAtAction != 0) & (dfIndicatorAtAction == dfIndicatorAtAction.shift(1))
    listColumn = dfAdjustPosition.columns
    indexCommon = dfAdjustPosition.index & dfReturnValid.index
    dfWeightDelta = dfWeightValid - dfWeightValid.shift(1)
    dfWeightDelta = dfWeightDelta.applymap(lambda x: max(0, x))
    dfReturnValid.ix[indexCommon, listColumn] = dfReturnValid.ix[indexCommon, listColumn] - Utils.COMMISSION * 2 * dfAdjustPosition.ix[indexCommon, listColumn].astype(np.float) * dfWeightDelta.ix[indexCommon, listColumn]

    dfWeightValid = (dfIndicatorAtAction.apply(lambda x: abs(x)) * dfWeightValid).ffill()   # added in 20170929, vital error, although not affecting result a lot.
    dfReturnWeighted = dfReturnValid * dfWeightValid

    # daily dollar return recovered from decimal simulation
    dfDollarReturnDailyRecovered = dfReturnWeighted * Utils.TOTALMONEY
    dfDollarReturnDailyRecovered = dfDollarReturnDailyRecovered.fillna(0).astype(int)
    dfDollarReturnDailyRecovered['Total'] = dfDollarReturnDailyRecovered.sum(1)

    # calculate stat
    seriesDailyReturn = dfReturnWeighted.sum(axis=1)
    seriesDailyReturn = seriesDailyReturn[seriesDailyReturn.index >= dtBackTestStart]
    seriesDailyReturn = seriesDailyReturn
    #'''

    # calculate daily return as in WH
    print 'calculate daily return WH'
    listEquityDeltaDaily = []
    for SecuCode in dfPositionDirection.columns:
        listColumn = ['OpenRaw', 'CloseRaw', 'SettleRaw', 'DeltaSettle', 'Open-PreSettle', 'Settle-Open']
        #listColumn = ['OpenRaw', 'CloseRaw', 'SettleRaw']
        dfSecu = Utils.getDFOneProduct(SecuCode, listColumn)
        #dfSecu['DeltaSettle'] = dfSecu['SettleRaw'].diff()
        #dfSecu['PreSettle'] = dfSecu['SettleRaw'].shift(1)
        #dfSecu['Open-PreSettle'] = dfSecu['OpenRaw'] - dfSecu['PreSettle']
        #dfSecu['Settle-Open'] = dfSecu['SettleRaw'] - dfSecu['OpenRaw']

        #dfTrading = dfExe_Friday.ix[SecuCode][['OpenRaw', 'CloseRaw', 'SettleRaw', 'DeltaSettle', 'Open-PreSettle', 'Settle-Open']]
        dfTrading = dfSecu
        dfTrading = dfTrading.rename({'OpenRaw':'Open', 'CloseRaw':'Close', 'SettleRaw':'Settle'})
        # contracts in and out
        sPositionDirection = dfPositionDirection[SecuCode]
        sPositionDirectionDiff = sPositionDirection.diff()
        sPositionIn = sPositionDirectionDiff.copy()
        sPositionOut = -sPositionDirectionDiff.copy()
        sPositionIn.ix[sPositionDirection.abs() < sPositionDirection.shift(1).abs()] = 0
        sPositionOut.ix[sPositionDirection.abs() > sPositionDirection.shift(1).abs()]  = 0

        # calculate daily equity delta
        sEquity = sPositionDirection * dfTrading['DeltaSettle'] - sPositionIn * dfTrading['Open-PreSettle'] + sPositionOut * dfTrading['Open-PreSettle']
        sEquity = sEquity - (sPositionIn.abs() + sPositionOut.abs()) * dfTrading['OpenRaw'] * Utils.COMMISSION
        sEquity = sEquity * Utils.dfDetail.ix[SecuCode, 'multiplier']
        sEquity.name = SecuCode
        listEquityDeltaDaily.append(sEquity)

    dfEquityDeltaDaily = pd.concat(listEquityDeltaDaily, 1)
    dfEquityDeltaDaily['Total'] = dfEquityDeltaDaily.sum(1)
    
    # calculate daily equity PCT
    ixCommon = dfEquityDeltaDaily.index.intersection(seriesDTRebalance)
    for dt in ixCommon:
        dfEquityDeltaDaily.ix[dt, 'NDTRebalance'] = dt.strftime('%Y%m%d')
    dfEquityDeltaDaily['NDTRebalance'] = dfEquityDeltaDaily['NDTRebalance'].ffill()
    def funcCalcEquityPCT(sEquity):
        sEquity = sEquity.cumsum() + Utils.TOTALMONEY
        sPCT = sEquity.pct_change()
        sPCT.ix[0] = sEquity.ix[0] / Utils.TOTALMONEY - 1
        return sPCT
    sEquityPCT = dfEquityDeltaDaily.groupby('NDTRebalance')['Total'].apply(funcCalcEquityPCT)
    seriesDailyReturn = sEquityPCT
    seriesDailyReturn.index.name = 'dtEnd'
    
    ### output for check
    excel_writer = pd.ExcelWriter(strFileAddress + 'Check.xlsx')
    Utils.funcWriteExcel(dfVolatility, excel_writer, 'Volatility')
    Utils.funcWriteExcel(dfReturnValid, excel_writer, 'ReturnValid')
    Utils.funcWriteExcel(dfWeightValid * dfIndicator, excel_writer, 'WeightDirection')
    Utils.funcWriteExcel(dfPositionDirection, excel_writer, 'PositionDirection')
    Utils.funcWriteExcel(pd.DataFrame(seriesDTRebalance), excel_writer, 'DTRebalance')
    Utils.funcWriteExcel(pd.DataFrame(seriesDailyReturn), excel_writer, 'DailyReturn')
    Utils.funcWriteExcel(pd.DataFrame(sEquityPCT), excel_writer, 'sEquityPCT')
    Utils.funcWriteExcel(dfEquityDeltaDaily, excel_writer, 'EquityDeltaDaily')

    #Utils.funcWriteExcel(dfDollarReturnDaily, excel_writer, 'DollarReturnDaily')
    #Utils.funcWriteExcel(dfDollarMarginDaily, excel_writer, 'DollarMarginDaily')
    #Utils.funcWriteExcel(dfDollarReturnDailyRecovered, excel_writer, 'DollarReturnDailyRecovered')
    #Utils.funcWriteExcel((dfDollarReturnDailyRecovered-dfDollarReturnDaily), excel_writer, 'DollarReturnDiff')

    if boolStoploss:
        Utils.funcWriteExcel(pd.concat(listSeriesStoploss, axis=1), excel_writer, 'Stoploss')
        Utils.funcWriteExcel(pd.concat(listSeriesReturnStoploss, axis=1), excel_writer, 'ReturnStoploss')
        Utils.funcWriteExcel(pd.concat(listStoplossPrice, axis=1), excel_writer, 'StoplossPrice')
        Utils.funcWriteExcel(pd.concat(listStoplossPriceTomorrow, axis=1), excel_writer, 'StoplossPriceTomorrow')

    #dfContractCode = Utils.dfExe['ContractCode'].reset_index().pivot_table(columns='SecuCode', index='TradingDay', values='ContractCode', aggfunc=lambda x: ' '.join(x))
    dfContractCode = Utils.getTableExe('ContractCode')
    Utils.funcWriteExcel(dfContractCode, excel_writer, sheet_name='ContractCodeFullRename')

    #dfContractCodeTomorrow = Utils.dfExe['ContractCodeTomorrow'].reset_index().pivot_table(columns='SecuCode', index='TradingDay', values='ContractCodeTomorrow', aggfunc=lambda x: ' '.join(x))
    dfContractCodeTomorrow = Utils.getTableExe('ContractCodeTomorrow')
    Utils.funcWriteExcel(dfContractCodeTomorrow, excel_writer, sheet_name='ContractCodeTomorrow')

    #dfDominantChange = Utils.dfExe['boolDominantChange'].reset_index().pivot_table(columns='SecuCode', index='TradingDay', values='boolDominantChange')
    dfDominantChange = Utils.getTableExe('boolDominantChange')
    Utils.funcWriteExcel(dfDominantChange, excel_writer, sheet_name='DominantChange')

    #dfDominantChangeTomorrow = Utils.dfExe['boolDominantChangeTomorrow'].reset_index().pivot_table(columns='SecuCode', index='TradingDay', values='boolDominantChangeTomorrow')
    dfDominantChangeTomorrow = Utils.getTableExe('boolDominantChangeTomorrow')
    Utils.funcWriteExcel(dfDominantChangeTomorrow, excel_writer, sheet_name='DominantChangeTomorrow')

    # freeze panes
    for sheet in excel_writer.sheets:
        excel_writer.sheets[sheet].freeze_panes(1, 1)
    excel_writer.close()

    # dfOut
    seriesDailyValue = (1 + seriesDailyReturn).cumprod()
    seriesDailyValue.name = 'Cum Return'
    seriesMaxValue = seriesDailyValue.expanding().max()
    seriesMaxDD = (seriesMaxValue - seriesDailyValue) / seriesMaxValue
    seriesMaxDD.name = 'Max DD'
    dfOut = pd.concat([seriesDailyValue-1, seriesMaxDD, seriesDailyPosition], axis=1)

    # MySQL
    if Utils.boolUsingDB:
        dictDataSpec = Utils.extractElementFromList(dictPerTopPort)
        listToRemove = ['strCloseAtDayEnd', 'strCase', 'Secu', 'listFileAddress', 'strMethodTrend', 'strModelName']
        listParam = list(set(dictPerTopPort.keys()).difference(set(listToRemove)))

        # dfPositionDirection
        df = dfPositionDirection.stack()
        df = df.reset_index()
        df.columns = ['trade_date', 'code', 'openInterest']
        for strColumn in listParam:
            if strColumn in dictDataSpec.keys():
                df[strColumn] = dictDataSpec[strColumn]
        strTB = dictDataSpec['strModelName']
        strTB = Utils.UtilsDB.strTBPrefixPosition + strTB
        listColumnIndex = ['trade_date', 'code'] + listParam
        Utils.UtilsDB.saveTB_DAILY(Utils.UtilsDB.DB_NAME_POSITION, strTB, df, listColumnIndex)

        # sDailyReturn
        df = pd.concat([seriesDailyReturn, seriesDailyPosition], axis=1)
        df = df.reset_index()
        df.columns = ['trade_date', 'PCT', 'Position']
        df['code'] = dictDataSpec['strModelName']
        for strColumn in listParam:
            if strColumn in dictDataSpec.keys():
                df[strColumn] = dictDataSpec[strColumn]
            else:
                df[strColumn] = np.nan
        strTB = dictDataSpec['strModelName']
        strTB = Utils.UtilsDB.strTBPrefixPerformance + strTB
        listColumnIndex = ['trade_date', 'code'] + listParam
        Utils.UtilsDB.saveTB_DAILY(Utils.UtilsDB.DB_NAME_PERFORMANCE, strTB, df, listColumnIndex)

    #if Utils.boolUsingDB:
    #    # dfPositionDirection
    #    df = dfPositionDirection.stack()
    #    df = df.reset_index()
    #    df.columns = ['trade_date', 'code', 'openInterest']
    #    listToRemove = ['strCloseAtDayEnd', 'strCase', 'Secu', 'listFileAddress', 'strMethodTrend', 'strModelName']
    #    listParam = list(set(dictPerTopPort.keys()).difference(set(listToRemove)))

    #    for strColumn in listParam:
    #        if strColumn in dictDataSpec.keys():
    #            df[strColumn] = dictDataSpec[strColumn]
    #    strTB = dictDataSpec['strModelName']
    #    strTB = Utils.UtilsDB.strTBPrefixPosition + strTB
    #    listColumnIndex = ['trade_date', 'code'] + listParam
    #    #Utils.UtilsDB.saveTB_DAILY(Utils.UtilsDB.DB_NAME_POSITION, strTB, df, listColumnIndex)

    #    # sDailyReturn
    #    df = pd.concat([seriesDailyReturn, seriesDailyPosition], axis=1)
    #    df = df.reset_index()
    #    df.columns = ['trade_date', 'PCT', 'Position']
    #    df['code'] = dictDataSpec['strModelName']
    #    for strColumn in listParam:
    #        if strColumn in dictDataSpec.keys():
    #            df[strColumn] = dictDataSpec[strColumn]
    #        else:
    #            df[strColumn] = np.nan
    #    strTB = dictDataSpec['strModelName']
    #    strTB = Utils.UtilsDB.strTBPrefixPerformance + strTB
    #    listIndexColumn = ['trade_date', 'code'] + listParam
    #    Utils.UtilsDB.saveTB_DAILY(Utils.UtilsDB.DB_NAME_PERFORMANCE, strTB, df, listColumnIndex)

    return dfOut

