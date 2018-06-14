#coding=utf8
import pandas as pd
import datetime
import numpy as np
import os, re, sys, shutil, logging

import ThaiExpress.Common.Utils as Utils
reload(Utils)
import UtilsPortAsync
reload(UtilsPortAsync)

def funcTopPort(strParamSweep):
    # dtBackTestStart & dtBackTestEnd
    dtBackTestStart = Utils.dtBackTestStart
    #dtBackTestEnd = Utils.dtBackTestEnd

    ######################################################### 
    # read backtest result of rquired cases
    ######################################################### 
    # read stored results
    listDictPerTopPort = Utils.getFileAddressForTopPort(strParamSweep)
    
    # read dfPCTAll, dfVolatilityAll
    print 'reading data'
    #dictDataSpec = dict(Utils.dictDataSpecTemplate)
    #dictDataSpecCase = listDictPerTopPort[0]
    #dictDataSpec['strModelName'] = dictDataSpecCase['strModelName'][0]
    #if 'freq' in dictDataSpecCase.keys():
    #    dictDataSpec['freq'] = dictDataSpecCase['freq'][0]
    dictDataSpec = Utils.extractElementFromList(listDictPerTopPort[0])
    listSPCT = []
    listVolatility = []
    for Secu in dictDataSpec['Secu']:
        dictDataSpec['Secu'] = Secu
        dfAll = Utils.getTradingDataPoint_Commodity(dictDataSpec).replace(np.inf, np.nan)
        dfAll = dfAll[['Open', 'High', 'Low', 'Close', 'Volume']]
        dfAll = dfAll.dropna()
        dtLastObservation = dfAll.index[-1]
        dtEnter = dtLastObservation + datetime.timedelta(1, 0)
        if dtEnter.weekday() >= 5:
            dtEnter = dtEnter + datetime.timedelta(3, 0)
        rowLast = dfAll.ix[dtLastObservation]
        rowLast.name = dtEnter
        dfAll = dfAll.append(rowLast)

        if 'PCT' not in dfAll.columns:
            dfAll['PCT'] = dfAll['Close'].pct_change()
        
        sPCT = dfAll['PCT']
        sPCT.name = Secu
        listSPCT.append(sPCT)
        
        sValueDaily = UtilsPortAsync.funcResampleDaily(dfAll['Close'])
        sVolatility = sValueDaily.pct_change().rolling(40).std().shift(1)
        sVolatility.name = Secu
        listVolatility.append(sVolatility)

    dfPCTAll = pd.concat(listSPCT, axis=1)
    dfVolatilityAll = pd.concat(listVolatility, axis=1)

    print 'iterate listDictPerTopPort'
    for dictPerTopPort in listDictPerTopPort:
        print dictPerTopPort
        ######################################################### 
        # prepare directory for saving
        ######################################################### 
        strDirParamSweep = Utils.dirResultPerCase + '/' + strParamSweep 
        if os.path.exists(strDirParamSweep) is False:
            os.mkdir(strDirParamSweep)
        strDirAddressCase = strDirParamSweep + '/' + dictPerTopPort['strCase'] + '/'
        if os.path.exists(strDirAddressCase) is False:
            os.mkdir(strDirAddressCase)
        strFileAddressPrefix = strDirAddressCase

        # save config file of the port
        dictPerTopPortOut = dict((key,value[0]) for key, value in dictPerTopPort.iteritems() if key not in ['strCase', 'listFileAddress', 'Secu'])
        pd.Series(dictPerTopPortOut).to_pickle(strFileAddressPrefix + '/' + 'portconfig.pickle')
    
        listDict = []
        dictDTEnter = {}
        dictIndicator = {}
        for strFileAddress in dictPerTopPort['listFileAddress']:
            pat = re.compile(r'.*?Secu([a-z]+\.[a-z]+).*?')
            Secu = pat.match(strFileAddress).groups()[0]
            sPCT = dfPCTAll[Secu]
            sVolatility = dfVolatilityAll[Secu]
            sVolatility.name = Secu

            # dictOne
            dictOne = dict(dictPerTopPort)
            del dictOne['strCase']
            del dictOne['Secu']
            del dictOne['listFileAddress']
            dictOneNew = {}
            for k, v in dictOne.iteritems():
                dictOneNew[k] = v[0]
            dictOne = dictOneNew
            
            ######################################################### 
            # calculate performance for each Secu
            ######################################################### 
            # read indicator
            try:
                strategy = pd.read_pickle(strFileAddress)['strategy']
            except:
                strToLog = 'unreadable file:\n%s'%strFileAddress
                logging.log(logging.ERROR, strToLog)
                continue

            sIndicator = strategy.df['indicator']
            dictIndicator[Secu] = sIndicator
            
            # calculate PCT daily, compatible with 1min back test
            sPCTHoldMin = sPCT * sIndicator
            sPCTHoldMin.name = Secu
            sValueMin = (1 + sPCTHoldMin).cumprod()
            sValueDaily = UtilsPortAsync.funcResampleDaily(sValueMin)
            sPCTDaily = sValueDaily.pct_change()

            dictMetric = Utils.funcMetric(sPCTDaily)
            dictOne.update(dictMetric)
            sPCTDaily.name = Secu

            # find index Enter to 1) calculate trading cost 2) determine position when entering
            sIndexBoth = strategy.indexLongEnter.append(strategy.indexShortEnter)
            listIndexEnter = set(sIndexBoth.date).intersection(set(sPCTDaily.dropna().index.date.tolist()))
            listIndexEnter = np.sort(list(listIndexEnter))
            dictDTEnter[Secu] = listIndexEnter

            sPCTDaily.ix[listIndexEnter] = sPCTDaily.ix[listIndexEnter] - Utils.COMMISSION * 2
            dictOne['sPCTDaily'] = sPCTDaily
            
            # determine position
            # here, the lambda x: 0.005/max(0.001, x) is to make sure any single product does not dominant the portfolio value
            sPosition = sVolatility.apply(lambda x: np.nan)
            sPosition.ix[listIndexEnter] = sVolatility.ix[listIndexEnter].apply(lambda x: 0.005/max(0.001,x))
            sPosition = sPosition.sort_index()
            sPosition = sPosition.ffill()
            sPCTDaily = sPCTDaily * sPosition
            sPosition.name = Secu
    
            # no position when sIndicator=0
            dictOne['sPosition'] = sPosition
            dictOne['sVolatility'] = sVolatility
            dictOne['listIndexEnter'] = listIndexEnter

            # 
            listDict.append(dictOne)
        
        # below are the dataframe for all products in a case
        dfResult = pd.DataFrame(listDict)
        dfPCT = pd.concat(dfResult['sPCTDaily'].values.tolist(), axis=1)
        dfPosition = pd.concat(dfResult['sPosition'].values.tolist(), axis=1)
        
        ######################################################### 
        # for interday strategy, only consider the daily return
        ######################################################### 
        if dictPerTopPort['freq'][0] != '1day':
            print 'not suitable for daily update'
            # fill na after first trading day
            for SecuCode in dfPCT.columns:
                ixFirstTradingDay = dfPCT[SecuCode].dropna().index[0]
                dfPCT.ix[(dfPCT.index>=ixFirstTradingDay)&(dfPCT[SecuCode].isnull()), SecuCode] = 0

            # adjust position
            sNSecu = Utils.generateNSecu()
            sNSecu = sNSecu.apply(lambda x: np.sqrt(x)) * 2
            for strSecu in dfPosition.columns:
                dfPosition[strSecu] = dfPosition[strSecu] / sNSecu

            # calculate daily return
            seriesDailyReturn = (dfPCT.fillna(0) * dfPosition).sum(1)
            seriesDailyPosition = dfPosition.mean(1)
            
            dictDataSpec = Utils.extractElementFromList(dictPerTopPort)
            listToRemove = ['strCloseAtDayEnd', 'strCase', 'Secu', 'listFileAddress', 'strMethodTrend', 'strModelName']
            listParam = list(set(dictPerTopPort.keys()).difference(set(listToRemove)))
            
            # push to database
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

            continue

        ######################################################### 
        # adjust the position according to the number of all products available
        ######################################################### 
        sNSecuRaw = Utils.generateNSecu()
        sNSecu = sNSecuRaw
        sNSecu = sNSecuRaw.apply(lambda x: np.sqrt(x)) * 2

        # Faith Position
        for strSecu in dfPosition.columns:
            dfPosition[strSecu] = dfPosition[strSecu] / sNSecu
            pass
        seriesDailyPosition = dfPosition.sum(1)
        seriesDailyPosition.name = 'Position'
        
        ######################################################### 
        # calculate number of contracts for a product
        ######################################################### 
        # if some secu's weight is smaller than one contract, then do not trade
        TotalMoney = Utils.TOTALMONEY 
        dfPositionDollar = dfPosition * TotalMoney
        listPositionContract = []
        listPositionDirection = []
        listPositionDecimal = []
        for SecuCode in dfPositionDollar.columns:
            sPositionDollar = dfPositionDollar[SecuCode]

            #sSettleRaw = Utils.dfExe.ix[SecuCode]['SettleRaw'].shift(1).ix[dictDTEnter[SecuCode]]
            sSettleRaw = Utils.getDFOneProduct(SecuCode, ['SettleRaw']).shift(1).ix[dictDTEnter[SecuCode]]
            sSettleRaw.name = SecuCode
            sNominalContract = sSettleRaw * Utils.dfDetail.ix[SecuCode, 'multiplier']

            sPositionContract = sPositionDollar / sNominalContract.ix[sPositionDollar.index].ffill()
            sPositionContract = sPositionContract.apply(lambda x: np.round(x))
            sPositionContract.name = SecuCode
            listPositionContract.append(sPositionContract)

            sIndicator = dictIndicator[SecuCode]
            sPositionDirection = sPositionContract * sIndicator.ix[sPositionContract.index].ffill()
            sPositionDirection.name = SecuCode
            listPositionDirection.append(sPositionDirection)

            sPositionDecimal = sPositionContract * sNominalContract / TotalMoney
            sPositionDecimal = sPositionDecimal * sIndicator.ix[sPositionDecimal.index].ffill()
            sPositionDecimal = sPositionDecimal.ffill()
            sPositionDecimal.name = SecuCode
            listPositionDecimal.append(sPositionDecimal)

        dfPositionContract = pd.concat(listPositionContract, axis=1)
        dfPositionDirection = pd.concat(listPositionDirection, axis=1)
        dfWeightDirection = pd.concat(listPositionDecimal, axis=1)
        
        # calculate daily return as in WH
        listEquityDeltaDaily = []
        for SecuCode in dfPositionDirection.columns:
            listColumn = ['OpenRaw', 'CloseRaw', 'SettleRaw', 'DeltaSettle', 'Open-PreSettle', 'Settle-Open']
            dfTrading = Utils.getDFOneProduct(SecuCode, listColumn)
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
            sEquity = sEquity * Utils.dfDetail.ix[SecuCode, 'multiplier']
            sEquity.name = SecuCode
            listEquityDeltaDaily.append(sEquity)
        dfEquityDeltaDaily = pd.concat(listEquityDeltaDaily, 1)
        dfEquityDeltaDaily['Total'] = dfEquityDeltaDaily.sum(1)

        # calculate daily equity PCT
        sEquityPCT = dfEquityDeltaDaily['Total'] / Utils.TOTALMONEY
        
        ######################################################### 
        # output
        ######################################################### 
        #'''
        # daily value & DD
        seriesDailyReturn = sEquityPCT
        sDailyReturn = sEquityPCT
        #sDailyReturn = sDailyReturn[(sDailyReturn.index >= dtBackTestStart) & (sDailyReturn.index <= dtBackTestEnd)]
        #seriesDailyPosition = seriesDailyPosition[(seriesDailyPosition.index >= dtBackTestStart) & (seriesDailyPosition.index <= dtBackTestEnd)]
        sDailyReturn = sDailyReturn[sDailyReturn.index >= dtBackTestStart]
        seriesDailyPosition = seriesDailyPosition[seriesDailyPosition.index >= dtBackTestStart]
        seriesDailyValue = (1 + sDailyReturn).cumprod()
        seriesDailyValue.name = 'Cum Return'
        seriesMaxValue = seriesDailyValue.expanding().max()
        seriesMaxDD = (seriesMaxValue - seriesDailyValue) / seriesMaxValue
        seriesMaxDD.name = 'Max DD'
        dfOut = pd.concat([seriesDailyValue-1, seriesMaxDD, seriesDailyPosition], axis=1)
        
        # plot & savefig
        dfOut = dfOut[['Cum Return', 'Max DD', 'Position']].ffill().dropna()
        if dfOut.empty:
            print 'empty dfOut'
            shutil.rmtree(strFileAddressPrefix)
            continue
        dfOut.to_pickle(strFileAddressPrefix + 'dfOut.pickle')

        # check
        strFileAddress = strFileAddressPrefix + '/Check.xlsx'
        excel_writer = pd.ExcelWriter(strFileAddress)
        Utils.funcWriteExcel(dfPositionDirection, excel_writer, 'PositionDirection')
        Utils.funcWriteExcel(dfEquityDeltaDaily, excel_writer, 'EquityDeltaDaily')
        #Utils.funcWriteExcel(dfReturnValid, excel_writer, 'ReturnValid')
        Utils.funcWriteExcel(dfWeightDirection, excel_writer, 'WeightDirection')
        #Utils.funcWriteExcel(dfDailyReturnCheck, excel_writer, 'DailyReturn')

        dfContractCode = Utils.getTableExe('ContractCode')
        Utils.funcWriteExcel(dfContractCode, excel_writer, sheet_name='ContractCode')
        
        dfContractCodeTomorrow = Utils.getTableExe('ContractCodeTomorrow')
        Utils.funcWriteExcel(dfContractCodeTomorrow, excel_writer, sheet_name='ContractCodeTomorrow')
        
        dfDominantChange = Utils.getTableExe('boolDominantChange')
        Utils.funcWriteExcel(dfDominantChange, excel_writer, sheet_name='DominantChange')
        
        dfDominantChangeTomorrow = Utils.getTableExe('boolDominantChangeTomorrow')
        Utils.funcWriteExcel(dfDominantChangeTomorrow, excel_writer, sheet_name='DominantChangeTomorrow')
        excel_writer.close()
        #'''
    
        # MySQL
        if Utils.boolUsingDB:
            #raise Exception
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

if __name__ == '__main__':
    #########################################
    # logging
    #########################################
    import logging
    logging.basicConfig(
            level = logging.INFO, 
            format = '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
            datefmt = '%m-%d %H:%M:%S',
            filename = 'Run.log',
            filemode = 'w'
            )
    if len(logging.getLogger().handlers) == 1:
        ch = logging.StreamHandler()
        logging.getLogger().addHandler(ch)
    logging.log(logging.DEBUG, 'Run starts')

    strParamSweep = sys.argv[1]
    funcTopPort(strParamSweep)

    logging.shutdown()

