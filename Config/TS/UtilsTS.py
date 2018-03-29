import pandas as pd
import datetime, os, re, sys, time, gc, logging
import numpy as np
from dateutil.parser import parse

import ThaiExpress.Common.Utils as Utils
reload(Utils)
import CommodityDataBase as CDB
reload(CDB)

#####################
# Excel
#####################
def funcPrepareDataExcel():
    # read data
    # prepare the SecuCode for CZCE
    if Utils.boolMonitor:
        strFileAddress = Utils.dirDataSource + '/' + 'dailyAll.xlsx'
    else:
        strFileAddress = Utils.dirDataSource + '/' + 'dailyAll10Y.xlsx'
    dfTrading = pd.read_excel(strFileAddress, sheetname='ALL')
    dfContract = pd.read_excel(strFileAddress, sheetname='Contract')
    print dfTrading.Date.max()
    def funcContractCode(x):
        pat = re.compile('([A-Z]+)-(\D+)(\d+)')
        dictSecu = {
                'WT': 'WH',
                'WS': 'WH',
                'ER': 'RI',
                'RO': 'OI',
                'ME': 'MA',
                'TC': 'ZC',
                }
        g = pat.match(x).groups()
        strCME = g[0]
        strSecu = g[1]
        strDeliveryYYMM = g[2]
        if strSecu in dictSecu.keys():
            strSecu = dictSecu[strSecu]
        ret = strSecu + strDeliveryYYMM
        return ret
    def funcSecuCode(x):
        pat = re.compile('([A-Z]+)-(\D+)(\d+)')
        dictSecu = {
                'WT': 'WH',
                'WS': 'WH',
                'ER': 'RI',
                'RO': 'OI',
                'ME': 'MA',
                'TC': 'ZC',
                }
        dictCME = {
                'SHFE': 'shf',
                'DCE': 'dce',
                'CZCE': 'czc',
                'CFFEX': 'cfe',
                }
        g = pat.match(x).groups()
        strCME = g[0]
        strSecu = g[1]
        strDeliveryYYMM = g[2]
        if strSecu in dictSecu.keys():
            strSecu = dictSecu[strSecu]
        ret = strSecu.lower() + '.' + dictCME[strCME]
        return ret
    dfContract['ContractCode'] = dfContract['SecuAbbrShort'].apply(funcContractCode)
    dfContract['SecuCode'] = dfContract['SecuAbbrShort'].apply(funcSecuCode)
    
    
    dfTrading = dfTrading.merge(dfContract[['SecuID', 'DeliveryDay', 'DeliveryMonth', 'ContractCode', 'SecuCode']], on='SecuID', how='inner')
    dfTrading['TradingDay'] = dfTrading['Date'].apply(lambda x: parse(str(x)))
    dfTrading['DeliveryDay'] = dfTrading['DeliveryDay'].apply(lambda x: parse(str(x)))
    #dfTrading.to_pickle('dfTrading.pickle')
    dfSimple = dfTrading[['TradingDay', 'SecuCode', 'ContractCode', 'Close', 'TurnoverVolume', 'Position', 'DeliveryDay']]
    dfSimple = dfSimple.set_index(['TradingDay', 'SecuCode'])
    dfSimple = dfSimple.sort_index()
    dfSimple.to_pickle('dfSimple.pickle')

def prepareTrendExcel():
    dfAll = pd.read_pickle('dfSimple.pickle')
    
    # calculate the TS of each SecuCode
    listDict = []
    dfAll = dfAll.dropna()
    for SecuCode in dfAll.index.get_level_values('SecuCode').unique():
        print SecuCode
        for dt in dfAll.xs(SecuCode, level='SecuCode').index.get_level_values('TradingDay').unique():
            df = dfAll.ix[(dt, SecuCode)].reset_index()
            dictOne = {}
            dictOne['TradingDay'] = dt
            dictOne['SecuCode'] = SecuCode
            # 
            ixDominant = df['Position'].argmax()
            ixNear = df['DeliveryDay'].argmin()
            ixDeferred = df['DeliveryDay'].argmax()
            # TS2 near~dominant
            #returnRoll = df.ix[ixDominant, 'Close'] / df.ix[ixNear, 'Close'] - 1
            returnRoll = df.ix[ixNear, 'Close'] / df.ix[ixDominant, 'Close'] - 1
            NDay = (df.ix[ixDominant, 'DeliveryDay'] - df.ix[ixNear, 'DeliveryDay']).days
            if NDay == 0:
                returnRoll = 0
            else:
                returnRoll = returnRoll * 365 / np.float(NDay)
            dictOne['TS2'] = returnRoll
            # TS3 near~deferred
            #returnRoll = df.ix[ixDeferred, 'Close'] / df.ix[ixNear, 'Close'] - 1
            if np.isnan(df.ix[ixDeferred, 'Close']):
                returnRoll = df.ix[ixNear, 'Close'] / df.ix[ixDeferred, 'Settle'] - 1
            else:
                returnRoll = df.ix[ixNear, 'Close'] / df.ix[ixDeferred, 'Close'] - 1
            NDay = (df.ix[ixDeferred, 'DeliveryDay'] - df.ix[ixNear, 'DeliveryDay']).days
            returnRoll = returnRoll * 365 / np.float(NDay)
            dictOne['TS3'] = returnRoll
    
            listDict.append(dictOne)

    dfTS = pd.DataFrame(listDict)
    dfTS = dfTS.set_index(['TradingDay', 'SecuCode'])
    dfTS = dfTS.sort_index()
    dfTS.to_pickle('dfTS.pickle')
    
    # MA of TS
    dfTSMA = dfTS.copy()
    def funcMA(df, NDayMA):
        df = df.set_index('TradingDay').sort_index()
        df = df.rolling(NDayMA).mean()
        df = df.reset_index()
        df = df.drop('SecuCode', axis=1)
        return df
    NDayMA = 5
    dfTSMA = dfTSMA.reset_index().groupby('SecuCode').apply(funcMA, NDayMA)
    dfTSMA = dfTSMA.reset_index().set_index(['TradingDay', 'SecuCode']).sort_index()
    dfTSMA = dfTSMA.drop('level_1', axis=1)
    dfTSMA.to_pickle('dfTSMA.pickle')
    
    # determine the trend TS2
    strMethodTrend = 'TS2'
    strMethodTrendIndicator = 'indicator' + strMethodTrend
    dfTS[strMethodTrendIndicator] = np.nan
    listDict = []
    for dt in dfTS.index.get_level_values('TradingDay').unique():
        print dt
        series = dfTS.ix[dt][strMethodTrend]
        listSecuCodeLong = series[series >= max(0.1, series.quantile(0.8))].index
        listSecuCodeShort = series[series <= min(-0.1, series.quantile(0.2))].index
        for SecuCode in listSecuCodeLong:
            listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': 1.0})
        for SecuCode in listSecuCodeShort:
            listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': -1.0})
    
    dfTrend = pd.DataFrame(listDict)
    dfTrend = dfTrend.set_index('TradingDay')
    dfTrend = dfTrend.sort_index()
    dfTrend.to_pickle('dfTrend%s.pickle'%strMethodTrend)
    
    # determine the trend TS3
    strMethodTrend = 'TS3'
    strMethodTrendIndicator = 'indicator' + strMethodTrend
    dfTS[strMethodTrendIndicator] = np.nan
    listDict = []
    for dt in dfTS.index.get_level_values('TradingDay').unique():
        print dt
        series = dfTS.ix[dt][strMethodTrend]
        listSecuCodeLong = series[series >= series.quantile(0.8)].index
        listSecuCodeShort = series[series <= series.quantile(0.2)].index
        for SecuCode in listSecuCodeLong:
            listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': 1.0})
        for SecuCode in listSecuCodeShort:
            listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': -1.0})
    
    dfTrend = pd.DataFrame(listDict)
    dfTrend = dfTrend.set_index('TradingDay')
    dfTrend = dfTrend.sort_index()
    dfTrend.to_pickle('dfTrend%s.pickle'%strMethodTrend)

#####################
# DataBase
#####################
def getAllContract(SecuCode, dtStart):
    import CommodityDataBase as CDB
    reload(CDB)
    dfContract = CDB.Utils.UtilsDB.getAllInTable(CDB.Utils.UtilsDB.strMySQLDB, CDB.Utils.UtilsDB.TB_CONTRACT_INFO)
    dfContract = dfContract[(dfContract['LastTradingDay'] >= dtStart)&(dfContract['ProductCode']==SecuCode)]

    listDF = []
    for ix, row in dfContract.iterrows():
        symbol = row['symbol']
        df = CDB.Utils.UtilsDB.readDB(CDB.Utils.UtilsDB.DAILY_DB_NAME, symbol, dtStart)
        df = df[(df['trade_date'] >= row['FirstTradingDay']) & (df['trade_date'] <=row['LastTradingDay'])]

        # use settle to fill null close
        if df.empty is False:
            listFFill = ['open', 'high', 'low', 'close']
            df[listFFill] = df[listFFill].replace(0, np.nan)
            ixNA = df[df['close'].isnull()].index
            for strColumn in listFFill:
                df.ix[ixNA, strColumn] = df.ix[ixNA, 'settle']
        df['SecuCode'] = row['SecuCode']
        df['DeliveryDay'] = row['DeliveryDay']
        df = df.rename(columns={
            'trade_date': 'TradingDay',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'TurnoverVolume',
            'openInterest': 'Position',
            'settle': 'Settle',
            })
        listDF.append(df)

    df = pd.concat(listDF, axis=0)
    df = df.set_index(['TradingDay', 'SecuCode']).sort_index()
    return df

def prepareTrend(dtStart):
    import ThaiExpress.Common.Utils as Utils
    reload(Utils)
    listSecuCode = Utils.ParamRange.listSecuCFE
    
    #for SecuCode in dfAll.index.get_level_values('SecuCode').unique():
    #listSecuCode = ['cf.czc']
    for SecuCode in listSecuCode:
        print SecuCode
        listDict = []
        dfAll = getAllContract(SecuCode, dtStart)
        listTradingDay = dfAll.xs(SecuCode, level='SecuCode').index.get_level_values('TradingDay').unique()
        for dt in listTradingDay:
            df = dfAll.ix[(dt, SecuCode)].reset_index()
            dictOne = {}
            dictOne['TradingDay'] = dt
            dictOne['SecuCode'] = SecuCode
            # 
            ixDominant = df['Position'].argmax()
            ixNear = df['DeliveryDay'].argmin()
            ixDeferred = df['DeliveryDay'].argmax()
            # TS2 near~dominant
            #returnRoll = df.ix[ixDominant, 'Close'] / df.ix[ixNear, 'Close'] - 1
            returnRoll = df.ix[ixNear, 'Close'] / df.ix[ixDominant, 'Close'] - 1
            NDay = (df.ix[ixDominant, 'DeliveryDay'] - df.ix[ixNear, 'DeliveryDay']).days
            if NDay == 0:
                returnRoll = 0
            else:
                returnRoll = returnRoll * 365 / np.float(NDay)
            dictOne['TS2'] = returnRoll
            # TS3 near~deferred
            #returnRoll = df.ix[ixDeferred, 'Close'] / df.ix[ixNear, 'Close'] - 1
            if np.isnan(df.ix[ixDeferred, 'Close']):
                returnRoll = df.ix[ixNear, 'Close'] / df.ix[ixDeferred, 'Settle'] - 1
            else:
                returnRoll = df.ix[ixNear, 'Close'] / df.ix[ixDeferred, 'Close'] - 1
            NDay = (df.ix[ixDeferred, 'DeliveryDay'] - df.ix[ixNear, 'DeliveryDay']).days
            returnRoll = returnRoll * 365 / np.float(NDay)
            dictOne['TS3'] = returnRoll
    
            listDict.append(dictOne)

        dfTS = pd.DataFrame(listDict)
        dfTS = dfTS.rename(columns={'SecuCode':'code', 'TradingDay': 'trade_date'})
        dfTS = dfTS[['trade_date','code','TS2','TS3']]
        import CommodityDataBase as CDB
        reload(CDB)
        CDB.Utils.UtilsDB.saveTB_DAILY(CDB.Utils.UtilsDB.DAILY_REFERENCE_DB_NAME, 'TS', dfTS)

        del dfTS, listDict, dfAll
        gc.collect()

def getTSXSMIndicator(dtStart, dictOption={}):
    if Utils.boolUsingDB:
        # get TS data
        import CommodityDataBase as CDB
        reload(CDB)
        sql = 'SELECT * FROM {1} where trade_date >= "{0}"'.format(dtStart.strftime('%Y%m%d'), CDB.Utils.UtilsDB.DAILY_REFERENCE_DB_NAME)
        dfTS = CDB.Utils.UtilsDB.queryDominant(sql)
        dfTS = dfTS.rename(columns={'code':'SecuCode', 'trade_date':'TradingDay'})
        dfTS = dfTS.set_index(['TradingDay', 'SecuCode']).sort_index()

        # determine the trend TS2
        #strMethodTrend = 'TS2'
        strMethodTrend = dictOption['strMethodTrend']
        strMethodTrendIndicator = 'indicator' + strMethodTrend
        dfTS[strMethodTrendIndicator] = np.nan
        listDict = []
        for dt in dfTS.index.get_level_values('TradingDay').unique():
            series = dfTS.ix[dt][strMethodTrend]
            listSecuCodeLong = series[series >= max(0.1, series.quantile(0.8))].index
            listSecuCodeShort = series[series <= min(-0.1, series.quantile(0.2))].index
            for SecuCode in listSecuCodeLong:
                listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': 1.0})
            for SecuCode in listSecuCodeShort:
                listDict.append({'SecuCode': SecuCode, 'TradingDay': dt, 'indicator': -1.0})
        
        dfTrend = pd.DataFrame(listDict)
        dfTrend = dfTrend.set_index('TradingDay')
        dfTrend = dfTrend.sort_index()
        #dfTrend.to_pickle('dfTrend%s.pickle'%strMethodTrend)

        del dfTS, listDict
        gc.collect()
    else:
        if Utils.boolMonitor:
            dfTrend = pd.read_pickle('dfTrend%s.pickle'%dictOption['strMethodTrend']).reset_index().set_index('TradingDay')
        else:
            dfTrend = pd.read_pickle('%s/dfTrend%s.pickle'%(Utils.dirDataSource, dictOption['strMethodTrend'])).reset_index().set_index('TradingDay')

    return dfTrend
    
def getTS(dtStart, dictOption=None):
    # get TS data
    import CommodityDataBase as CDB
    reload(CDB)
    sql = 'SELECT * FROM {1} where trade_date >= "{0}"'.format(dtStart.strftime('%Y%m%d'), CDB.Utils.UtilsDB.DAILY_REFERENCE_DB_NAME)
    dfTS = CDB.Utils.UtilsDB.queryDominant(sql)
    dfTS = dfTS.rename(columns={'code':'SecuCode', 'trade_date':'TradingDay'})
    dfTS = dfTS.set_index(['TradingDay', 'SecuCode']).sort_index()

    return dfTS
    
if __name__ == '__main__': 
    if Utils.boolUsingDB:
        #prepareTrend(CDB.Utils.UtilsDB.getDTLastData()-datetime.timedelta(30, 0))
        prepareTrend(Utils.dtBackTestStart)
    else:
        funcPrepareDataExcel()
        prepareTrendExcel()

