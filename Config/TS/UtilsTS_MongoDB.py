import pandas as pd
import datetime
import numpy as np
import os
import re
import sys
from dateutil.parser import parse

import ThaiExpress.Common.Utils as Utils
reload(Utils)

#####################
# configuration & data
#####################

#####################
# define functions
#####################
def getAllContract(SecuCode, dtStart):
    import CommodityDataBase as CDB
    reload(CDB)
    dfContract = CDB.UtilsConcat.getDFContract(CDB.Utils.strAddressContractInfo)
    dfContract = dfContract[dfContract['LastTradingDay'] >= dtStart]

    listDF = []
    for ix, row in dfContract.iterrows():
        symbol = row['symbol']
        df = CDB.Utils.readDB(CDB.Utils.DAILY_DB_NAME, symbol, dtStart)

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
    listSecuCode = Utils.ParamRange.listSecuAll
    
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
        CDB.Utils.saveTB_DAILY(CDB.Utils.DAILY_REFERENCE_DB_NAME, 'TS', dfTS)


def getTSXSMIndicator(dtStart, dictOption={}):
    # get TS data
    import CommodityDataBase as CDB
    reload(CDB)
    dfTS = CDB.Utils.readDB(CDB.Utils.DAILY_REFERENCE_DB_NAME, 'TS', dtStart)
    dfTS = dfTS.rename(columns={'SecuCode': 'code', 'TradingDay': 'trade_date'})
    dfTS = dfTS.set_index(['TradingDay', 'SecuCode']).sort_index()

    # determine the trend TS2
    strMethodTrend = 'TS2'
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
    return dfTrend
    
if __name__ == '__main__': 
    #funcPrepareData()
    prepareTrend(Utils.dtBackTestStart)


