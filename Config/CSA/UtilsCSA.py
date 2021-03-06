# -*- coding:utf8 -*-
import pandas as pd
import datetime
import numpy as np
import os, gc, re, sys, logging
from dateutil.parser import parse

import ThaiExpress.Common.Utils as Utils
reload(Utils)
import ThaiExpress.Common.ParamRange as ParamRange
reload(ParamRange)
import ThaiExpress.Config.TS.UtilsTS as UtilsTS
reload(UtilsTS)
import CommodityDataBase as CDB
reload(CDB)

#####################
# parameter
#####################
TB_NAME_TS159 = 'TS159'

#####################
# logging
#####################

#########################
# prepare diff contract
#########################
def concatContractDiff():
    dfContract = CDB.Utils.UtilsDB.getAllInTable(CDB.Utils.UtilsDB.strMySQLDB, CDB.Utils.UtilsDB.TB_CONTRACT_INFO)
    dfContract = dfContract.set_index('ContractCodeFullRename')
    
    import ThaiExpress.Common.Utils as Utils
    reload(Utils)
    listSecuCode = Utils.ParamRange.listSecuAll
    dtStart = Utils.dtBackTestStart
    
    #listSecuCode = ['j.dce']
    dfContractDiffAll = pd.DataFrame()
    #----------- prepare contract pairs
    for SecuCode in listSecuCode:
    #for SecuCode in ['sr.czc']:
        print SecuCode
        dfOneProduct = dfContract[dfContract['ProductCode']==SecuCode]
        listContractCode = dfOneProduct.index.unique()
        if SecuCode in ['rb.shf', 'hc.shf']:
            listContractDominant = [x for x in listContractCode if (x.endswith('01') or x.endswith('05') or x.endswith('10'))]
        elif SecuCode in ['zc.czc', 'j.dce', 'jm.dce', 'i.dce', 'sm.czc', 'sf.czc', 'fg.czc', 'ru.shf'] + ParamRange.listSecuNONG + ParamRange.listSecuHUA:
            listContractDominant = [x for x in listContractCode if (x.endswith('01') or x.endswith('05') or x.endswith('09'))]
        else:
            print '%s not suitable for CSA'%SecuCode
            continue
        
        dfTrading = UtilsTS.getAllContract(SecuCode, dtStart)
        dfTrading = dfTrading.rename(columns={'ContractCodeFullRename': 'ContractCode'})
        listContractDominant = list(set(dfTrading['ContractCode'].unique()).intersection(listContractDominant))
    
        listContractDominant = list(np.sort(listContractDominant))
        if len(listContractDominant) == 0:
            continue
    
    
        ContractCodeNear = listContractDominant[0]
        listDictContractPair = []
        for ContractCodeFar in listContractDominant[1:]:
            listDictContractPair.append({
                'near': ContractCodeNear,
                'far': ContractCodeFar,
                'FirstTradingDay': parse(str(dfContract.ix[ContractCodeFar, 'FirstTradingDay'])),
                'LastTradingDay': parse(str(dfContract.ix[ContractCodeNear, 'LastTradingDay'])),
                'SecuCode': re.compile('(\D+)\d+').match(ContractCodeNear).groups()[0],
                'SecuCodeRaw': SecuCode,
                })
            ContractCodeNear = ContractCodeFar
            dfContractPair = pd.DataFrame(listDictContractPair)
        
        #----------- merge diff of contract pairs 15, 59, 91
        # merge diff of contract 15, 59, 91
        dfTrading = dfTrading.reset_index().set_index(['ContractCode', 'TradingDay']).sort_index()
        listDFDiffContract = []
        for ix, rowContractPair in dfContractPair.iterrows():
            dfNear = dfTrading.ix[rowContractPair['near']]
            dfFar = dfTrading.ix[rowContractPair['far']]
            dtFirst = rowContractPair['FirstTradingDay']
            dtLast = rowContractPair['LastTradingDay']
            dfNear = dfNear.ix[(dfNear.index>=dtFirst)&(dfNear.index<=dtLast)]
            dfFar = dfFar.ix[(dfFar.index>=dtFirst)&(dfFar.index<=dtLast)]
        
            # prep
            dictMonth = {1:1, 5:5, 9:9, 0:9}
            strDiffContractCode = rowContractPair['near'] + '_CSA'
            strDiffSecuCode = rowContractPair['SecuCode'] + str(dictMonth[int(rowContractPair['near'][-1])]) + str(dictMonth[int(rowContractPair['far'][-1])])
            listS = []
            for strColumn in ['Open', 'High', 'Low', 'Close', 'Settle', 'TurnoverVolume', 'Position']:
                # ratio
                s = dfNear[strColumn] / dfFar[strColumn] - 1
                s.name = 'Ratio%s'%strColumn
                listS.append(s)
                sRatio = s
                # diff
                s = dfNear[strColumn] - dfFar[strColumn]
                s.name = 'Diff%s'%strColumn
                listS.append(s)
                sDiff = s
                # sum
                s = dfNear[strColumn] + dfFar[strColumn]
                s.name = 'Sum%s'%strColumn
                listS.append(s)
                sSum = s
                # near
                s = dfNear[strColumn]
                s.name = 'Near%s'%strColumn
                listS.append(s)
                sNear = s
                # far
                s = dfFar[strColumn]
                s.name = 'Far%s'%strColumn
                listS.append(s)
                sFar = s
                # min
                s = pd.concat([dfNear[strColumn], dfFar[strColumn]], axis=1).min(1)
                s.name = 'Min%s'%strColumn
                listS.append(s)
                sMin = s
                # max
                s = pd.concat([dfNear[strColumn], dfFar[strColumn]], axis=1).max(1)
                s.name = 'Max%s'%strColumn
                listS.append(s)
                sMax = s
                # PCT
                s = sDiff / sMax
                s.name = 'PCTMax%s'%strColumn
                listS.append(s)
                s = sDiff / sSum
                s.name = 'PCTSum%s'%strColumn
                listS.append(s)
                s = sDiff / sNear
                s.name = 'PCTNear%s'%strColumn
                listS.append(s)
        
            df = pd.concat(listS, axis=1)
            df['NearContractCode'] = rowContractPair['near']
            df['FarContractCode'] = rowContractPair['far']
            df['ContractCode'] = strDiffContractCode
            df['SecuCode'] = strDiffSecuCode
            df['SecuCodeRaw'] = rowContractPair['SecuCodeRaw']
        
            # forage in accordance with concat.py
            df['PCT'] = df['DiffClose'].pct_change()
            df['Delta'] = df['DiffClose'].diff()
            df['DeltaSettle'] = df['DiffSettle'].diff()
            df['Open-PreSettle'] = df['DiffOpen'] - df['DiffSettle'].shift(1)
            df['Settle-Open'] = df['DiffSettle'] - df['DiffOpen']
            df['Open'] = df['DiffOpen']
            df['High'] = df['DiffHigh']
            df['Low'] = df['DiffLow']
            df['Close'] = df['DiffClose']
            df['Settle'] = df['DiffSettle']
            df['Position'] = df['MinPosition']
            df['TurnoverVolume'] = df['MinTurnoverVolume']
            df['boolDominantChange'] = False
            
            listDFDiffContract.append(df)
        dfContractDiffAll = dfContractDiffAll.append(pd.concat(listDFDiffContract, axis=0))
    
    ## output
    strFileAddress = Utils.dirDataSource + '/1day/' + 'CommodityFuture_CSA.pickle'
    dfContractDiffAll.to_pickle(strFileAddress)
    
    
#####################
# define functions
#####################
def prepareTrend(dtStart):
    listSecuCode = Utils.ParamRange.listSecuAll
    listDict = []
    for SecuCode in listSecuCode:
        print SecuCode
        # determine listMonth
        if SecuCode in ['rb.shf', 'hc.shf']:
            listMonth = [1, 5, 10]
        elif SecuCode in ['zc.czc', 'j.dce', 'jm.dce', 'i.dce', 'sm.czc', 'sf.czc', 'fg.czc', 'ru.shf'] + ParamRange.listSecuNONG + ParamRange.listSecuHUA:
            listMonth = [1, 5, 9]
        else:
            strLog = '%s not suitable for CSA'%SecuCode
            logging.log(logging.INFO, strLog)
            continue
        
        import ThaiExpress.Config.TS.UtilsTS as UtilsTS
        reload(UtilsTS)
        dfAll = UtilsTS.getAllContract(SecuCode, dtStart)

        # calculate
        dictMonth = {1:1, 5:5, 9:9, 10:9}
        for dt in dfAll.xs(SecuCode, level='SecuCode').index.get_level_values('TradingDay').unique():
            df = dfAll.ix[(dt, SecuCode)].reset_index()
            dictOne = {}
            dictOne['TradingDay'] = dt
            dictOne['SecuCode'] = SecuCode
            
            # near month contract
            ixNear = df['DeliveryDay'].argmin()

            # TS near~Jan,May,Sep
            for month in listMonth:
                dfDominantMonth = df[df['DeliveryDay'].apply(lambda x: x.month==month)]
                if dfDominantMonth.empty is False:
                    ixDominant = dfDominantMonth.index[0]
                else:
                    logging.log(logging.INFO, 'SecuCode %s at date %s: month %d does not exist'%(SecuCode, dt, month))
                    continue
                if ixNear == ixDominant:
                    continue
                returnRoll = df.ix[ixNear, 'Close'] / df.ix[ixDominant, 'Close'] - 1
                deltaRoll = df.ix[ixNear, 'Close'] - df.ix[ixDominant, 'Close'] - 1
                deltaSettleRoll = df.ix[ixNear, 'Settle'] - df.ix[ixDominant, 'Settle'] - 1
                NDay = (df.ix[ixDominant, 'DeliveryDay'] - df.ix[ixNear, 'DeliveryDay']).days
                if NDay == 0:
                    returnRoll = 0
                else:
                    returnRoll = returnRoll * 365 / np.float(NDay)
                strColumn = 'TSRet_S%d'%dictMonth[month]
                dictOne[strColumn] = returnRoll
                strColumn = 'TSDelta_S%d'%dictMonth[month]
                dictOne[strColumn] = deltaRoll
                strColumn = 'TSDeltaSettle_S%d'%dictMonth[month]
                dictOne[strColumn] = deltaSettleRoll
                strColumn = 'NDayToSettle_S%d'%dictMonth[month]
                dictOne[strColumn] = NDay
            listDict.append(dictOne)

    dfTS = pd.DataFrame(listDict)
    dfTS = dfTS.set_index(['SecuCode', 'TradingDay'])
    dfTS = dfTS.sort_index()
    dfTS.to_pickle('dfTS.pickle')


if __name__ == '__main__': 
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
    prepareTrend(Utils.dtBackTestStart - datetime.timedelta(365,1))


