import pandas as pd
import datetime
import numpy as np
import os, gc, re, sys
from dateutil.parser import parse

import ThaiExpress.Common.Utils as Utils
reload(Utils)

import Utils as UtilsConcat
reload(UtilsConcat)

# read data
#'''
strFileName = UtilsConcat.getFileName()
dfContract = UtilsConcat.getDFContract(strFileName)
dfTrading = UtilsConcat.getDFTrading(strFileName, dfContract)

# calculate PCT
dfPCT = pd.DataFrame()
listColumn = ['Open', 'High', 'Low', 'Close', 'Settle', 'TurnoverVolume', 'Position', 'ContractCode', 'SecuCode', 'DeliveryDay']
for strContractCode in dfTrading['ContractCode'].unique():
    print strContractCode
    dfOneContract = dfTrading.ix[dfTrading['ContractCode']==strContractCode, listColumn].copy()
    listFFill = ['Open', 'High', 'Low', 'Close']
    ixNA = dfOneContract[dfOneContract['Close'].isnull()].index
    for strColumn in listFFill:
        dfOneContract.ix[ixNA, strColumn] = dfOneContract.ix[ixNA, 'Settle']
    dfOneContract['PCT'] = dfOneContract['Close'].pct_change()
    dfOneContract['Delta'] = dfOneContract['Close'].diff()
    dfOneContract['DeltaSettle'] = dfOneContract['Settle'].diff()
    dfOneContract['Open-PreSettle'] = dfOneContract['Open'] - dfOneContract['Settle'].shift(1)
    dfOneContract['Settle-Open'] = dfOneContract['Settle'] - dfOneContract['Open']
    dfPCT = dfPCT.append(dfOneContract)
dfPCT.to_pickle('dfPCT.pickle')
#'''

# determine the main contract
dfPCT = pd.read_pickle('dfPCT.pickle')
dfPCT = dfPCT.reset_index().set_index(['TradingDay', 'SecuCode']).sort_index()
dfPCT['Position'] = dfPCT['Position'].fillna(0)
listDict = []
setPrevDominant = set()
dictDominant = {}
dictDominantCandidate = {}
dictBoolDominantChange = {}
dictBoolDominantChangeCandidate = {}

def getYYYYMM(strYYMM):
    pat = re.compile('(\D+)(\d+)')
    yymm = int(pat.match(strYYMM).groups()[1])
    if yymm > 9000:
        ret = yymm + 190000
    else:
        ret = yymm + 200000
    return ret

dfPCT = dfPCT[dfPCT.index.get_level_values('TradingDay')>Utils.dtBackTestStart]
for ix in dfPCT.index.unique():
    dt = ix[0]
    SecuCode = ix[1]
    #print ix
    df = dfPCT.ix[ix].copy().reset_index().set_index('ContractCode').sort_index(ascending=False)
    ixDominant = df['Position'].argmax()

    # if ixDominant delivering next trading day 
    if SecuCode in Utils.ParamRange.listSecuCFE:
        NDayForceToChangeDominant = 2
    else:
        NDayForceToChangeDominant = 35
    NDayBeforeDelivery = (df.ix[ixDominant, 'DeliveryDay'] - dt).days
    if NDayBeforeDelivery < NDayForceToChangeDominant:
        dfCandidateContract = df[df.index > ixDominant]
        if dfCandidateContract.empty == False:
            ixDominant = dfCandidateContract['Position'].nlargest(1).index[0]
        else:
            ixDominant = df.index[0]

    # for the start of loop
    if dictDominant.has_key(SecuCode) is False:
        dictDominant[SecuCode] = ixDominant
        dictDominantCandidate[SecuCode] = ixDominant
        setPrevDominant.add(ixDominant)
        dictBoolDominantChange[SecuCode] = False
        dictBoolDominantChangeCandidate[SecuCode] = False

    # determine the contract
    strContractCode = dictDominant[SecuCode]
    if strContractCode == 'FG1805':
        #raise Exception
        pass
    # 19951115 RU9511 is dominant until the delivery day, so RU9512 should be the dominant contract
    if strContractCode not in df.index:
        print ix, 'dominant contract not available'
        strContractCode = ixDominant

    # append to listDict
    row = df.ix[strContractCode]
    boolDominantChange = dictBoolDominantChange[SecuCode]

    dictOne = {
        'TradingDay': ix[0],
        'SecuCode': SecuCode,
        'PCT': row['PCT'],
        'Delta': row['Delta'],
        'DeltaSettle': row['DeltaSettle'],
        'Open-PreSettle': row['Open-PreSettle'],
        'Settle-Open': row['Settle-Open'],
        'ContractCode': strContractCode,
        'Open': row['Open'],
        'High': row['High'],
        'Low': row['Low'],
        'Close': row['Close'],
        'Settle': row['Settle'],
        'Position': row['Position'],
        'TurnoverVolume': row['TurnoverVolume'],
        'boolDominantChange': boolDominantChange,
        }

    # update the dominant contract candidate (ready for Friday adjustment)
    if ixDominant not in setPrevDominant and ixDominant != dictDominant[SecuCode]:
        # only change dominant contract forward
        yymmNew = getYYYYMM(ixDominant)
        yymmOld = getYYYYMM(dictDominant[SecuCode])
        if yymmNew <= yymmOld:
            dictBoolDominantChange[SecuCode] = False
            pass
        else:
            if (Utils.boolDominantChangeOnlyOnFriday is False) or (Utils.boolDominantChangeOnlyOnFriday is True and dt.weekday()==3):
                print ix, ixDominant, dictDominant[SecuCode], dictBoolDominantChange[SecuCode]
                dictDominant[SecuCode] = ixDominant
                setPrevDominant.add(ixDominant)
                dictBoolDominantChange[SecuCode] = True
            else:
                dictBoolDominantChange[SecuCode] = False
    else:
        dictBoolDominantChange[SecuCode] = False
        pass

    dictOne['boolDominantChangeTomorrow'] = dictBoolDominantChange[SecuCode]
    dictOne['ContractCodeTomorrow'] = dictDominant[SecuCode]
    listDict.append(dictOne)

dfConcat = pd.DataFrame(listDict)
#dfConcat = dfConcat.to_pickle('dfConcat.pickle')

# cumprod
#dfConcat = pd.read_pickle('dfConcat.pickle') 
dfConcat = dfConcat.set_index('TradingDay')
dfConcat['PCT'] = dfConcat['PCT'].fillna(0)
dfConcat['Value'] = np.nan
dfResult = pd.DataFrame()
for SecuCode in dfConcat['SecuCode'].unique():
    if SecuCode in ['wh.czc', 'ri.czc', 'oi.czc', 'ma.czc', 'zc.czc']:
        #continue
        pass
    print SecuCode
    dfOneSecu = dfConcat[dfConcat['SecuCode']==SecuCode].copy()
    dfOneSecu['Value'] = (dfOneSecu['PCT'] + 1).cumprod()
    dfResult = dfResult.append(dfOneSecu)

# scale OHLC
dfResult['OpenRaw'] = dfResult['Open']
dfResult['SettleRaw'] = dfResult['Settle']
dfResult['CloseRaw'] = dfResult['Close']
dfResult['Open'] = dfResult['Open'] / dfResult['Close'] * dfResult['Value']
dfResult['High'] = dfResult['High'] / dfResult['Close'] * dfResult['Value']
dfResult['Low'] = dfResult['Low'] / dfResult['Close'] * dfResult['Value']
dfResult['Settle'] = dfResult['Settle'] / dfResult['Close'] * dfResult['Value']
dfResult['Close'] = dfResult['Value']
dfResult = dfResult.reset_index().set_index(['SecuCode', 'TradingDay'])
strFileAddress = Utils.dirDataSource + '/1day/' + 'ExeCommodityFuture.pickle'
dfResult = dfResult[dfResult.index.get_level_values('SecuCode').isin(Utils.listSecuAll)]
dfResult.to_pickle(strFileAddress)


