# -*- coding = utf8 -*-
import pandas as pd
import datetime
import numpy as np
import os, gc, re, sys
from dateutil.parser import parse
import ThaiExpress.Common.Utils as Utils
reload(Utils)

# prepare the SecuCode for CZCE
def getFileName():
    if Utils.boolMonitor:
        strFileAddress = 'dailyAll.xlsx'
    else:
        strFileAddress = 'dailyAll10Y.xlsx'
    return strFileAddress

def getDFContract(strFileAddress):
    dfContract = pd.read_excel(Utils.dirDataSource + strFileAddress, sheetname='Contract')
    #dfTrading.to_pickle('dfTrading_Raw.pickle')
    #dfContract.to_pickle('dfContract_Raw.pickle')
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
            #strSecu = dictSecu[strSecu]
            strSecu = strSecu
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

    return dfContract
    
def getDFTrading(strFileAddress, dfContract):
    dfTrading = pd.read_excel(Utils.dirDataSource + strFileAddress, sheetname='ALL')

    dfTrading = dfTrading.merge(dfContract[['SecuID', 'DeliveryDay', 'DeliveryMonth', 'ContractCode', 'SecuCode']], on='SecuID', how='inner')
    dfTrading['TradingDay'] = dfTrading['Date'].apply(lambda x: parse(str(int(x))))
    dfTrading['DeliveryDay'] = dfTrading['DeliveryDay'].apply(lambda x: parse(str(int(x))))
    #dfTrading.to_pickle('dfTrading.pickle')
    dfSimple = dfTrading[['TradingDay', 'SecuCode', 'ContractCode', 'Close', 'Settle', 'TurnoverVolume', 'Position', 'DeliveryDay']]
    dfSimple = dfSimple.set_index(['TradingDay', 'SecuCode'])
    dfSimple = dfSimple.sort_index()
    #dfSimple.to_pickle('dfSimple.pickle')
    
    # concat the dominant contract
    #dfSimple = pd.read_pickle('dfSimple.pickle')
    #dfTrading = pd.read_pickle('dfTrading.pickle')
    dfTrading = dfTrading.set_index('TradingDay').sort_index()
    return dfTrading
    
