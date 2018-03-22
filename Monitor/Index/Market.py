import os, sys, shutil
import pandas as pd
import numpy as np
import datetime

#import ThaiExpress.Common.Utils as Utils
#reload(Utils)

########################
# retrieve data
########################
import StockDataBase as SDB
reload(SDB)
import StockDataBase.DataReader as SDBReader
reload(SDBReader)

dfPEQuantile = pd.read_pickle('PEQuantile_M.pickle')

listPCT = []
listIndexCode = ['000016', '000300', '000905', '399001', '399005', '399006']
for SecuCode in ['000300', '000905']:
    strFileAddress = '%s.pickle'%SecuCode
    if os.path.exists(strFileAddress):
        df = pd.read_pickle(strFileAddress)
    else:
        dtStart = datetime.datetime(2005, 1, 1)
        if SecuCode not in listIndexCode:
            strTB = SDB.Utils.strTB_TradingDataDailyExe
        else:
            strTB = SDB.Utils.strTB_IndexTrading
        df = SDBReader.getTradingDataDaily_TSM(SecuCode, dtStart, None, strTB)
        df.to_pickle(strFileAddress)
    df = df.rename(columns={'TurnoverVolume': 'Volume'})
    df.name = SecuCode
    
    ########################
    # generate indicator
    ########################
    import Param
    reload(Param)
    import ThaiExpress.Common.Utils as Utils
    reload(Utils)
    import ThaiExpress.Config.Tech.UtilsTech as UtilsTech
    reload(UtilsTech)
    import ThaiExpress.Config.Tech.UtilsTech as UtilsTALib
    reload(UtilsTALib)
    
    dictDataSpec = Param.dictDataSpecMA
    dictDataSpec['df'] = df
    sIndicatorMA = UtilsTech.generateIndicator(dictDataSpec)
    
    dictDataSpec = Param.dictDataSpecTALib
    dictDataSpec['df'] = df
    sIndicator = UtilsTech.generateIndicator(dictDataSpec)
    sIndicator = sIndicator.replace(-1, 0)

    ########################
    # PE PB for position 
    ########################
    strPBPE = 'PB'
    if SecuCode in ['000016', '000300', '000001']:
        sPB = dfPEQuantile['%s_Median_300'%strPBPE]
    elif SecuCode in ['000905']:
        sPB = dfPEQuantile['%s_Median_500'%strPBPE]
    else:
        sPB = dfPEQuantile['%s_Median_ALL'%strPBPE]
    #sPB = dfPEQuantile['%s_Median_25PCT'%strPBPE]
    sPB = dfPEQuantile['%s_Median_800'%strPBPE]
    sPosition = sPB.copy()
    alpha = 0.02
    PBUpper = sPosition.quantile(1-alpha)
    PBLower = sPosition.quantile(alpha)
    print SecuCode, PBLower, PBUpper
    sPositionRaw = sPosition.apply(lambda x: max(PBUpper-max(PBLower,x), 0)/ (PBUpper-PBLower))
    sPositionAPV = sPositionRaw
    sPositionAPV = sPositionRaw.apply(lambda x: np.power(x, 1.2))

    '''
    # adjust position x2, x0.5
    NWide = 4
    NDouble = 2.

    PBL = sPosition.quantile(alpha*NWide)
    PBH = sPosition.quantile(1-alpha*NWide)
    
    ixPrev = sPB.index[0]
    PBPrev = sPB.ix[ixPrev]
    strTrend = None
    for ix, PB in sPB.iteritems():
        if strTrend is None:
            if PB < PBL:
                strTrend = 'Up'
                dfPEQuantile.ix[ix, 'Trend'] = 1
            elif PB > PBH:
                strTrend = 'Dn'
                dfPEQuantile.ix[ix, 'Trend'] = -1
        elif strTrend == 'Up':
            if PB > PBH:
                strTrend = 'Dn'
                dfPEQuantile.ix[ix, 'Trend'] = -1
        elif strTrend == 'Dn':
            if PB < PBL:
                strTrend = 'Up'
                dfPEQuantile.ix[ix, 'Trend'] = 1
        ixPrev = ix
        PBPrev = PB

    dfPEQuantile['Trend'] = dfPEQuantile['Trend'].ffill()
    ixUp = dfPEQuantile[dfPEQuantile['Trend']==1].index
    ixDn = dfPEQuantile[dfPEQuantile['Trend']==-1].index
    sPosition.ix[ixUp] = sPosition.ix[ixUp] * NDouble
    sPosition.ix[ixDn] = sPosition.ix[ixDn] / NDouble
    '''

    ########################
    # NPBLT1 for position 
    ########################
    strPBPE = 'NPBLT1'
    #strPBPE = 'NPELT10'
    if SecuCode in ['000016', '000300', '000001']:
        sPB = dfPEQuantile['%s_300'%strPBPE]
    elif SecuCode in ['000905']:
        sPB = dfPEQuantile['%s_500'%strPBPE]
    else:
        sPB = dfPEQuantile['%s_ALL'%strPBPE] /dfPEQuantile['NStock_ALL']
    sPB = dfPEQuantile['%s_300'%strPBPE]
    #sPB = dfPEQuantile['%s_ALL'%strPBPE] /dfPEQuantile['NStock_ALL']
    sPosition = sPB.copy()
    alpha = 0.1
    PBUpper = sPosition.quantile(1-alpha)
    PBLower = sPosition.quantile(alpha*2)
    print SecuCode, PBLower, PBUpper
    sPositionRaw = sPosition.apply(lambda x: max(PBUpper-max(PBLower,x), 0)/ (PBUpper-PBLower))
    sPositionAPN = 1-sPositionRaw

    ########################
    # evaluate 
    ########################
    '''
    #sIndicator = sIndicatorMA
    sPCT = df['PCT'] / 100.
    sPCTHold = sPCT * sIndicator.ffill().shift(1)
    sPCTHold.ix[sIndicator.dropna().index] = sPCTHold.ix[sIndicator.dropna().index] - 0.001
    sPCTHold.name = SecuCode
    
    dictResult = Utils.funcMetric(sPCTHold)
    print SecuCode
    print dictResult
    print sIndicator.dropna().size
    #'''
    
    #'''
    import ThaiExpress.Common.Strategy as Strategy
    dictParam = dict(Utils.dictParamTemplate)
    dictParam['commission'] = 0.002
    dictParam['switchPlot'] = False
    dictParam['boolStoploss'] = False
    dictParam['boolStopProfit'] = False
    strategy = Strategy.Strategy(df, dictParam)
    strategy.df['indicator'] = sIndicator
    dictResult = strategy.evaluateLongShortSimplified()
    print dictResult['DTMaxDD']

    sPCTPort = strategy.df['returnPCTHold']
    dictResult = Utils.funcMetric(sPCTPort)

    #sPCTPortAPN = sPCTPort * sPositionAPN.resample('1D').last().ffill().ix[sPCTPort.index].ffill()
    #dictResult = Utils.funcMetric(sPCTPortAPN)
    
    sPositionAPV = sPositionAPV.resample('1D').last().ffill().ix[sPCTPort.index].ffill()
    sPCTPortAPV = sPCTPort * sPositionAPV
    sPCTPortAPV = sPCTPortAPV + 0.025/250 * (1 - sPositionAPV)
    dictResult = Utils.funcMetric(sPCTPortAPV)

    sPCTHold = sPCTPortAPV
    sPCTHold.name = SecuCode
    print dictResult
    print sIndicator.dropna().size
    print sIndicator.dropna().tail()
    #'''
    
    listPCT.append(sPCTHold)

dfPCT = pd.concat(listPCT, axis=1)
sPCTPort = dfPCT.mean(1)
dictResultPort = Utils.funcMetric(sPCTPort)
print dictResultPort

sV = (sPCTPort+1).cumprod()
sA = sV.resample('1A').last().pct_change()
sQ = sV.resample('1Q').last().pct_change()
sM = sV.resample('1M').last().pct_change()
s240 = sV/sV.shift(240) - 1

'''
dictResultPort = Utils.funcMetric(sPCTPortAP)
print dictResultPort
''' 
    
