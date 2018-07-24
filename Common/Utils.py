# -*- coding:utf-8 -*-
import pandas as pd
import datetime
import numpy as np
import pdb
import scipy.io as sio
import os, shutil
import random
from scipy.signal import butter, lfilter, freqz
from scipy import signal
from dateutil.parser import parse

import UtilsUtils
reload(UtilsUtils)

import ParamRange
reload(ParamRange)

import DumpExcel
reload(DumpExcel)

import UtilsDB
reload(UtilsDB)

import CommodityDataBase as CDB
reload(CDB)

############
# configuration
############
#pd.set_option('display.width', pd.util.terminal.get_terminal_size()[0])

############
# directories
############
import platform
strSystem = 'PC'
dirDataSource_1min = '/mnt/Tera/data/Future/1min/'

dirProjectRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dirDataSource = dirProjectRoot + '/Data/'
dirResultPerCase = dirProjectRoot + '/Output/'
if os.path.exists(dirResultPerCase) is False:
    os.mkdir(dirResultPerCase)
if os.path.exists(dirDataSource) is False:
    os.mkdir(dirDataSource)

############
# parameters
############
COMMISSION = 0.0005         # 3CNY is cost when buying or selling a contract with contract value of x 10000CNY
TOTALMONEY = 200e4          # the total money can be invested
LEVERAGE = 1.0              # the contract value cannot be larger than TOTALMONEY * LEVERAGE
NThresholdVolume = 1000     # contracts with daily volume lower than 1000 cannot be traded 
boolMonitor = False          # True: used for Monitor/ only 
boolClearData = True        # True: clear temporary data, to redo everything. 
boolDominantChangeOnlyOnFriday = False   # True: in concat.py, only change dominant contract on Friday.
UpperPositionSingleContract = 0.5
boolUsingDB = True

NTradingDayPerYear = UtilsUtils.NTradingDayPerYear
dfDetail = pd.read_excel(dirDataSource + '/SecuDetail.xlsx').set_index('SecuCode')
dfDetail.ix[ParamRange.listSecuHEISE, 'Industry'] = 'HEISE'
dfDetail.ix[ParamRange.listSecuYOUSE, 'Industry'] = 'YOUSE'
dfDetail.ix[ParamRange.listSecuNONG, 'Industry'] = 'NONG'
dfDetail.ix[ParamRange.listSecuHUA, 'Industry'] = 'HUA'
dfDetail.ix[ParamRange.listSecuGUI, 'Industry'] = 'GUI'
dfDetail.ix[ParamRange.listSecuGU, 'Industry'] = 'GUZHI'
dfDetail.ix[ParamRange.listSecuZHAI, 'Industry'] = 'ZHAI'


############
# template
############
dictParamTemplate = {
    'commission': COMMISSION * 2,
    #'boolStoploss': True,
    'stoploss': 0.05,
    'stoplossSlippage': 0.001,
    'boolStopProfit': False,
    'stopProfit': 0.05,
    'switchPlot': False,
    }

dictDataSpecTemplate = {
    'freq': '1day',
    'Secu': 'j.dce',
    'strModelName': 'TSM',
    'FixedEnter': False,
    'strFilePrefix': dirDataSource,
    'strCase': 'check',
    'dictStrategyParam': None,
    'boolStoploss': False,
    }

############
# simple logic
############
if boolMonitor:
    dtBackTestStart = datetime.datetime(2017, 2, 1)
    strDailyData = 'ExeCommodityFuture.pickle'
else:
    dtBackTestStart = datetime.datetime(2005, 1, 1)
    strDailyData = 'ExeCommodityFuture_BackTest.pickle'
#dtBackTestEnd = CDB.Utils.UtilsDB.getDTLastData() + datetime.timedelta(10, 0)

###################
# dfExe
###################
if boolUsingDB is False:
    strFileAddress = dirDataSource + '/1day/' + strDailyData
    if os.path.exists(strFileAddress):
        dfExe = pd.read_pickle(strFileAddress)
    else:
        dfExe = None

def getTableExe(strColumn, dtStart=dtBackTestStart):
    if boolUsingDB:
        sql = 'select code, trade_date, {0} from {2} where trade_date >= "{1}";'.format(strColumn, dtStart.strftime('%Y%m%d'), CDB.Utils.UtilsDB.DAILY_DOMINANT_DB_NAME)
        df = CDB.Utils.UtilsDB.queryDominant(sql)
        df = df.rename(columns={'code': 'SecuCode', 'trade_date': 'TradingDay'})
        df = df.pivot_table(columns='SecuCode', index='TradingDay', values=strColumn, aggfunc=lambda x: ' '.join(x))
    else:
        df = dfExe[strColumn].reset_index().pivot_table(columns='SecuCode', index='TradingDay', values=strColumn, aggfunc=lambda x: ' '.join(x))

    return df

def getDFOneProduct(SecuCode, listColumn, dtStart=dtBackTestStart):
    if boolUsingDB:
        strColumn = ','.join(listColumn)
        sql = 'select code, trade_date, {1} from {3} where code = "{0}" and trade_date >= "{2}";'.format(SecuCode, strColumn, dtStart.strftime('%Y%m%d'), CDB.Utils.UtilsDB.DAILY_DOMINANT_DB_NAME)
        sql = sql.replace("Open-PreSettle", "`Open-PreSettle`")
        sql = sql.replace("Settle-Open", "`Settle-Open`")

        df = CDB.Utils.UtilsDB.queryDominant(sql)
        df = df.rename(columns={'code': 'SecuCode', 'trade_date': 'TradingDay'})
        df = df.set_index('TradingDay').sort_index()
        if len(listColumn) == 1:
            s = df[listColumn[0]]
            s.name = listColumn[0]
            return s
    else:
        if len(listColumn) == 1:
            s = dfExe.ix[SecuCode][listColumn[0]]
        else:
            df = dfExe.ix[SecuCode][listColumn]

    return df

############
# dtLastData
############
if boolUsingDB:
    dtLastData = CDB.Utils.UtilsDB.getDTLastData()
else:
    if dfExe is not None:
        dtLastData = dfExe.index.get_level_values('TradingDay').max()


############
# wrapper
############
def funcWriteExcel(df, excel_writer, sheet_name='Sheet1'):
    DumpExcel.funcWriteExcel(df, excel_writer, sheet_name)

listSecuAll = ParamRange.listSecuAll
dictStrategyParamRange = ParamRange.dictStrategyParamRange
dictNDayTest_NWeekStart = ParamRange.dictNDayTest_NWeekStart
dictNDayTest_NMonthStart = ParamRange.dictNDayTest_NMonthStart

############
# functions
############
def getTradingDataPoint_Commodity(dictDataSpec):
    if boolUsingDB:
        return getTradingDataPoint_Commodity_DB(dictDataSpec)
    else:
        return getTradingDataPoint_Commodity_File(dictDataSpec)

def getTradingDataPoint_Commodity_DB(dictDataSpec):
    freq = dictDataSpec['freq']
    Secu = dictDataSpec['Secu']
    if freq == '1day':
        import CommodityDataBase as CDB
        reload(CDB)
        df = CDB.Utils.UtilsDB.readDB(CDB.Utils.UtilsDB.DAILY_DOMINANT_DB_NAME, Secu, dtBackTestStart-datetime.timedelta(365*2, 1))
        #df['trade_date'] = df['trade_date'].apply(lambda x: parse(str(int(x))))
        df['Close'] = (1+df['PCT']).cumprod()
        df['High'] = df['HighRaw'] / df['CloseRaw'] * df['Close']
        df['Low'] = df['LowRaw'] / df['CloseRaw'] * df['Close']
        df['Open'] = df['OpenRaw'] / df['CloseRaw'] * df['Close']
        df['Settle'] = df['SettleRaw'] / df['CloseRaw'] * df['Close']
        df = df.set_index('trade_date').sort_index()
        df = df.rename(columns={'TurnoverVolume': 'Volume'})
        df.index.name = 'dtEnd'
        return df

    elif freq == '1min':
        import CommodityDataBase as CDB
        reload(CDB)
        strDirCache = dirDataSource + '/Cache1min/'
        if os.path.exists(strDirCache) is False:
            os.mkdir(strDirCache)


        # vwp 
        if dictDataSpec.has_key('VBB') and dictDataSpec['VBB']==True:
            strFileCacheVBB = strDirCache + Secu + '_vwp_%d.pickle'%dictDataSpec['VBBMinPerBar']
            if os.path.exists(strFileCacheVBB):
                df = pd.read_pickle(strFileCacheVBB)
            else:
                # time based bar
                strFileCache = strDirCache + Secu + '.pickle'
                if os.path.exists(strFileCache):
                    df = pd.read_pickle(strFileCache)
                else:
                    df = CDB.Utils.UtilsDB.readDB1minDominant(CDB.Utils.UtilsDB.strMySQLDB, Secu)
                    df = df.set_index('datetime')
                    df.index.name = 'dtEnd'
                    df['Close'] = (df['PCT']+1).cumprod()
                    df['High'] = df['HighRaw'] / df['CloseRaw'] * df['Close']
                    df['Low'] = df['LowRaw'] / df['CloseRaw'] * df['Close']
                    df['Open'] = df['OpenRaw'] / df['CloseRaw'] * df['Close']
                    df.to_pickle(strFileCache)

                # read 1min data
                df = calculateVolumeBasedBar(df, dictDataSpec['VBBMinPerBar'])
                df.to_pickle(strFileCacheVBB)
        else:
            # time based bar
            strFileCache = strDirCache + Secu + '.pickle'
            if os.path.exists(strFileCache):
                df = pd.read_pickle(strFileCache)
            else:
                df = CDB.Utils.UtilsDB.readDB1minDominant(CDB.Utils.UtilsDB.strMySQLDB, Secu)
                df = df.set_index('datetime')
                df.index.name = 'dtEnd'
                df['Close'] = (df['PCT']+1).cumprod()
                df['High'] = df['HighRaw'] / df['CloseRaw'] * df['Close']
                df['Low'] = df['LowRaw'] / df['CloseRaw'] * df['Close']
                df['Open'] = df['OpenRaw'] / df['CloseRaw'] * df['Close']
                df.to_pickle(strFileCache)

        df = df[df.index >= dtBackTestStart]
        return df

def getTradingDataPoint_Commodity_File(dictDataSpec):
    freq = dictDataSpec['freq']
    Secu = dictDataSpec['Secu']
    if freq == '1day':
        strFileAddressAll = dictDataSpec['strFilePrefix'] + '/' + freq + '/' + strDailyData

        dfXY = pd.read_pickle(strFileAddressAll)
        dfXY = dfXY.ix[Secu]
        dfXY = dfXY.rename(columns={
                'OpenPrice': 'Open',
                'HighPrice': 'High',
                'LowPrice': 'Low',
                'ClosePrice': 'Close',
                'TurnoverVolume': 'Volume',
                })
        dfXY.index.name = 'dtEnd'
        dfXY = dfXY[['Open', 'High', 'Low', 'Close', 'Volume', 'OpenRaw', 'boolDominantChange']].astype(np.float)
        return dfXY
    elif freq == '1min':
        strFilePrefix = dirDataSource_1min
        strFileAddressAll = strFilePrefix + '/' + Secu + '.pickle'
        dfXY = pd.read_pickle(strFileAddressAll)
        listCol = ['Open', 'High', 'Low', 'Close', 'Volume']
        dfXY[listCol] = dfXY[listCol].astype(np.float)
        dfXY.index.name = 'dtEnd'
    return dfXY

def calculateVolumeBasedBar(df, VBBMinPerBar):
    # find prev volume
    df['trade_date'] = df.index.strftime('%Y%m%d')
    sDailyVolume = df.reset_index().groupby('trade_date')['Volume'].sum()
    df = df.reset_index().set_index('trade_date')
    sDailyVolumePre = sDailyVolume.shift(1).fillna(sDailyVolume.mean())

    # split
    df = df.reset_index().set_index('dtEnd').sort_index()
    def funcSplitVBB(df, sDailyVolumePre):
        print df.name
        if df.index.size < VBBMinPerBar * 5:
            listColumnOut = ['Open', 'High', 'Low', 'Close', 'Volume', 'vwp']
            df['vwp'] = df['Close']
            df = df.reset_index()
            return df[listColumnOut]
        # param
        timeMarketEndMorning = datetime.time(11,30)
        # find start & end of each bin
        volumeYesterday = sDailyVolumePre.ix[df.name]
        volumePerInterval = volumeYesterday / (df.index.size / VBBMinPerBar)
        volumePerInterval = int(volumePerInterval)
        df['volumeCumulated'] = df['Volume'].cumsum() % volumePerInterval
        df['volumeCumulatedPrev'] = df['volumeCumulated'].shift(1)
        df.ix[0, 'volumeCumulatedPrev'] = volumeYesterday
        listTimePeriodStart = df[df['volumeCumulated'] < df['volumeCumulatedPrev']].index.tolist()
        listTimePeriodEnd = list(listTimePeriodStart[1:])
        listTimePeriodEnd.append(df.index[-1])
        # calculate bar
        listDict = []
        for nInterval in range(0, len(listTimePeriodStart)):
            # get the current period data
            dtPeriodStart = listTimePeriodStart[nInterval]
            dtPeriodEnd = listTimePeriodEnd[nInterval]
            if dtPeriodStart == dtPeriodEnd:
                mask = (df.index == dtPeriodEnd)
            else:
                mask = (df.index >= dtPeriodStart) & (df.index < dtPeriodEnd)
            dfPeriod = df[mask]

            # calculate OHLCV
            dictOne = {}
            dictOne['Open'] = dfPeriod.ix[0, 'Open']
            dictOne['High'] = dfPeriod['High'].max()
            dictOne['Low'] = dfPeriod['Low'].min()
            dictOne['Close'] = dfPeriod.ix[-1, 'Close']
            dictOne['Volume'] = dfPeriod['Volume'].sum()
            dictOne['vwp'] = (dfPeriod['Close'] * dfPeriod['Volume']).sum() / dfPeriod['Volume'].sum()
            dictOne['dtEnd'] = dfPeriod.index[-1]
            listDict.append(dictOne)
        ret = pd.DataFrame(listDict)
        return ret

    df = df.groupby('trade_date').apply(funcSplitVBB, sDailyVolumePre)
    # cast for old format
    df = df.reset_index().set_index('dtEnd')
    if 'level_1' in df.columns:
        df = df.drop('level_1', axis=1)
    return df


###################
# rebalance date
###################
def generateTimeStandard_Before20180321(NDayShift):
    dictDataSpec = {'freq': '1day','Secu': 'a.dce', 'strModelName': 'TSM', 'strFilePrefix': dirDataSource, 'boolGenerateTimeStandard': True}

    dfAll = getTradingDataPoint_Commodity(dictDataSpec).replace(np.inf, np.nan).dropna()
    return dfAll.ix[NDayShift:]

def generateTimeStandard():
    import RoyalMountain.TradingDay as TradingDay
    dfAll = TradingDay.getTradingDayCommodity()
    return dfAll

def generateDTTestStartCalendarDay(NDayTest, NDayShift, NWeekStart):
    dfForDTTestStart = generateTimeStandard()
    dfForDTTestStart = dfForDTTestStart[dfForDTTestStart.index >= dtBackTestStart]
    listDTTestStart = []
    dtTestStart = dfForDTTestStart.index[0]
    for dt in dfForDTTestStart.index:
        if (dt - dtTestStart).days > NDayTest and dt.weekday() == NDayShift:
            dtTestStart = dt
            listDTTestStart.append(dtTestStart)
    # shift 1 week
    ret = []
    for dt in listDTTestStart:
        dtShifted = dt + datetime.timedelta(7 * NWeekStart, 0)
        for n in [0, 7]:
            dtShifted = dtShifted + datetime.timedelta(n, 0)
            if dtShifted in dfForDTTestStart.index:
                ret.append(dtShifted)
                break
    listDTTestStart = ret
    return listDTTestStart

def generateNthFriday(NthFriday, NMonthStart, NDayShift=4):
    dfForDTTestStart = generateTimeStandard()
    listDTTestStart = []
    listDT = pd.date_range(datetime.datetime(2004, 1, 1), dfForDTTestStart.index.max())
    dfDTFull = pd.Series(range(0, len(listDT)), index=listDT)
    def funcThridFriday(df, NthFriday):
        dfTemp = df[df.index.weekday==NDayShift]
        if dfTemp.index.size >= NthFriday:
            ret = dfTemp.index[NthFriday-1]
        else:
            ret = None
        return ret
    sDTTestStart = dfDTFull.groupby(dfDTFull.index.strftime('%Y%m')).apply(funcThridFriday, NthFriday).dropna()
    sDTTestStart = sDTTestStart[NMonthStart::2].tolist()
    ret = []
    for dt in sDTTestStart:
        for n in range(0, 10):
            dtTrading = dt + datetime.timedelta(n, 0)
            if dtTrading in dfForDTTestStart.index and dtTrading.weekday()==NDayShift:
                ret.append(dtTrading)
                break
    return ret

def generateNSecu():
    if boolUsingDB:
        sql = 'select code, min(trade_date) as trade_date from VnTrader_Daily_Dominant_Db group by code;'
        dfRaw = CDB.Utils.UtilsDB.queryDominant(sql)
        dfRaw = dfRaw.rename(columns={'code':'SecuCode','trade_date':'TradingDay'})
        sSecuFirstTrading = dfRaw.set_index('SecuCode').sort_index()
    else:
        sSecuFirstTrading = dfExe.reset_index().groupby('SecuCode')['TradingDay'].first()

    dfSecuFirstTrading = pd.DataFrame(sSecuFirstTrading)
    dfSecuFirstTrading = dfSecuFirstTrading.reset_index().set_index('TradingDay').sort_index()
    dfSecuFirstTrading['NSecu'] = 1
    dfSecuFirstTrading['NSecu'] = dfSecuFirstTrading['NSecu'].cumsum()
    sNSecu = dfSecuFirstTrading['NSecu']
    dtToday = datetime.datetime.combine(datetime.datetime.now(), datetime.time(0,0))
    sNSecu.ix[dtToday] = np.nan
    sNSecu = sNSecu.resample('1D').last().ffill()
    sNSecu.index = sNSecu.index.to_datetime()
    return sNSecu


############
# functions
############
def extractElementFromList(d):
    ret = {}
    listKException = ['Secu', 'listFileAddress']
    for k in d.keys():
        v = d[k]
        if k not in listKException:
            ret[k] = v[0]
        else:
            ret[k] = v

    return ret

def sweepParam(dictDefault, dictParamRange):
    # sweep paramter range
    listDictToExpand = [dictDefault]
    listKey = dictParamRange.keys()
    for strParam in listKey:
        listValueParam = dictParamRange[strParam]
        listDictExpanded = []
        for vParam in listValueParam:
            for dictToExpand in listDictToExpand:
                dictOne = dict(dictToExpand)
                dictOne[strParam] = vParam
                listDictExpanded.append(dictOne)
        listDictToExpand = listDictExpanded
    listDictTaskSetting = listDictToExpand

    # exceptions
    listDictTaskSettingFiltered = []
    for dictOne in listDictTaskSetting:
        if dictOne.has_key('NWeekStart') and dictOne['NWeekStart'] > max(dictNDayTest_NWeekStart[dictOne['NDayTest']]):
            continue 
        if dictOne.has_key('NMonthStart') and dictOne['NMonthStart'] > max(dictNDayTest_NMonthStart[dictOne['NDayTest']]):
            continue 
        listDictTaskSettingFiltered.append(dictOne)
    listDictTaskSetting = listDictTaskSettingFiltered

    # generate strCase
    listKey = listDictTaskSetting[0].keys()
    listKey = np.sort(listKey).tolist()
    for dictOne in listDictTaskSetting:
        strCase = ''
        for k in listKey:
            v = dictOne[k]
            if k in dictParamRange.keys():
                strCase = strCase + str(k) + str(v) + '_'
        dictOne['strCase'] = strCase[:-1]
    
    # determine boolStoploss
    for dictOne in listDictTaskSetting:
        if dictOne.has_key('decimalStoploss') and dictOne['decimalStoploss'] < 0.99:
            dictOne['boolStoploss'] = True
            
    return listDictTaskSetting

def sweepParamForTopPort(listDictDataSpec):
    listDictForTopPort = []
    SecuUsed = listDictDataSpec[0]['Secu']
    for dictDataSpec in listDictDataSpec:
        if SecuUsed != dictDataSpec['Secu']:
            continue

        dictDataSpecNew = {}
        for k, v in dictDataSpec.iteritems():
            dictDataSpecNew[k] = [v]
        listDictForTopPort.append(dictDataSpecNew)
            
    return listDictForTopPort

def getFileAddressForTopPort(strParamSweep):
    listDictTaskSetting = sweepParam(dictDataSpecTemplate, dictStrategyParamRange[strParamSweep])
    listDictForTopPort = sweepParamForTopPort(listDictTaskSetting)  # each dict has single param set for later TopPort

    listDictPerTopPort = []
    for dictForTopPort in listDictForTopPort:
        listFileAddress = []
        dictForTopPort = dict((key,value) for key, value in dictForTopPort.iteritems() if key in dictStrategyParamRange[strParamSweep])
        #dictForTopPort['Secu'] = listSecuAll
        dictForTopPort['Secu'] = dictStrategyParamRange[strParamSweep]['Secu']
        dictPerTopPort = dict(dictForTopPort)
        listDictDataSpec = sweepParam(dictDataSpecTemplate, dictForTopPort)
        for dictDataSpec in listDictDataSpec:
            strFileAddress = dirResultPerCase + '/' + dictDataSpec['strCase'] + '.pickle'
            listFileAddress.append(strFileAddress)
        dictPerTopPort['listFileAddress'] = listFileAddress
        dictPerTopPort['strCase'] = listDictDataSpec[0]['strCase']
        listDictPerTopPort.append(dictPerTopPort)
        
    return listDictPerTopPort
    
###################
# metric 
###################
def funcMetric(sPCT, boolOutColumn=False):
    """
    calculate performance of one daily return series
    
    Parameters:
    ----------
    * sPCT: daily return series
                                            
    Returns:
    -------
    * ret: including all the metrics and the daily return series
    """
    if sPCT.dropna().empty:
        return {}
    sValue = (sPCT+1).cumprod()
    ValueFinal = sValue.dropna().ix[-1]
    sMax = sValue.expanding().max()
    sDD = sValue / sMax - 1
    Ret = np.power(ValueFinal, 252./sPCT.index.size) - 1
    Vol = sPCT.std() * np.sqrt(252.)
    DD = -sDD.min()
    ret = {}
    ret['Ret'] = Ret
    ret['Vol'] = Vol
    ret['DD'] = DD
    ret['SR'] = Ret / Vol
    ret['CR'] = Ret / DD
    ret['ValueFinal'] = ValueFinal
    listOutCommon = ['Ret', 'Vol', 'DD', 'SR', 'CR', 'ValueFinal']
    # rolling return 
    listOutRR = []
    for NDayRolling in [20, 40, 60, 120, 240]:
        sRollingReturn = sValue / sValue.shift(NDayRolling) - 1
        ret['RollingReturnMin%02d'%NDayRolling] = sRollingReturn.min()
        ret['RollingReturn5PCT%02d'%NDayRolling] = sRollingReturn.quantile(0.05)
        listOutRR.append('RollingReturnMin%02d'%NDayRolling)
        listOutRR.append('RollingReturn5PCT%02d'%NDayRolling)

    '''
    # successive profit/loss day
    dfNDayProfitCount = pd.concat([sPCT], 1)
    dfNDayProfitCount.columns = ['PCTDaily']
    dfNDayProfitCount['NDayProfitSuccessive'] = 0
    dfNDayProfitCount['NDayLossSuccessive'] = 0
    rowPrev = dfNDayProfitCount.ix[0]
    for ix, row in dfNDayProfitCount[1:].iterrows():
        if row['PCTDaily'] >= 0:
            dfNDayProfitCount.ix[ix, 'NDayProfitSuccessive'] = rowPrev['NDayProfitSuccessive'] + 1
        else:
            dfNDayProfitCount.ix[ix, 'NDayProfitSuccessive'] = 0
        
        if row['PCTDaily'] <= 0:
            dfNDayProfitCount.ix[ix, 'NDayLossSuccessive'] = rowPrev['NDayLossSuccessive'] + 1
        else:
            dfNDayProfitCount.ix[ix, 'NDayLossSuccessive'] = 0
        rowPrev = dfNDayProfitCount.ix[ix]
    ret['NDayProfitSuccessive'] = dfNDayProfitCount['NDayProfitSuccessive'].max()
    ret['NDayLossSuccessive'] = dfNDayProfitCount['NDayLossSuccessive'].max()
    listOutSuccessive = ['NDayProfitSuccessive', 'NDayLossSuccessive']

    # longest drawdown days
    dfNDayDD = pd.concat([sValue-sMax], 1)
    dfNDayDD.columns = ['PCTBelowMax']
    dfNDayDD['NDayDD'] = 0
    rowPrev = dfNDayDD.ix[0]
    for ix, row in dfNDayDD[1:].iterrows():
        if row['PCTBelowMax'] < 0:
            dfNDayDD.ix[ix, 'NDayDD'] = rowPrev['NDayDD'] + 1
        else:
            dfNDayDD.ix[ix, 'NDayDD'] = 0
        rowPrev = dfNDayDD.ix[ix]
    dfNDayDD = dfNDayDD.sort_values('NDayDD', ascending=False)
    listOutDD = []
    for nIndex in range(0, 3) :
        index = dfNDayDD.index[nIndex]
        ret['NDayDD%d'%nIndex] = dfNDayDD.ix[index, 'NDayDD']
        ret['DTNDayDD%d'%nIndex] = index
        listOutDD.append('NDayDD%d'%nIndex)
        listOutDD.append('DTNDayDD%d'%nIndex)

    if boolOutColumn:
        listColumn = listOutCommon + listOutRR + listOutSuccessive + listOutDD
        ret = ret, listColumn
    #'''
    return ret


def funcCalcPort(listStrParamSweep, dtBackTestStart=None):
    """
    calculate performance of portfolio specified in listStrParamSweep
    
    Parameters:
    ----------
    * listStrParamSweep (list or str): which strategies are included. 
    the strategies available are listed in Common/ParamRange.py
    * dtBackTestStart (datetime): back test start date
                                            
    Returns:
    -------
    * dfRet: metrics of all strategies included in listStrParamSweep
    * sV: the daily value of the strategies
    """


    if type(listStrParamSweep) != list:
        listStrParamSweep = [listStrParamSweep]

    # get all dictPerTopPort
    listS = []
    for strParamSweep in listStrParamSweep:
        ld = getFileAddressForTopPort(strParamSweep)
        for dictPerTopPort in ld:
            strCase = dictPerTopPort['strCase']
            
            if boolUsingDB:
                sPortConfig = extractElementFromList(dictPerTopPort)
                sPortConfig = pd.Series(sPortConfig)
                strModelName = sPortConfig['strModelName']
                listToRemove = ['strCloseAtDayEnd', 'strCase', 'Secu', 'listFileAddress']
                listColumnToDB = list(set(dictPerTopPort.keys()).difference(set(listToRemove)))
                sPortConfig = sPortConfig[listColumnToDB]
            
                strTB = UtilsDB.strTBPrefixPerformance + strModelName
                dfPerformance = UtilsDB.readPerformance(UtilsDB.DB_NAME_PERFORMANCE, strTB, dtBackTestStart)
                dfPerformance = dfPerformance.rename(columns={'code': 'strModelName', 'trade_date':'TradingDay'})
                listK = set(sPortConfig.keys()).intersection(dfPerformance.columns.tolist())
                for k in listK:
                    dfPerformance = dfPerformance[dfPerformance[k]==sPortConfig[k]]
                dfPerformance = dfPerformance.set_index('TradingDay').sort_index()
                sDailyReturn = dfPerformance['PCT']

                sPortConfig.ix['sDailyReturn'] = sDailyReturn
                sMetric = funcMetric(sDailyReturn.ix[:-1])
                sPortConfig = sPortConfig.append(pd.Series(sMetric))
                listS.append(sPortConfig.to_dict())

            else:
                strFileAddressPrefix = '%s/%s/%s/'%(dirResultPerCase, strParamSweep, strCase)
                sPortConfig = pd.read_pickle('%s/portconfig.pickle'%(strFileAddressPrefix))
                sDailyReturn = (pd.read_pickle('%s/dfOut.pickle'%(strFileAddressPrefix))['Cum Return'] + 1).pct_change()
                sDailyReturn.name = strCase
                # dtBackTestStart
                if dtBackTestStart is None:
                    dtBackTestStart = sDailyReturn.index.min()
                sDailyReturn = sDailyReturn[sDailyReturn.index >= dtBackTestStart]

                sPortConfig.ix['sDailyReturn'] = sDailyReturn
                sMetric = funcMetric(sDailyReturn.ix[:-1])
                sPortConfig = sPortConfig.append(pd.Series(sMetric))
                listS.append(sPortConfig.to_dict())
    
    # build common index
    ix = listS[0]['sDailyReturn'].index

    for dictOne in listS:
        ix = ix.union(dictOne['sDailyReturn'].index)
    for n, d in enumerate(listS):
        listS[n]['sDailyReturn'] = d['sDailyReturn'].ix[ix].fillna(0)
    
    # calculate daily return of port
    dfPCT = pd.DataFrame(listS)['sDailyReturn'].tolist()
    dfPCT = pd.concat(dfPCT, axis=1).ix[:-1]
    dfPCT['Port'] = dfPCT.mean(1)

    #raise Exception
    dfRet = pd.DataFrame(listS)
    dfRet = dfRet[dfRet.columns.drop('sDailyReturn')]
    
    # port value
    s = dfPCT['Port']
    sV = (s+1).cumprod()
    dfV = (dfPCT + 1).cumprod()

    # 
    sMetric = funcMetric(s)
    print sMetric
    
    # Worst Case Test
    for NDayRolling in [1, 5, 10, 20, 60, 120, 240]:
        sPCT = sV / sV.shift(NDayRolling) - 1
        print 'NDayRolling:%03d,\tMin:%0.2f,\t1PTile:%0.2f,\t2PTile:%0.2f,\t5PTile:%0.2f,\t10PTile:%0.2f'%(
            NDayRolling,
            sPCT.min(), 
            sPCT.quantile(0.01),
            sPCT.quantile(0.02),
            sPCT.quantile(0.05),
            sPCT.quantile(0.10)
            )
    
    return dfRet, sV
    
#########################################
# plot
#########################################
def plotCandleStick(df):
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.finance import candlestick_ohlc

    data = df.reset_index()
    data['MA20'] = data['Close'].rolling(20).mean()
    data['Date2'] = data['TradingDay'].apply(lambda d: mdates.date2num(d.to_pydatetime()))
    tuples = [tuple(x) for x in data[['Date2','Open','High','Low','Close']].values]

    fig, ax = plt.subplots()
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.xticks(rotation=45)
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.title(df.name)
    candlestick_ohlc(ax, tuples, width=.6, colorup='r', colordown='g', alpha =.4);

    ixLong = data[data['indicator']==1].index
    ixShort = data[data['indicator']==-1].index
    plt.plot(data.ix[ixLong, 'Date2'], data.ix[ixLong, 'Close'], 'r^')
    plt.plot(data.ix[ixShort, 'Date2'], data.ix[ixShort, 'Close'], 'gv')
    plt.plot(data['Date2'], data['MA20'], 'k')
    plt.show()

    return


#########################################
# directory
######################################### 
def funcClearData(strParamSweep):
    ld = getFileAddressForTopPort(strParamSweep)
    for n, dictPerTopPort in enumerate(ld):
        strDirAddress = '%s/%s/%s'%(dirResultPerCase, strParamSweep, ld[n]['strCase'])
        if os.path.exists(strDirAddress):
            shutil.rmtree(strDirAddress)
        for strFileAddress in dictPerTopPort['listFileAddress']:
            if os.path.exists(strFileAddress):
                os.remove(strFileAddress)


if __name__ == '__main__':
    sNSecu = sNSecu

