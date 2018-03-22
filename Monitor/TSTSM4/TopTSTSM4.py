import os, sys, shutil
import pandas as pd
import numpy as np
import datetime

import ThaiExpress.Common.Utils as Utils
reload(Utils)

# parameter
strMonitorDirectory = 'TSTSM4'

strParamSweepTSM = 'TSM_TSTSM4'
strParamSweepTS = 'TS_TSTSM4'
dictBoolStoploss = {
    'TSM': False,
    'TS': False,
    }
dictAdjustPosition = {
    'TSM0': True,   # 20170721, 20170804, 20170818, 20170901, 20170915, 20170929, 20171013, 20171027, 20171110, 20171123, 20171207, 20171221, 20180104, 20180118
    'TSM1': True,
    'TS0': False,   # Jan, March, May, July, September, November
    'TS1': False,
    }

#'''
# clear data
if Utils.boolClearData:
    Utils.funcClearData(strParamSweepTSM)
    Utils.funcClearData(strParamSweepTS)

# concat data
print 'concat'
os.chdir('../../Prepdata/')
if Utils.boolMonitor:
    os.system('python concat.py')
# back test
print 'TSM'
os.chdir('../Config/TSM')
os.system('python TopTSM.py %s'%strParamSweepTSM)

print 'Structure Rate'
os.chdir('../TS')
if Utils.boolMonitor:
    os.system('python UtilsTS.py')
print 'TS'
os.system('python TopTS.py %s'%strParamSweepTS)

os.chdir('../../')
os.system('python Weight/TopPortSync.py %s'%strParamSweepTSM)
os.system('python Weight/TopPortSync.py %s'%strParamSweepTS)

os.chdir('Monitor/%s'%strMonitorDirectory)
#'''


# getFileAddressForTopPort
ldTSM = Utils.getFileAddressForTopPort(strParamSweepTSM)
ldTS = Utils.getFileAddressForTopPort(strParamSweepTS)

# generate daily report
strFileAddress = '%s/%s/%s/Check.xlsx'%(Utils.dirResultPerCase, strParamSweepTSM, ldTSM[0]['strCase'])
sDailyReturnTSM = pd.read_excel(strFileAddress, sheetname='DailyReturn', index_col='dtEnd')
dtToday = sDailyReturnTSM.index[-2]
dtTomorrow = sDailyReturnTSM.index[-1]
excel_writer = pd.ExcelWriter('DailyReport{0}_TOTALMONEY{1}.xlsx'.format(dtToday.strftime('%Y%m%d'), int(Utils.TOTALMONEY)))
workbook = excel_writer.book
ExcelFormat = workbook.add_format()
ExcelFormat.set_text_wrap()

# 
listDictPerTopPort = []
for n, dictPerTopPort in enumerate(ldTSM):
    listDictPerTopPort.append({
        'strStrategy': 'TSM%d'%n, 
        'strFileAddress': '%s/%s/%s/Check.xlsx'%(Utils.dirResultPerCase, strParamSweepTSM, ldTSM[n]['strCase'])
        })
for n, dictPerTopPort in enumerate(ldTS):
    listDictPerTopPort.append({
        'strStrategy': 'TS%d'%n, 
        'strFileAddress': '%s/%s/%s/Check.xlsx'%(Utils.dirResultPerCase, strParamSweepTS, ldTS[n]['strCase'])
        })

for nStrategy in range(0, len(listDictPerTopPort)):
    dictPerTopPort = listDictPerTopPort[nStrategy]
    strStrategy = dictPerTopPort['strStrategy']
    strFileAddress = dictPerTopPort['strFileAddress']

    # extract rows
    rowWeightDirection = pd.read_excel(strFileAddress, sheetname='WeightDirection', index_col='dtEnd').ix[-2]
    rowPositionDirection = pd.read_excel(strFileAddress, sheetname='PositionDirection', index_col='dtEnd').ix[-2]
    rowPositionDirectionTomorrow = pd.read_excel(strFileAddress, sheetname='PositionDirection', index_col='dtEnd').ix[-1]
    rowReturn = pd.read_excel(strFileAddress, sheetname='ReturnValid', index_col='dtEnd').ix[-2]
    rowDominantChange = pd.read_excel(strFileAddress, sheetname='DominantChange', index_col='TradingDay').ix[-1]
    rowDominantChangeTomorrow = pd.read_excel(strFileAddress, sheetname='DominantChangeTomorrow', index_col='TradingDay').ix[-1]
    rowContractCode = pd.read_excel(strFileAddress, sheetname='ContractCodeFullRename', index_col='TradingDay').ix[-1]
    rowContractCodeTomorrow = pd.read_excel(strFileAddress, sheetname='ContractCodeTomorrow', index_col='TradingDay').ix[-1]
    listRow = [rowWeightDirection, rowPositionDirection, rowPositionDirectionTomorrow, rowReturn, rowDominantChange, rowDominantChangeTomorrow, rowContractCode, rowContractCodeTomorrow]
    listRowName = ['Weight', 'Contract', 'ContractTomorrow', 'Return(%)', 'DominantChange', 'DominantChangeTomorrow', 'ContractCode', 'ContractCodeTomorrow']
    if dictBoolStoploss['TS']:
        rowStoploss = pd.read_excel(strFileAddress, sheetname='Stoploss', index_col='dtEnd').ix[-2]
        rowStoplossPrice = pd.read_excel(strFileAddress, sheetname='StoplossPrice', index_col='dtEnd').ix[-2]
        rowStoplossPriceTomorrow = pd.read_excel(strFileAddress, sheetname='StoplossPriceTomorrow', index_col='dtEnd').ix[-2]
        listRow = listRow + [rowStoploss, rowStoplossPrice, rowStoplossPriceTomorrow]
        listRowName = listRowName + ['Stoploss', 'StoplossPrice', 'StoplossPriceTomorrow']
    dfResult = pd.concat(listRow, axis=1)
    dfResult.columns = listRowName
    
    # dump to excel
    Utils.funcWriteExcel(dfResult, excel_writer, sheet_name=strStrategy)
    dfResult = dfResult.fillna(0)
        
    dictPerTopPort['dfResult'] = dfResult
    sDailyReturn = pd.read_excel(strFileAddress, sheetname='DailyReturn', index_col='dtEnd')
    sDailyReturn = sDailyReturn[sDailyReturn.columns[0]]
    sDailyReturn.name = strStrategy
    dictPerTopPort['sDailyReturn'] = sDailyReturn
    
    #s = pd.read_excel(strFileAddress, sheetname='DollarMarginDaily', index_col='dtEnd')['Total']
    #s.name = strStrategy
    #dictPerTopPort['sDollarMarginDaily'] = s
    df = pd.read_excel(strFileAddress, sheetname='WeightDirection', index_col='dtEnd')
    df.name = strStrategy
    dictPerTopPort['dfWeightDirection'] = df

# SUM
dfResultSum = None
lSum = ['Contract', 'DominantChange', 'DominantChangeTomorrow']
for dictPerTopPort in listDictPerTopPort:
    dfResult = dictPerTopPort['dfResult'].drop('Return(%)', 1)
    if dfResultSum is None:
        dfResultSum = dfResult
    else:
        dfResultSum[lSum] = dfResultSum[lSum] + dfResult[lSum]
dfResultSum = dfResultSum[lSum + ['ContractCode', 'ContractCodeTomorrow']]
Utils.funcWriteExcel(dfResultSum, excel_writer, sheet_name='SUM Summary')

# Daily Return of Port
print 'producing daily report'
listSDailyReturn = []
for dictPerTopPort in listDictPerTopPort:
    sDailyReturn = dictPerTopPort['sDailyReturn']
    listSDailyReturn.append(sDailyReturn)
dfDailyReturn = pd.concat(listSDailyReturn, 1).ix[:-1]
dfDailyReturn['PortPCT'] = dfDailyReturn.mean(1)
dfDailyReturn['PortValue'] = (dfDailyReturn['PortPCT'] + 1).cumprod()
dfDailyReturn['PortDD'] = dfDailyReturn['PortValue'] / dfDailyReturn['PortValue'].expanding().max() - 1
Utils.funcWriteExcel(dfDailyReturn, excel_writer, sheet_name='DailyReturn')

## Daily Margin
#listS = []
#for dictPerTopPort in listDictPerTopPort:
#    s = dictPerTopPort['sDollarMarginDaily']
#    listS.append(s)
#dfDollarMarginDaily = pd.concat(listS, 1).ix[:-1]
#dfDollarMarginDaily['Total'] = dfDollarMarginDaily.sum(1)
#dfDollarMarginDaily['Leverage'] = dfDollarMarginDaily['Total'] / (Utils.TOTALMONEY * 4)
#dfDollarMarginDaily = dfDollarMarginDaily[dfDollarMarginDaily.index>=Utils.dtBackTestStart]
#Utils.funcWriteExcel(dfDollarMarginDaily, excel_writer, sheet_name='DailyMargin')

# Daily Position
listS = []
dfWeightDirectionTotal = pd.DataFrame()
for dictPerTopPort in listDictPerTopPort:
    dfWeightDirection = dictPerTopPort['dfWeightDirection']
    dfWeightDirection = dfWeightDirection[dfWeightDirection.index>Utils.dtBackTestStart]
    dfWeightDirection.name = dictPerTopPort['dfWeightDirection'].name
    Utils.funcWriteExcel(dfWeightDirection, excel_writer, sheet_name='DailyPosition_%s'%dfWeightDirection.name)

    if dfWeightDirectionTotal.empty:
        dfWeightDirectionTotal = dfWeightDirection
    else:
        dfWeightDirectionTotal = dfWeightDirectionTotal + dfWeightDirection
dfWeightDirectionTotal['Total'] = dfWeightDirectionTotal.abs().sum(1)
dfWeightDirectionTotal = dfWeightDirectionTotal[dfWeightDirectionTotal.index>=Utils.dtBackTestStart]
Utils.funcWriteExcel(dfWeightDirectionTotal, excel_writer, sheet_name='DailyPosition')

# calculate daily return from dfPositionDirection and Utils.dfExe.ix[][['Open-PreSettle', 'DeltaSettle']]
listDollarReturn = []
for nStrategy in range(0, len(listDictPerTopPort)):
    dictPerTopPort = listDictPerTopPort[nStrategy]
    strStrategy = dictPerTopPort['strStrategy']
    strFileAddress = dictPerTopPort['strFileAddress']
    df = pd.read_excel(strFileAddress, sheetname='EquityDeltaDaily')
    sTotalDaily = df['Total']
    sTotalDaily.name = strStrategy
    listDollarReturn.append(sTotalDaily)
dfDollarReturn = pd.concat(listDollarReturn, 1)
sV = dfDollarReturn.sum(1)
sDollar = sV.copy()
sDollar.name = 'DollarReturn'
sPCT = sV / (Utils.TOTALMONEY * 4)
sV = (1+sPCT).cumprod()
sPCT.name = 'DollarPCT'
sV.name = 'DollarV'
sPCTFee = sPCT - 0.25/1e4
sVFee = (sPCTFee+1).cumprod()
sPCTFee.name = 'DollarPCTFee'
sVFee.name = 'DollarVFee'
dfDollarPCT = pd.concat([sPCT, sV, sPCTFee, sVFee, sDollar], axis=1)
Utils.funcWriteExcel(dfDollarPCT.dropna(), excel_writer, sheet_name='DollarPCT')

# calculate rolling return and metrics
if Utils.boolMonitor is False:
    dt2005 = datetime.datetime(2005,1,1)
    dt2010 = datetime.datetime(2010,1,1)
    dt2015 = datetime.datetime(2015,1,1)
    listDictMetric = []
    for dt in [dt2005, dt2010, dt2015]:
        dMetric, listOut = Utils.funcMetric(sPCT[sPCT.index>dt], True)
        dMetric['Fee'] = 0.0
        dMetric['dtStart'] = dt
        listDictMetric.append(dMetric)
        '''
        for fee in [0.25, 0.3, 0.4, 0.5]:
            dMetric, listOut = Utils.funcMetric(sPCT[sPCT.index>dt]-fee/1e4, True)
            dMetric['Fee'] = fee
            dMetric['dtStart'] = dt
            listDictMetric.append(dMetric)
        '''
    dfMetric = pd.DataFrame(listDictMetric)[listOut + ['dtStart', 'Fee']]
    Utils.funcWriteExcel(dfMetric, excel_writer, sheet_name='MetricDollarPCT')

# freeze panes
for sheet in excel_writer.sheets:
    excel_writer.sheets[sheet].freeze_panes(1, 1)
excel_writer.close()

# WIND monitor
print 'producing WIND monitor'
dfDetail = Utils.dfDetail
dtToday = dfDailyReturn.index.max()
dtOneWeekLater = dtToday + datetime.timedelta(7 * 2, 0)
daterange = pd.date_range(dtToday, dtOneWeekLater, freq='1B')[1:]
excel_writer = pd.ExcelWriter('WindMonitor{0}_TOTALMONEY{1}.xlsx'.format(dtToday.strftime('%Y%m%d'), Utils.TOTALMONEY))
ExcelStrColumnStart = 'D'
ExcelNRowStart = 2
for dictPerTopPort in listDictPerTopPort:
    strStrategy = dictPerTopPort['strStrategy']
    df = dictPerTopPort['dfResult']
    # DF to excel
    df['Multiplier'] = dfDetail.ix[df.index, 'multiplier']
    for ix, row in df.iterrows():
        if ix.endswith('czc'):
            ContractCode = row['ContractCodeTomorrow']
            ContractCode = ContractCode[:-4] + ContractCode[-3:]
            df.ix[ix, 'ContractCodeTomorrow'] = ContractCode
    df = df.reset_index()

    # whether to change position tomorrow 
    if dictAdjustPosition[strStrategy]:
        lOut = ['ContractCodeTomorrow', 'ContractTomorrow', 'Multiplier']
    else:
        lOut = ['ContractCodeTomorrow', 'Contract', 'Multiplier']
    df = df[lOut]
    df.columns = ['ContractCode', 'Position', 'Multiplier']
    df = df.set_index('ContractCode')
    for dt in daterange:
        strColumn = dt.strftime('%Y%m%d')
        df[strColumn] = np.nan
    Utils.funcWriteExcel(df, excel_writer, sheet_name=strStrategy)

    # write formula
    worksheet = excel_writer.sheets[strStrategy]
    for nDate in range(0, len(daterange)):
        dt = daterange[nDate]
        strColumn = dt.strftime('%Y%m%d')
        ExcelStrColumnThis = chr(ord(ExcelStrColumnStart) + nDate)
        ExcelStrColumnPrevThis = chr(ord(ExcelStrColumnStart) - 1 + nDate)
        for nSecu in range(0, df.index.size):
            ExcelPosition = ExcelStrColumnThis + str(nSecu + ExcelNRowStart)
            ExcelnRow = nSecu + ExcelNRowStart
            if nDate == 0:
                ExcelFormula = '=s_dq_close(A%d, %s1) - s_dq_close(A%d, %s)'%(ExcelnRow, ExcelStrColumnThis, ExcelnRow, dtToday.strftime('%Y%m%d'))
            else:
                ExcelFormula = '=s_dq_close(A%d, %s1) - s_dq_close(A%d, %s1)'%(ExcelnRow, ExcelStrColumnThis, ExcelnRow, ExcelStrColumnPrevThis)
            worksheet.write_formula(ExcelPosition, ExcelFormula)
    for nDate in range(0, len(daterange)):
        dt = daterange[nDate]
        strColumn = dt.strftime('%Y%m%d')
        ExcelStrColumnThis = chr(ord(ExcelStrColumnStart) + nDate)
        NRow = df.index.size + ExcelNRowStart
        ExcelPosition = ExcelStrColumnThis + str(NRow)
        ExcelFormula = '=SUMPRODUCT(B2:B{0}, C2:C{0}, {1}2:{1}{0})'.format(NRow-1, ExcelStrColumnThis)
        worksheet.write_formula(ExcelPosition, ExcelFormula)

# freeze panes
for sheetname in excel_writer.sheets:
    sheet = excel_writer.sheets[sheetname]
    sheet.freeze_panes(1, 1)
excel_writer.close()



