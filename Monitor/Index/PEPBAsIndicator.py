import os, sys, shutil, gc
import pandas as pd
import numpy as np
import datetime

#import ThaiExpress.Common.Utils as Utils
#reload(Utils)

import StockDataBase as SDB
reload(SDB)
import StockDataBase.DataReader as SDBReader
reload(SDBReader)

dtStart = datetime.datetime(2004, 1, 1)
dtEnd = datetime.datetime(2017, 12, 10)
listDT = pd.date_range(dtStart, dtEnd, freq='1M')
listDict = []
#for dt in listDT[:1]:
for dt in listDT:
    print dt
    ########################
    # retrieve data
    ########################
    dfFactor = SDBReader.getFactorOfOneDate('TotalCap, PE,PB', dt, SDB.Utils.strTB_FactorDaily)
    dfFactor['PE'] = dfFactor['PE'].apply(lambda x: 10000 if x < 0 else x)
    dfFactor['PB'] = dfFactor['PB'].apply(lambda x: 10000 if x < 0 else x)
    ThresholdTotalCap300 = dfFactor['TotalCap'].nlargest(300).values[-1]
    ThresholdTotalCap800 = dfFactor['TotalCap'].nlargest(800).values[-1]
    ThresholdTotalCap25PCT = dfFactor['TotalCap'].quantile(0.5)
    dfFactor300 = dfFactor[dfFactor['TotalCap'] >= ThresholdTotalCap300]
    dfFactor800 = dfFactor[dfFactor['TotalCap'] >= ThresholdTotalCap800]
    dfFactor25PCT = dfFactor[dfFactor['TotalCap'] >= ThresholdTotalCap25PCT]
    dfFactor500 = dfFactor.ix[dfFactor800.index.difference(dfFactor300.index)]
    
    ########################
    # calculate PE PB quantiles
    ########################
    dictOne = {'TradingDay': dt}
    #for df in [dfFactor, dfFactor300, dfFactor500, dfFactor800]:
    for strIndex in ['ALL', '300', '500', '800', '25PCT']:
        if strIndex == 'ALL':
            df = dfFactor
        elif strIndex == '300':
            df = dfFactor300
        elif strIndex == '500':
            df = dfFactor500
        elif strIndex == '800':
            df = dfFactor800
        elif strIndex == '25PCT':
            df = dfFactor25PCT

        dictOneTemp = {
            'PE_Median_%s'%strIndex: df['PE'].median(),
            'PB_Median_%s'%strIndex: df['PB'].median(),
            'NStock_%s'%strIndex: df.index.size,
            'NPELT10_%s'%strIndex: df[df['PE'] < 10].index.size,
            'NPBLT1_%s'%strIndex: df[df['PB'] < 1].index.size,
            }
        dictOne.update(dictOneTemp)
    listDict.append(dictOne)
    del dfFactor, dfFactor300, dfFactor500, dfFactor800
    gc.collect()
dfFactorStats = pd.DataFrame(listDict).set_index('TradingDay')
df = dfFactorStats

########################
# make indicator
########################


