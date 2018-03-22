# -*- coding: gbk -*-
import pandas as pd
import numpy as np
import os, sys, re, datetime, gc
from dateutil.parser import parse

strFilePositionOld = 'Position20180222.csv'
strFilePositionNew = 'WindMonitor20180222.xlsx'

# Old
df0 = pd.read_csv(strFilePositionOld, encoding='gbk')
df0 = df0.dropna()
dictRename = {u'合约号': 'ContractCode', u'多空': 'LongShort', u'总仓': 'NContract'}
df0 = df0.rename(columns=dictRename)
df0 = df0[dictRename.values()]
df0['ContractCode'] = df0['ContractCode'].apply(lambda x: x.upper())
df0 = df0.set_index('ContractCode').sort_index()
df0['LongShort'] = df0['LongShort'].apply(lambda x: x.find(u'多')>=0)
df0['Position'] = (df0['LongShort']-0.5)*2 * df0['NContract']
sPosition0 = df0['Position'].astype(int)
sPosition0.name = 'OLD'

# New
listPosition = []
for strSheetName in ['TSM0', 'TSM1', 'TS0', 'TS1']:
    sPosition = pd.read_excel(strFilePositionNew, sheetname=strSheetName).set_index('ContractCode')['Position']
    sPosition = sPosition.dropna()
    sPosition = sPosition.astype(int)
    sPosition.name = strSheetName
    listPosition.append(sPosition)
sPosition1 = pd.concat(listPosition, axis=1).sum(1)
sPosition1.name = 'NEW'

# delta
ix = sPosition0.index.union(sPosition1.index).unique()
sPosition0 = sPosition0.ix[ix].fillna(0)
sPosition0 = sPosition0.reset_index().groupby('ContractCode').sum()
sPosition1 = sPosition1.ix[ix].fillna(0)
dfPosition = pd.concat([sPosition0, sPosition1], axis=1)
dfPosition = dfPosition.fillna(0)
dfPosition['DELTA'] = dfPosition['NEW'] - dfPosition['OLD']
dfPosition = dfPosition[dfPosition['DELTA']!=0]

# order
listOrder = []
for ix, row in dfPosition.iterrows():
    if row['OLD'] < 0 and row['NEW'] < 0:
        if row['DELTA'] < 0:
            listOrder.append('%s\tSELL\tENTER\t%d'%(ix, abs(row['DELTA'])) )
        else:
            listOrder.append('%s\tBUY\tCLOSE\t%d'%(ix, abs(row['DELTA'])) )
    elif row['OLD'] < 0 and row['NEW'] == 0:
        listOrder.append('%s\tBUY\tCLOSE\t%d'%(ix, abs(row['DELTA'])) )
    elif row['OLD'] < 0 and row['NEW'] > 0:
        listOrder.append('%s\tBUY\tCLOSE\t%d'%(ix, abs(row['OLD'])) )
        listOrder.append('%s\tBUY\tENTER\t%d'%(ix, abs(row['NEW'])) )

    elif row['OLD'] == 0 and row['NEW'] < 0:
        listOrder.append('%s\tSELL\tENTER\t%d'%(ix, abs(row['DELTA'])) )
    elif row['OLD'] == 0 and row['NEW'] > 0:
        listOrder.append('%s\tBUY\tENTER\t%d'%(ix, abs(row['DELTA'])) )

    elif row['OLD'] > 0 and row['NEW'] < 0:
        listOrder.append('%s\tSELL\tCLOSE\t%d'%(ix, abs(row['OLD'])) )
        listOrder.append('%s\tSELL\tENTER\t%d'%(ix, abs(row['NEW'])) )
    elif row['OLD'] > 0 and row['NEW'] == 0:
        listOrder.append('%s\tSELL\tCLOSE\t%d'%(ix, abs(row['DELTA'])) )
    elif row['OLD'] > 0 and row['NEW'] > 0:
        if row['DELTA'] < 0:
            listOrder.append('%s\tSELL\tCLOSE\t%d'%(ix, abs(row['DELTA'])) )
        else:
            listOrder.append('%s\tBUY\tENTER\t%d'%(ix, abs(row['DELTA'])) )

fh = open('order.txt', 'w')
for x in listOrder:
    fh.write(x + '\n')
fh.close()
dfOrder = pd.read_csv('order.txt', delimiter='\t', header=None)
dfOrder.columns = ['ContractCode', 'Direction', 'EnterClose', 'Quantity']
dfOrder = dfOrder.set_index('ContractCode').sort_index()

# output 
excel_writer = pd.ExcelWriter('ORDER.xlsx')
dfPosition.to_excel(excel_writer, sheet_name='position')
dfOrder.to_excel(excel_writer, sheet_name='order')
excel_writer.close()


