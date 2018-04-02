import pandas as pd
import numpy as np
import scipy.io as sio
import datetime, os, shutil, platform, sys, gc, time, re, pdb, random
from scipy.signal import butter, lfilter, freqz
from scipy import signal
from dateutil.parser import parse
import CommodityDataBase as CDB
reload(CDB)
import RoyalMountain.DataBase.MySQLDBAPI as MySQLDBAPI
reload(MySQLDBAPI)
import RoyalMountain.Dialect as Dialect
reload(Dialect)

#--------- table name
#DB_NAME_POSITION = 'Position'
#
#strTB_TEMP = 'TEMP'
#TB_NAME_PORT_PERFORMANCE = 'Daily_Port_Performance'
#dictTableName = {}
#dictTableName['TSM'] = 'TSM'
#dictTableName['TS'] = 'TS'

strTB_TEMP = 'TEMP'
DB_NAME_POSITION = Dialect.dictMySQL['DB_NAME_CommodityDataBase']
DB_NAME_PERFORMANCE = Dialect.dictMySQL['DB_NAME_CommodityDataBase']

strTBPrefixPosition = 'POSI_'
strTBPrefixPerformance = 'PERF_'

dtTomorrow = datetime.datetime(2025,1,1)     # date of tomorrow position adjustment

#------------------ save 
def clearTBFakeTomorrow(strDB, strTB):
    con = MySQLDBAPI.connectMDB(strDB)
    cur = con.cursor()
    sql = 'delete from {0} where trade_date="{1}";'.format(strTB, dtTomorrow)
    cur.execute(sql)
    con.commit()
    cur.close()
    con.close()
    return 

def saveTB_DAILY(strDB, strTB, df, listColumnIndex=['code', 'trade_date']):
    # save to temporary table
    MySQLDBAPI.dropTable(strDB, strTB_TEMP)
    MySQLDBAPI.save(strDB, strTB_TEMP, df)

    strIndex = '_'.join(listColumnIndex)
    strIndex = strIndex[:15]
    sqlIndexExists = "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE `TABLE_CATALOG` = 'def' AND `TABLE_SCHEMA` = DATABASE() AND `TABLE_NAME` = '{0}' AND `INDEX_NAME` = '{1}';".format(strTB, strIndex)
    con = MySQLDBAPI.connect(strDB)
    df = pd.read_sql(sqlIndexExists, con)
    con.dispose()

    con = MySQLDBAPI.connectMDB(strDB)
    cur = con.cursor()
    if df.empty:
        # insert 1 row
        sqlCreate = "CREATE TABLE IF NOT EXISTS {0} SELECT * FROM {1} WHERE code IS NOT NULL LIMIT 1;".format(strTB, strTB_TEMP)
        cur.execute(sqlCreate)

        # not null
        sqlAlterColumnTradingDay = "ALTER TABLE {0} MODIFY trade_date datetime NOT NULL".format(strTB)
        cur.execute(sqlAlterColumnTradingDay)
        sqlAlterColumnCode = "ALTER TABLE {0} MODIFY code varchar(30) NOT NULL".format(strTB)
        cur.execute(sqlAlterColumnCode)
        for strColumn in set(listColumnIndex).difference(set(['code', 'trade_date'])):
            if strColumn.startswith('str') or strColumn in ['freq', 'ContractRange']:
                sql = "ALTER TABLE {0} MODIFY {1} varchar(20) NOT NULL".format(strTB, strColumn)
            else:
                sql = "ALTER TABLE {0} MODIFY {1} double".format(strTB, strColumn)
            cur.execute(sql)


        # add unique index if necessary
        sqlAddIndexUnique = "CREATE UNIQUE INDEX {0} ON {1} ({2})".format(strIndex, strTB, ','.join(listColumnIndex))
        cur.execute(sqlAddIndexUnique)

        # add non-unique index
        for column in listColumnIndex:
            sqlAddIndexOne = "CREATE INDEX {0} ON {1} ({0})".format(column, strTB)
            cur.execute(sqlAddIndexOne)

    # temp -> formal table
    print 'insert from TEMP to FORMAL'
    cur.execute("INSERT IGNORE INTO {0} SELECT * FROM {1}".format(strTB, strTB_TEMP))
    con.commit()
    cur.close()
    con.close()

##------------------ load 
#def readPosition(dictOption):
#    if 'code' in dictOption.keys():
#        sql = 'select * from {d[TB_NAME]} where code="{d[code]}" and trade_date>="{d[strDTStart]}" order by trade_date;'.format(d=dictOption)
#    else:
#        sql = 'select * from {d[TB_NAME]} where trade_date>="{d[strDTStart]}" order by trade_date;'.format(d=dictOption)
#
#    con = MySQLDBAPI.connect(dictOption['DB_NAME'])
#    df = pd.read_sql(sql, con)
#    con.dispose()
#    return df

#------------- read database, for performance and position
def readPerformance(strDB, strTB, dtStart=None):
    con = MySQLDBAPI.connect(strDB)
    if dtStart is None:
        sql = 'SELECT * FROM {0}'.format(strTB)
    else:
        sql = 'SELECT * FROM {0} where trade_date > "{1}"'.format(strTB, dtStart.strftime('%Y%m%d'))
    df = pd.read_sql(sql, con)
    con.dispose()
    return df

def readPosition(strDB, strTB, dtStart):
    con = MySQLDBAPI.connect(strDB)
    sql = 'SELECT * FROM {0} where trade_date = "{1}"'.format(strTB, dtStart.strftime('%Y%m%d'))
    df = pd.read_sql(sql, con)
    con.dispose()
    return df

def readContractCode(dtStart):
    import CommodityDataBase as CDB
    reload(CDB)
    strTB = CDB.Utils.UtilsDB.DAILY_DOMINANT_DB_NAME
    con = MySQLDBAPI.connect(CDB.Utils.UtilsDB.strMySQLDB)
    sql = 'SELECT * FROM {0} where trade_date="{1}"'.format(strTB, dtStart.strftime('%Y%m%d'))
    df = pd.read_sql(sql, con)
    con.dispose()
    return df


