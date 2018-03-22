#coding=utf8
######################################################### 
'''
The weight of a product's position is determined by its standard deviation's reciprocal. More academic resources can be found in[Qian2005](http://www.dailyalts.com/wp-content/uploads/2014/06/Risk-Parity-and-Diversification.pdf)

For better cash management, the total weight is always constant, and the individual risk is used to determine the relative importance in the portfolio. 

Future works
1. the lookback period for calculating the product's risk
2. the method to calculate the product's risk. a) standard deviation; b) exponential weighted moving average of standard deviation; c) other risk measures. 
3. how to determine the total weight of the portfolio. If all the products are volatile, then holding more cash is a better option. 
'''
######################################################### 
import pandas as pd
import datetime, os, re, sys, time, logging, gc
import numpy as np

import ThaiExpress.Common.Utils as Utils
reload(Utils)
import UtilsPortSync
reload(UtilsPortSync)

# prepare XLSX file
def funcTopPort(strParamSweep):
    # param
    strMethodVolatility = 'EWMAN2'
    
    # dtBackTestStart & dtBackTestEnd
    dtBackTestStart = Utils.dtBackTestStart
    #dtBackTestEnd = Utils.dtBackTestEnd

    ######################################################### 
    # read backtest result of rquired cases
    ######################################################### 
    # read stored results
    listDictPerTopPort = Utils.getFileAddressForTopPort(strParamSweep)
    
    #---------------- clear fake tomorrow
    dictDataSpec = listDictPerTopPort[0]
    strTB = dictDataSpec['strModelName'][0]
    try:
        Utils.UtilsDB.clearTBFakeTomorrow(Utils.UtilsDB.DB_NAME_POSITION, strTB) 
        Utils.UtilsDB.clearTBFakeTomorrow(Utils.UtilsDB.DB_NAME_PERFORMANCE, strTB)
    except:
        pass

    #---------------- iterate 
    for dictPerTopPort in listDictPerTopPort:
        gc.collect()
        listDictDataSpec = []
        for strFileAddress in dictPerTopPort['listFileAddress']:
            listDictDataSpec.append(pd.read_pickle(strFileAddress))
        ######################################################### 
        # prepare directory for saving
        ######################################################### 
        strDirParamSweep = Utils.dirResultPerCase + '/' + strParamSweep 
        if os.path.exists(strDirParamSweep) is False:
            os.mkdir(strDirParamSweep)
        strDirAddressCase = strDirParamSweep + '/' + dictPerTopPort['strCase'] + '/'
        if os.path.exists(strDirAddressCase) is False:
            os.mkdir(strDirAddressCase)
        strFileAddressPrefix = strDirAddressCase

        # save config file of the port
        dictPerTopPortOut = dict((key,value[0]) for key, value in dictPerTopPort.iteritems() if key not in ['strCase', 'listFileAddress', 'Secu'])
        pd.Series(dictPerTopPortOut).to_pickle(strFileAddressPrefix + '/' + 'portconfig.pickle')
    
        NDayTrain = dictPerTopPort['NDayTrain'][0]
        NDayTest = dictPerTopPort['NDayTest'][0]
        NDayShift = dictPerTopPort['NDayShift'][0]
        strModelName = dictPerTopPort['strModelName'][0]
    
        ######################################################### 
        # determine the rebalance datetime
        ######################################################### 
        if strModelName in ['TS', 'TSC']:
            listDTTestStart = Utils.generateNthFriday(dictPerTopPort['NthFriday'][0], dictPerTopPort['NMonthStart'][0])
        elif strModelName in ['TSM', 'XSM']:
            listDTTestStart = Utils.generateDTTestStartCalendarDay(NDayTest, NDayShift, dictPerTopPort['NWeekStart'][0])
        else:
            print 'incorrect model name: %s'%strModelName
        listDTTestStart = [x for x in listDTTestStart if x <= Utils.dtLastData]
        
        dtEnter = Utils.UtilsDB.dtTomorrow
        listDTTestStart.append(dtEnter)
        seriesDTRebalance = pd.Series(listDTTestStart)
        
        ######################################################### 
        # calculate the portfolio return
        ######################################################### 
        dtBackTestStart = seriesDTRebalance[seriesDTRebalance>=Utils.dtBackTestStart].values[0]
        if strModelName in ['TSM', 'TS', 'TSC', 'XSM']:
            dfOut = UtilsPortSync.funcShowStrategyPortSum(
                dictPerTopPort,
                listDictDataSpec, 
                seriesDTRebalance, 
                strMethodVolatility, 
                NDayTest, 
                dtBackTestStart, 
                #dtBackTestEnd, 
                strFileAddressPrefix
                )
        dfOut = dfOut[dfOut.index >= dtBackTestStart]
        
        # plot & savefig
        dfOut = dfOut[['Cum Return', 'Max DD', 'Position']].ffill().dropna()
        #ax = dfOut[['Cum Return', 'Max DD', 'Position']].plot(secondary_y=['Position'])
        #ax.set_ylabel('Cum Return')
        #ax.set_ylabel('Position')
        #plt.savefig(strFileAddressPrefix + 'figure.png', format='png')
        #plt.close()
        print strFileAddressPrefix
                    
        # output
        dfOut.to_pickle(strFileAddressPrefix + 'dfOut.pickle')

if __name__ == '__main__':
    strParamSweep = sys.argv[1]
    dfOut = funcTopPort(strParamSweep)

