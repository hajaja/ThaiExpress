import pandas as pd
import datetime
import numpy as np
import pdb
import gc

import Utils
reload(Utils)

class Strategy:
    def __init__(self, df, dictParam):
        self.dictEvaluation = {}
        self.dictParam = dictParam
        self.df = df
        self.rfr = 0.03
        self.rfrDaily = np.power(self.rfr + 1, 1./250) - 1
   
    def evaluateLongShortSimplified(self):
        # parameter 
        decimalCommission = self.dictParam['commission']
        if self.dictParam.has_key('stoplossSlippage'):
            decimalStoplossSlippage = abs(self.dictParam['stoplossSlippage'])
        else:
            decimalStoplossSlippage = 0.0

        if self.dictParam.has_key('boolStoploss'):
            boolStoploss = self.dictParam['boolStoploss']
            decimalStoploss = abs(self.dictParam['stoploss'])
        else:
            boolStoploss = False
        
        if self.dictParam.has_key('boolStopProfit'):
            boolStopProfit = self.dictParam['boolStopProfit']
            decimalStopProfit = abs(self.dictParam['stopProfit'])
        else:
            boolStopProfit = False
        
        # set indexLongEnter & indexShortEnter
        self.df['indicatorOfDecision'] = self.df['indicator']
        self.df['indicator'] = self.df['indicator'].shift(1)

        seriesIndicator = self.df['indicator'].copy()
        seriesIndicator = Utils.UtilsUtils.keepOperation(seriesIndicator.ffill())
        self.indexLongEnter = seriesIndicator[seriesIndicator==1].index
        self.indexShortEnter = seriesIndicator[seriesIndicator==-1].index

        seriesIndicatorNonShift = self.df['indicatorOfDecision'].ffill()
        self.indexLongClose = seriesIndicatorNonShift[(seriesIndicatorNonShift!=1)&(seriesIndicatorNonShift.shift(1)==1)].index
        self.indexShortClose = seriesIndicatorNonShift[(seriesIndicatorNonShift!=-1)&(seriesIndicatorNonShift.shift(1)==-1)].index


        if (self.indexLongEnter.size != 0) and self.indexLongEnter[-1] == self.df.index[-1]:
            self.indexLongEnter = self.indexLongEnter[:-1]
        if (self.indexShortEnter.size != 0) and self.indexShortEnter[-1] == self.df.index[-1]:
            self.indexShortEnter = self.indexShortEnter[:-1]
        
        # calculate daily return
        self.df['returnPCT'] = self.df['Close'].pct_change().fillna(0)
        self.df['returnPCTOpenToClose'] = self.df['Close'] / self.df['Open'] - 1
        self.df['returnPCTPrevCloseToOpen'] = self.df['Open'] / self.df['Close'].shift(1) - 1
        self.df['returnPCTPrevCloseToHigh'] = self.df['High'] / self.df['Close'].shift(1) - 1
        self.df['returnPCTPrevCloseToLow'] = self.df['Low'] / self.df['Close'].shift(1) - 1
        self.df['returnPCTOpenToHigh'] = self.df['High'] / self.df['Open'] - 1
        self.df['returnPCTOpenToLow'] = self.df['Low'] / self.df['Open'] - 1

        # stop loss
        if boolStoploss:
            self.stopLoss(decimalStoploss, decimalStoplossSlippage)
            self.df.ix[(self.df['Stoploss'].shift(1)==1)&(self.df['indicator'].isnull()), 'indicator'] = 0

        # stop profit
        if boolStopProfit:
            self.stopProfit(decimalStopProfit)
            self.df.ix[(self.df['StopProfit'].shift(1)==1)&(self.df['indicator'].isnull()), 'indicator'] = 0

        # ffill the indicator
        self.df['indicator'] = self.df['indicator'].ffill().fillna(0)

        # calculate metric
        self.df['returnPCTHold'] = self.df['returnPCT'] * self.df['indicator']

        # calculate metric - PrevCloseToOpen at operating day, valid if adjust position at open
        self.df.ix[self.indexLongEnter, 'returnPCTHold'] = self.df.ix[self.indexLongEnter, 'returnPCTOpenToClose'] * self.df['indicator'] - decimalCommission
        self.df.ix[self.indexShortEnter, 'returnPCTHold'] = self.df.ix[self.indexShortEnter, 'returnPCTOpenToClose'] * self.df['indicator'] - decimalCommission
        
        self.df.ix[self.indexLongClose, 'returnPCTHold'] = self.df.ix[self.indexLongClose, 'returnPCTHold'] + self.df['returnPCTPrevCloseToOpen'].shift(-1).ix[self.indexLongClose] * 1 - decimalCommission
        self.df.ix[self.indexShortClose, 'returnPCTHold'] = self.df.ix[self.indexShortClose, 'returnPCTHold'] + self.df['returnPCTPrevCloseToOpen'].shift(-1).ix[self.indexShortClose] * (-1) - decimalCommission

        # calculate metric - if stop loss
        if boolStoploss:
            self.df.ix[(self.df['Stoploss']==1), 'returnPCTHold'] = self.df.ix[(self.df['Stoploss']==1), 'returnStoploss']
        if boolStopProfit:
            self.df.ix[(self.df['StopProfit']==1), 'returnPCTHold'] = self.df.ix[(self.df['StopProfit']==1), 'returnStopProfit']

        # calculate metric - calculate metric
        seriesCumulatedValue = (self.df['returnPCTHold'] + 1).cumprod()
        seriesMaxUntilNow = pd.expanding_max(seriesCumulatedValue)
        seriesMaxUntilNow = seriesMaxUntilNow.apply(lambda x: max(x, 1))
        seriesDD = (seriesMaxUntilNow - seriesCumulatedValue) / seriesMaxUntilNow
        seriesReturnPCTHoldDaily = self.df['returnPCTHold'].copy()
        def funcCalculateDailyReturn(s):
            return (s+1).cumprod()[-1] - 1
        seriesReturnPCTHoldDaily = seriesReturnPCTHoldDaily.groupby(seriesReturnPCTHoldDaily.index.date).apply(funcCalculateDailyReturn)
        self.seriesReturnPCTHoldDaily = seriesReturnPCTHoldDaily
        dictResult = {}
        dictResult['ReturnFinal'] = (seriesReturnPCTHoldDaily + 1).cumprod().dropna()[-1]
        dictResult['maxDD'] = seriesDD.max()
        dictResult['DTMaxDD'] = seriesDD.argmax()
        dictResult['returnAnnualized'] = np.power(dictResult['ReturnFinal'], 250. / len(seriesReturnPCTHoldDaily)) - 1.
        dictResult['sigmaAnnualized'] = seriesReturnPCTHoldDaily.std() * np.sqrt(250.)
        dictResult['SharpeRatio'] = (seriesReturnPCTHoldDaily.mean()*250. - self.rfr) / (seriesReturnPCTHoldDaily.std() * np.sqrt(250.))
        dictResult['SharpeRatio'] = (seriesReturnPCTHoldDaily.mean()*250. - self.rfr) / (seriesReturnPCTHoldDaily.std() * np.sqrt(250.))
        self.dictEvaluation.update(dictResult)

        del seriesCumulatedValue, seriesMaxUntilNow, seriesDD
        gc.collect()

        return dictResult

    def stopProfit(self, decimalStopProfit):
        position = 0
        returnSinceEnter = 1.
        self.df['StopProfit'] = np.nan
        self.df['returnStopProfit'] = np.nan
        for ix, row in self.df.iterrows():
            # determine position
            if row['indicator'] != position and pd.notnull(row['indicator']):
                position = row['indicator']
                returnSinceEnter = 1.
            
            # calculate the worst return in this bin
            if position == 1:
                returnSinceEnterMax = returnSinceEnter * (1 + row['returnPCTPrevCloseToHigh'])
            elif position == -1:
                returnSinceEnterMax = returnSinceEnter * (1 - row['returnPCTPrevCloseToLow'])
            else:
                returnSinceEnterMax = 1.

            # whether stoploss in this bin
            assert decimalStopProfit >= returnSinceEnter - 1
            if returnSinceEnterMax > 1 + decimalStopProfit:
                self.df.ix[ix, 'StopProfit'] = 1
                self.df.ix[ix, 'returnStopProfit'] = decimalStopProfit - (returnSinceEnter-1)
                position = 0
                returnSinceEnter = 1.
            returnSinceEnter = returnSinceEnter * (1 + row['returnPCT'] * position)

    def stopLoss(self, decimalStoploss, decimalStoplossSlippage):
        position = 0
        returnSinceEnter = 1.
        self.df['Stoploss'] = np.nan
        self.df['returnStoploss'] = np.nan
        for ix, row in self.df.iterrows():
            # determine position
            if row['indicator'] != position and pd.notnull(row['indicator']):
                position = row['indicator']
                returnSinceEnter = 1.
            
            # calculate the worst return in this bin
            if position == 1:
                returnSinceEnterMin = returnSinceEnter * (1 + row['returnPCTPrevCloseToLow'])
            elif position == -1:
                returnSinceEnterMin = returnSinceEnter * (1 - row['returnPCTPrevCloseToHigh'])
            else:
                returnSinceEnterMin = 1.

            # whether stoploss in this bin
            assert decimalStoploss >= 1 - returnSinceEnter
            if returnSinceEnterMin < 1 - decimalStoploss:
                self.df.ix[ix, 'Stoploss'] = 1
                self.df.ix[ix, 'returnStoploss'] = -(decimalStoploss - (1 - returnSinceEnter)) - decimalStoplossSlippage
                position = 0
                returnSinceEnter = 1.
            returnSinceEnter = returnSinceEnter * (1 + row['returnPCT'] * position)

    def stopLoss(self, decimalStoploss, decimalStoplossSlippage):
        # stop profit & loss
        position = 0
        returnSinceEnter = 1.
        maxSinceEnter = 1.
        self.df['Stoploss'] = np.nan
        self.df['returnStoploss'] = np.nan
        self.df['StoplossPrice'] = np.nan
        self.df['StoplossPriceTomorrow'] = np.nan
        NTrade = self.df['indicator'].dropna().size
        NDay = len(set(self.df.index.date))
        boolEnterDay = False
        for ix, row in self.df.iterrows():
            if ix == datetime.datetime(2017,5,31):
                #raise Exception
                pass
            # determine position
            if row['indicator'] != position and pd.notnull(row['indicator']):
                position = row['indicator']
                returnSinceEnter = 1.
                maxSinceEnter = 1.
                boolEnterDay = True
                PrevClose = row['OpenRaw']
            else:
                boolEnterDay = False

            # Whether the enter day
            if ix in self.indexLongEnter or ix in self.indexShortEnter:
                returnToHigh = row['returnPCTOpenToHigh']
                returnToLow = row['returnPCTOpenToLow']
                returnToClose = row['returnPCTOpenToClose']
            elif ix != self.df.index[-1]:
                returnToHigh = row['returnPCTPrevCloseToHigh']
                returnToLow = row['returnPCTPrevCloseToLow']
                returnToClose = row['returnPCT']
            else:
                returnToHigh = 0
                returnToLow = 0

            # calculate the worst return in this bin
            if position == 1:
                returnSinceEnterMin = returnSinceEnter * (1 + returnToLow)
            elif position == -1:
                returnSinceEnterMin = returnSinceEnter * (1 - returnToHigh)
            else:
                returnSinceEnterMin = 1.

            # whether stoploss in this bin
            maxDDInThisBin = (maxSinceEnter - returnSinceEnterMin) / maxSinceEnter
            maxDDAtOpen = (maxSinceEnter - returnSinceEnter) / maxSinceEnter

            # set stoploss price of tomorrow
            if position != 0:
                if boolEnterDay:
                    self.df.ix[ix, 'StoplossPrice'] = PrevClose * (1 - decimalStoploss * position)
                else:
                    self.df.ix[ix, 'StoplossPrice'] = (1 + ((1-decimalStoploss)/(1-maxDDAtOpen)-1)*position) * PrevClose

            #'''
            if maxDDAtOpen > decimalStoploss:
                self.df.ix[ix, 'returnStoploss'] = row['returnPCTPrevCloseToOpen'] * position - decimalStoplossSlippage
                position = 0
                returnSinceEnter = 1.
                maxSinceEnter = 1.
                self.df.ix[ix, 'Stoploss'] = 1
                #raise Exception
            #'''
            elif maxDDInThisBin > decimalStoploss:
                #self.df.ix[ix, 'returnStoploss'] = -(decimalStoploss - maxDDAtOpen) - decimalStoplossSlippage
                self.df.ix[ix, 'returnStoploss'] = -(1 - (1-decimalStoploss) / (1-maxDDAtOpen)) - decimalStoplossSlippage
                position = 0
                returnSinceEnter = 1.
                maxSinceEnter = 1.
                self.df.ix[ix, 'Stoploss'] = 1

            # update maxSinceEnter
            if position == 1:
                returnSinceEnterMax = returnSinceEnter * (1 + returnToHigh)
            elif position == -1:
                returnSinceEnterMax = returnSinceEnter * (1 - returnToLow)
            else:
                returnSinceEnterMax = 1.
            maxSinceEnter = max(returnSinceEnterMax, maxSinceEnter)
            returnSinceEnter = returnSinceEnter * (1 + returnToClose * position)

            # for StoplossPrice
            PrevClose = row['OpenRaw'] * row['Close'] / row['Open']

            # for StoplossPrice used for stoploss order
            if position != 0:
                maxDDAtOpen = (maxSinceEnter - returnSinceEnter) / maxSinceEnter
                self.df.ix[ix, 'StoplossPriceTomorrow'] = (1 + ((1-decimalStoploss)/(1-maxDDAtOpen)-1)*position) * PrevClose

            print ix, maxSinceEnter, maxDDInThisBin, maxDDAtOpen, returnSinceEnter, returnSinceEnterMax, returnSinceEnterMin, PrevClose


