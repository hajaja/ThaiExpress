import pandas as pd
listSecu = []
# by market 
listSecuSHF = ['cu.shf', 'al.shf', 'zn.shf', 'pb.shf', 'ni.shf', 'sn.shf', 'au.shf', 'ag.shf', 'rb.shf', 'wr.shf', 'hc.shf', 'fu.shf', 'bu.shf', 'ru.shf']
listSecuDCE = ['bb.dce', 'fb.dce', 'l.dce', 'v.dce', 'pp.dce', 'j.dce', 'jm.dce', 'i.dce', 'm.dce', 'y.dce', 'a.dce', 'b.dce', 'p.dce', 'c.dce', 'cs.dce', 'jd.dce']
listSecuCZC = ['wh.czc', 'pm.czc', 'cf.czc', 'sr.czc', 'ta.czc', 'ri.czc', 'lr.czc', 'oi.czc', 'ma.czc', 'fg.czc', 'rs.czc', 'rm.czc', 'zc.czc', 'jr.czc', 'sf.czc', 'sm.czc']
listSecuCFE = ['if.cfe', 'ic.cfe', 'ih.cfe', 't.cfe', 'tf.cfe']

# by industry
listSecuHEISE = ['i.dce', 'j.dce', 'jm.dce', 'rb.shf', 'hc.shf', 'sm.czc', 'sf.czc', 'ru.shf', 'fg.czc', 'zc.czc', 'wr.shf']
listSecuYOUSE = ['cu.shf', 'al.shf', 'zn.shf', 'pb.shf', 'ni.shf', 'sn.shf']
listSecuNONG = ['m.dce', 'y.dce', 'a.dce', 'b.dce', 'p.dce', 'c.dce', 'cs.dce', 'jd.dce', 'wh.czc', 'pm.czc', 'cf.czc', 'sr.czc', 'ri.czc', 'lr.czc', 'oi.czc', 'rs.czc', 'rm.czc', 'jr.czc', 'bb.dce', 'fb.dce']
listSecuGUI = ['au.shf', 'ag.shf']
listSecuHUA = ['fu.shf', 'bu.shf', 'l.dce', 'v.dce', 'pp.dce', 'ta.czc', 'ma.czc']
listSecuGU = ['if.cfe', 'ic.cfe', 'ih.cfe']
listSecuZHAI = ['tf.cfe', 't.cfe']

listSecuInactive = ['wr.shf', 'bu.shf', 'fu.shf', 'bb.dce', 'fb.dce', 'b.dce', 'wh.czc', 'pm.czc', 'ri.czc', 'lr.czc', 'rs.czc', 'jr.czc', 'sf.czc', 'sm.czc']
listSecuAll = listSecuSHF + listSecuDCE + listSecuCZC
listCloseAtDayEnd = [None]
dictStrategyParamRange = {}

############
# Inter Product Arbitrage
############
dfSecuPairIPA = pd.DataFrame([
    {'SecuA': 'hc.shf', 'SecuB': 'rb.shf',},
    {'SecuA': 'j.dce', 'SecuB': 'jm.dce',},
    {'SecuA': 'zn.shf', 'SecuB': 'pb.shf',},
    ])

############
# Machine Learning
############
dictStrategyParamRange['ML_Test0'] = {
        'Secu': listSecuAll,
        'strModelName': ['DecisionTreeClassifier', 'SVM', 'LogisticRegression', 'GaussianHMM'],
        'NDayTrain': [60, 120, 240],
        'NDayTest': [20, 40],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['ML_Test1'] = {
        'Secu': listSecuAll,
        'strModelName': ['DecisionTreeClassifier'],
        'NDayTrain': [240],
        'NDayTest': [20],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }


############
# Gu Zhi Qi Huo
############

dictStrategyParamRange['MARUBOZUGu'] = {
        'Secu': ['if.cfe'],
        'strModelName': ['MARUBOZU'],
        'NDayHist': [240],
        'LLThreshold': [0.7], 
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['DualThrustIF'] = {
        'Secu': ['if.cfe'],
        'strModelName': ['DualThrust'],
        'NDay': [10],
        'K': [0.1],
        'NPeriodRollingFast': [60],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1min'],
        'VBB': [True],
        'VBBMinPerBar': [5],
        }

dictStrategyParamRange['DualThrustIC'] = {
        'Secu': ['ic.cfe'],
        'strModelName': ['DualThrust'],
        'NDay': [6],
        'K': [0.25],
        'NPeriodRollingFast': [10],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1min'],
        'VBB': [True],
        'VBBMinPerBar': [5],
        }

dictStrategyParamRange['DualThrustIH'] = {
        'Secu': ['ih.cfe'],
        'strModelName': ['DualThrust'],
        'NDay': [6],
        'K': [0.28],
        'NPeriodRollingFast': [30],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1min'],
        'VBB': [True],
        'VBBMinPerBar': [5],
        }

dictStrategyParamRange['DualThrustGuReview'] = {
        'Secu': ['ih.cfe'],
        'strModelName': ['DualThrust'],
        'NDay': [5,6],
        'K': [0.1, 0.15, 0.2,  0.25],
        'NPeriodRollingFast': [3, 10, 30, 60, 120],
        'strCloseAtDayEnd': [None],
        'freq': ['1min'],
        'VBB': [True],
        'VBBMinPerBar': [5],
        }

dictStrategyParamRange['DBOIIGuReview'] = {
        'Secu': ['ic.cfe'],
        'strModelName': ['DBOII'],
        'NPeriodBBANDS': [5, 10, 20],
        'WidthBBANDS': [1.5, 2.0],
        'NPeriodRollingSlow': [10, 20, 40, 60],
        'NPeriodRollingFast': [3, 5, 10],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1min'],
        'VBB': [True],
        'VBBMinPerBar': [5],
        }

dictStrategyParamRange['TSCGu'] = {
        'Secu': ['if.cfe', 'ih.cfe', 'ic.cfe'],
        'strModelName': ['TSC'],
        'NDayHist': [80],
        'QuantileTS': [0.1],
        'ratioExtreme': [0.9], 
        'NRebound': [4.0],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['SpiderGu'] = {
        'Secu': ['if.cfe', 'ih.cfe', 'ic.cfe'],
        'strModelName': ['Spider'],
        'NTopBroker': [20],
        'NDayLookBack': [23],
        'ContractRange': ['DominantContract'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['SpiderAll'] = {
        'Secu': listSecuAll,
        'strModelName': ['Spider'],
        'NTopBroker': [20],
        'NDayLookBack': [23],
        'ContractRange': ['DominantContract'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

############
# Tech - IntraDay
############
dictStrategyParamRange['DualThrust'] = {
        'Secu': listSecuAll,
        'strModelName': ['DualThrust'],
        'NDay': [4],
        'K': [0.2],
        'NPeriodRollingFast': [60],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1min'],
        }

dictStrategyParamRange['DualThrust_Review'] = {
        'Secu': listSecuAll,
        'strModelName': ['DualThrust'],
        'NDay': [4],
        'K': [0.18, 0.2, 0.22],
        'NPeriodRollingFast': [60, 120],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1min'],
        }

dictStrategyParamRange['RBreak_Review'] = {
        'Secu': listSecuAll,
        'strModelName': ['RBreak'],
        'KSetup': [1.2, 1.8],
        'KBreak': [0.1, 0.2],
        'KEnter': [0.1, 0.12],
        'NPeriodRollingFast': [30, 60],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1min'],
        }
dictStrategyParamRange['DBOII_Review'] = {
        'Secu': listSecuAll,
        'strModelName': ['DBOII'],
        'NPeriodBBANDS': [10, 20],
        'WidthBBANDS': [1.5, 2.0],
        'NPeriodRollingSlow': [60, 180],
        'NPeriodRollingFast': [30],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1min'],
        }
dictStrategyParamRange['Dochian_Review'] = {
        'Secu': listSecuAll,
        'strModelName': ['Dochian'],
        'NPeriod': [60, 120, 240],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1min'],
        }
dictStrategyParamRange['Keltner_Review'] = {
        'Secu': listSecuAll,
        'strModelName': ['Keltner'],
        'NPeriod': [60, 120, 240, 480],
        'width': [3],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1min'],
        }

dictStrategyParamRange['FD_Review'] = {
        'Secu': listSecuAll,
        'strModelName': ['FD'],
        'FLower': [0.0],
        'FUpper': [0.01],
        'NPeriodRollingFast': [90],
        'Order': [240],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1min'],
        }

############
# Tech - InterDay
############
dictStrategyParamRange['MA'] = {
        #'Secu': listSecuAll,
        'Secu': ['if.cfe'],
        'strModelName': ['MA'],
        'NDayFast': [1,2, 3, 5, 10, 15, 20, 30],
        'NDaySlow': [5,10,15,20, 40, 60, 80, 120, 240],
        #'NDayFast': [5],
        #'NDaySlow': [30],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['MARUBOZU'] = {
        'Secu': listSecuAll,
        'strModelName': ['MARUBOZU'],
        'NDayHist': [240],
        'LLThreshold': [0.7], 
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['BTBreak'] = {
        #'Secu': ['if.cfe'],
        'Secu': listSecuAll,
        'p': [0.01],
        'strModelName': ['BTBreak'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['HLMonitor'] = {
        'Secu': listSecuAll,
        'strModelName': ['HL'],
        'NDayHist': [20],
        'NDayHistSL': [10],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['HL1'] = {
        'Secu': listSecuAll,
        'strModelName': ['HL'],
        'NDayHist': [20],
        'NDayHistSL': [10],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['HL2'] = {
        'Secu': listSecuAll,
        'strModelName': ['HL'],
        'NDayHist': [80, 90],
        'NDayHistSL': [10],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['HL_Review'] = {
        'Secu': listSecuAll,
        #'Secu': ['if.cfe'],
        'strModelName': ['HL'],
        'NDayHist': [15,20,25,30],
        'NDayHistSL': [10],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['TSCReboundASL_Review'] = {
        'Secu': listSecuAll,
        'strModelName': ['TSCReboundASL'],
        'NDayHist': [60, 80, 90, 100, 120, 240],
        'QuantileTS': [0.05, 0.10, 0.15],
        'ratioExtreme': [0.75, 0.8, 0.85, 0.90, 0.95], 
        'NRebound': [3.0, 3.5, 4.0, 4.5],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['TSCReboundASL_IF'] = {
        'Secu': listSecuAll,
        'strModelName': ['TSCReboundASL'],
        'NDayHist': [240],
        'QuantileTS': [0.10],
        'ratioExtreme': [0.85], 
        'NRebound': [3.5],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['TSCReboundASL_Review2'] = {
        'Secu': listSecuAll,
        'strModelName': ['TSC'],
        'NDayHist': [30, 60, 90],
        'QuantileTS': [0.10],
        'ratioExtreme': [0.8, 0.9], 
        'NRebound': [4.0, 5.0],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['TSC'] = {
        'Secu': listSecuAll,
        'strModelName': ['TSC'],
        'NDayHist': [60],
        'QuantileTS': [0.10],
        'ratioExtreme': [0.8], 
        'NRebound': [4.0],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['TSCReboundASLMonitor'] = {
        'Secu': listSecuAll,
        'strModelName': ['TSCReboundASL'],
        'NDayHist': [60],
        'QuantileTS': [0.10],
        'ratioExtreme': [0.8], 
        'NRebound': [4.0],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['TSCReboundASLMonitorR'] = {
        'Secu': listSecuAll,
        'strModelName': ['TSCReboundASL'],
        'NDayHist': [60],
        'QuantileTS': [0.10],
        'ratioExtreme': [0.8], 
        'NRebound': [4.0],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['TSCReboundA1'] = {
        'Secu': listSecuAll,
        'strModelName': ['TSCReboundA'],
        'NDayHist': [80],
        'NDayHistSL': [80, 100, 120],
        'QuantileTS': [0.10],
        'ratioExtreme': [0.8], 
        'NRebound': [4.0],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }
dictStrategyParamRange['TSCReboundA'] = {
        'Secu': listSecuAll,
        'strModelName': ['TSCReboundA'],
        'NDayHist': [70, 80, 90],
        'NDayHistSL': [20, 40, 60],
        'QuantileTS': [0.08, 0.10, 0.12],
        'ratioExtreme': [0.7, 0.8], 
        'NRebound': [3.0, 3.5, 4.0],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['TSCRebound'] = {
        'Secu': listSecuAll,
        'strModelName': ['TSCRebound'],
        'NDayHist': [20],
        'NDayHistSL': [20],
        'RetTS': [0.05],
        'ratioExtreme': [0.8], 
        'ratioRebound': [0.05],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['TSCRebound_Review'] = {
        'Secu': listSecuAll,
        'strModelName': ['TSCRebound'],
        'NDayHist': [15, 20, 30, 40],
        'NDayHistSL': [10, 15, 20, 25],
        'RetTS': [0.05, 0.1, 0.15, 0.2],
        'ratioExtreme': [0.6, 0.7, 0.8, 0.9], 
        'ratioRebound': [0.03, 0.04, 0.05, 0.06],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['TSCRebound_Review2'] = {
        'Secu': listSecuAll,
        'strModelName': ['TSCRebound'],
        'NDayHist': [15, 20, 80],
        'NDayHistSL': [10, 15, 20, 25],
        'RetTS': [0.05, 0.1],
        'ratioExtreme': [0.7, 0.8, 0.9], 
        'ratioRebound': [0.04, 0.05, 0.06],
        'strMethodTrend': ['TS2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['AOGE1'] = {
        'Secu': listSecuAll,
        'strModelName': ['AOGE'],
        'p': [0.03],
        'ratioAOGE': [1.04],
        'ratioAOGEInit': [1.01],
        'strPastVolume': ['VolumeAvgPast2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['AOGE2'] = {
        'Secu': listSecuAll,
        'strModelName': ['AOGE'],
        'Np': [3.0,3.5,4.0,4.5],
        'ratioAOGE': [0.8, 0.9, 0.95, 1.0, 1.05],
        'strPastVolume': ['VolumeAvgPast1', 'VolumeAvgPast2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['AOGE_Review1'] = {
        'Secu': listSecuAll,
        'strModelName': ['AOGE'],
        'p': [0.02, 0.03],
        'ratioAOGE': [0.97, 1.0, 1.02, 1.04, 1.09],
        'ratioAOGEInit': [1.0, 1.01, 1.2],
        'strPastVolume': ['VolumeAvgPast2', 'VolumeAvgPast1'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['AOGE_Review2'] = {
        'Secu': listSecuAll,
        'strModelName': ['AOGE'],
        'p': [0.03],
        'ratioAOGE': [1.02, 1.04, 1.06],
        'ratioAOGEInit': [1.04, 1.06, 1.08],
        'strPastVolume': ['VolumeAvgPast2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

dictStrategyParamRange['AOGE_Review3'] = {
        'Secu': listSecuAll,
        'strModelName': ['AOGE'],
        'p': [0.03],
        'ratioAOGE': [1.04],
        'ratioAOGEInit': [1.04],
        'strPastVolume': ['VolumeAvgPast2'],
        'strCloseAtDayEnd': listCloseAtDayEnd,
        'freq': ['1day'],
        }

############
# TS
############
dictNDayTest_NMonthStart = {
    20: [0],
    40: [0, 1],
    60: [0, 1, 2],
    }

dictStrategyParamRange['TS'] = {
       'Secu': listSecuAll,
       'strModelName': ['TS'],
       'strMethodTrend': ['TS3'],
       'NDayTrain': [40],
       'NDayTest': [40],
       'NDayShift': [4],
       'NthFriday': [1],
       'NMonthStart': [0],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TS_TSTSM4'] = {
       'Secu': listSecuAll,
       'strModelName': ['TS'],
       'strMethodTrend': ['TS2'],
       'NDayTrain': [40],
       'NDayTest': [40],
       'NDayShift': [4],
       'NthFriday': [1],
       'NMonthStart': [0, 1],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TSHS_TSTSM4'] = {
       'Secu': listSecuAll,
       'strModelName': ['TS'],
       'strMethodTrend': ['TS2'],
       'NDayTrain': [40],
       'NDayTest': [40],
       'NDayShift': [4],
       'NthFriday': [1],
       'NMonthStart': [0, 1],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TS_TSTSM4_PastVol'] = {
       'Secu': listSecuAll,
       'strModelName': ['TS'],
       'strMethodTrend': ['TS2'],
       'NDayTrain': [40],
       'NDayTest': [40],
       'NDayShift': [4],
       'NthFriday': [1],
       'NMonthStart': [0, 1],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TS_TSTSM4_PastVol2'] = {
       'Secu': listSecuAll,
       'strModelName': ['TS'],
       'strMethodTrend': ['TS2'],
       'NDayTrain': [40],
       'NDayTest': [40],
       'NDayShift': [4],
       'NthFriday': [1],
       'NMonthStart': [0, 1],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TS_CarryMomentum'] = {
       'Secu': listSecuAll,
       'strModelName': ['TS'],
       'strMethodTrend': ['TS2'],
       'NDayTrain': [40],
       'NDayTest': [40],
       'NDayShift': [4],
       'NthFriday': [1],
       'NMonthStart': [0, 1],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TS_Review'] = {
       'Secu': listSecuAll,
       'strModelName': ['TS'],
       'strMethodTrend': ['TS2'],
       'NDayTrain': [40],
       'NDayTest': [20, 40, 60],
       'NDayShift': [0,1,2,3,4],
       'NthFriday': [1,2,3,4],
       'NMonthStart': [0, 1, 2],
       'decimalStoploss': [0.99],
        }

############
# Time Series Carry
############
dictStrategyParamRange['TSC_1'] = {
       'Secu': listSecuAll,
       'strModelName': ['TSC'],
       'strMethodTrend': ['TS2'],
       'RetTS': [0.15],
       'NDayTrain': [80],
       'NDayTest': [20],
       'NDayShift': [4],
       'NthFriday': [1],
       'NMonthStart': [0],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TSC_CarryMomentum'] = {
       'Secu': listSecuAll,
       'strModelName': ['TSC'],
       'strMethodTrend': ['TS2'],
       'RetTS': [0.05, 0.10, 0.15],
       'NDayTrain': [80],
       'NDayTest': [20],
       'NDayShift': [4],
       'NthFriday': [3],
       'NMonthStart': [0],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TSC_Review'] = {
       'Secu': listSecuAll,
       'strModelName': ['TSC'],
       'strMethodTrend': ['TS2', 'TS3'],
       #'RetTS': [0.05, 0.10, 0.15],
       #'NDayTrain': [20, 40, 60, 80],
       #'NDayTest': [20, 40],
       'RetTS': [0.10, 0.15],
       'NDayTrain': [80],
       'NDayTest': [20],
       'NDayShift': [4],
       'NthFriday': [1, 3],
       #'NMonthStart': [0, 1, 2, 3, 4, 5, 6],
       'NMonthStart': [0, 1, 2, 3, 4, 5, 6],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TSC_Review2'] = {
       'Secu': listSecuAll,
       'strModelName': ['TSC'],
       'strMethodTrend': ['TS2', 'TS3'],
       'RetTS': [0.05, 0.10, 0.15],
       'NDayTrain': [20, 40, 60, 80],
       'NDayTest': [20, 40],
       'NDayShift': [4],
       'NthFriday': [1, 3],
       'NMonthStart': [0, 1, 2, 3, 4, 5, 6],
       'decimalStoploss': [0.99],
        }
############
# TSM
############
dictNDayTest_NWeekStart = {
    5: [0],
    10: [0, 1],
    15: [0, 1, 2],
    20: [0, 1, 2, 3],
    }

dictStrategyParamRange['TSM'] = {
       'Secu': listSecuAll,
       'strModelName': ['TSM'],
       'strMethodTrend': ['Simple'],
       'NDayTrain': [10],
       'NDayTest': [10],
       'NDayShift': [4],
       'NWeekStart': [0],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TSM_TSTSM4'] = {
       'Secu': listSecuAll,
       'strModelName': ['TSM'],
       'strMethodTrend': ['Simple'],
       'NDayTrain': [10],
       'NDayTest': [10],
       'NDayShift': [4],
       'NWeekStart': [0, 1],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TSM_CarryMomentum'] = {
       'Secu': listSecuAll,
       'strModelName': ['TSM'],
       'strMethodTrend': ['Simple'],
       'NDayTrain': [10],
       'NDayTest': [10],
       'NDayShift': [4],
       'NWeekStart': [0, 1],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['TSM_Review'] = {
       'Secu': listSecuAll,
       'strModelName': ['TSM'],
       'strMethodTrend': ['Simple'],
       'NDayTrain': [5, 10, 15],
       'NDayTest': [5, 10, 15],
       'NDayShift': [0, 1, 2, 3, 4],
       'NWeekStart': [0, 1, 2, 3, 4],
       'decimalStoploss': [0.99],
        }

############
# XSM
############
dictStrategyParamRange['XSM'] = {
       'Secu': listSecuAll,
       'strModelName': ['XSM'],
       'strMethodTrend': ['Simple'],
       'NDayTrain': [10],
       'NDayTest': [10],
       'NDayShift': [4],
       'NWeekStart': [0],
       'decimalStoploss': [0.99],
        }

dictStrategyParamRange['XSM_Review'] = {
       'Secu': listSecuAll,
       'strModelName': ['XSM'],
       'strMethodTrend': ['Simple'],
       'NDayTrain': [5, 10, 20],
       'NDayTest': [5, 10, 20],
       'NDayShift': [4],
       'NWeekStart': [0, 1],
       'NWeekStart': [0, 1, 2, 3, 4],
       'decimalStoploss': [0.99],
        }

