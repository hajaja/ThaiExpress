# -*- coding = utf8 -*-
import pandas as pd
import datetime, re, gc, os, sys, logging
import numpy as np

import ThaiExpress.Common.Strategy as Strategy
reload(Strategy)
import ThaiExpress.Common.Utils as Utils
reload(Utils)
import ThaiExpress.Common.ParamRange as ParamRange
reload(ParamRange)

# param
NDayRolling = 40
lListPairHEISE = [['rb.shf','hc.shf'], ['rb.shf', 'i.dce'], ['j.dce', 'jm.dce']]
lListPairYOUSE = [['zn.shf', 'pb.shf'], ['au.shf', 'cu.shf'], ['al.shf', 'cu.shf']]
dictListPair = {'HEISE': lListPairHEISE, 'YOUSE': lListPairYOUSE}

# check the possible pairs (for momentum following or mean reversion)
def funcRollCumValue(s):
    return (pd.Series(s)+1).cumprod().values[-1]

for k, lListPair in dictListPair.iteritems():
    listS = []
    for lPair in lListPair:
        sA = Utils.dfExe.ix[lPair[0]]['PCT']
        sB = Utils.dfExe.ix[lPair[1]]['PCT']
        sDiff = (sA-sB).rolling(NDayRolling).apply(funcRollCumValue).dropna()
        sDiff.name = '_'.join(lPair)
        listS.append(sDiff)
    df = pd.concat(listS, axis=1)
