import pandas as pd
import datetime
import numpy as np
import pdb
import scipy.io as sio
import os
import gc
import random
from scipy.signal import butter, lfilter, freqz
from scipy import signal

import ThaiExpress.Common.Utils as Utils
reload(Utils)
import logging

###################
# wrapper
###################
def generateIndicator(dictDataSpec):
    if dictDataSpec['strModelName'] in ['ML']:
        import ThaiExpress.Config.Tech.ML as ML
        reload(ML)
        return ML.UtilsML.generateIndicatorPoint(dictDataSpec)
    elif dictDataSpec['strModelName'] in ['Aberration', 'RBreak', 'Hans123', 'Dochian', 'Keltner', 'BBANDS', 'DBOII', 'DualThrust', 'FD']:
        import ThaiExpress.Config.Tech.UtilsIntraDay as UtilsIntraDay
        reload(UtilsIntraDay)
        return UtilsIntraDay.generateIndicatorTech(dictDataSpec)
    elif dictDataSpec['strModelName'] in ['TALib', 'MARUBOZU', 'MA', 'HL', 'BT', 'BTBreak', 'TSC', 'TSCRebound', 'TSCReboundA', 'TSCReboundASL', 'AOGE']:
        import ThaiExpress.Config.Tech.UtilsInterDay as UtilsInterDay
        reload(UtilsInterDay)
        return UtilsInterDay.generateIndicatorTech(dictDataSpec)
    else:
        print 'undefined strModelName %s'%dictDataSpec['strModelName']
        raise Exception

