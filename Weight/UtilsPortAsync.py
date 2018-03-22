import pandas as pd
import datetime
import gc
import numpy as np
import os, re

import ThaiExpress.Common.Utils as Utils
reload(Utils)

################################
# parameter
################################

################################
# functions
################################
def funcResampleDaily(s):
    s.index = s.index + datetime.timedelta(0, 4*3600)
    s = s.resample('1D').last().dropna()
    s = s[s.index.weekday<=4]
    return s
    
