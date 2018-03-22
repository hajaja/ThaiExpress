import os, sys, shutil
import pandas as pd
import numpy as np
import datetime

import ThaiExpress.Common.Utils as Utils
reload(Utils)

listStrFileAddress = [
        '/mnt/Tera/Code_Result/TSM_TSTSM4/NDayShift4_NDayTest10_NDayTrain10_NWeekStart0_Secucu.shf_decimalStoploss0.99_strMethodTrendSimple_strModelNameTSM/Check.xlsx',
        '/mnt/Tera/Code_Result/TSM_TSTSM4/NDayShift4_NDayTest10_NDayTrain10_NWeekStart1_Secucu.shf_decimalStoploss0.99_strMethodTrendSimple_strModelNameTSM/Check.xlsx',
        '/mnt/Tera/Code_Result/TS_TSTSM4/NDayShift4_NDayTest40_NDayTrain40_NMonthStart0_NthFriday1_Secucu.shf_decimalStoploss0.99_strMethodTrendTS2_strModelNameTS/Check.xlsx',
        '/mnt/Tera/Code_Result/TS_TSTSM4/NDayShift4_NDayTest40_NDayTrain40_NMonthStart1_NthFriday1_Secucu.shf_decimalStoploss0.99_strMethodTrendTS2_strModelNameTS/Check.xlsx',
        ]

dfSum = None
for strFileAddress in listStrFileAddress:
    df = pd.read_excel(strFileAddress, sheetname='PositionDirection').set_index('dtEnd').fillna(0)
    if dfSum is None:
        dfSum = df
    else:
        dfSum = dfSum + df

