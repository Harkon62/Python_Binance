from modify_gd50crross200 import GetGD50cross200
from backup.modul_db import connect_db_engine, getPairsPrice


import pandas as pd
import talib.abstract as ta
import numpy as np
from datetime import datetime


    
pair = "ETHUSDT"

# Datenbankverbindung aufbauen
engine = connect_db_engine()

# Daten aus DB.binance_price  holen
df = getPairsPrice(pair, engine, "ASC")

# Indikator MA200 in Spalte lowma speichern
df['lowma'] = np.nan_to_num(ta.MA(df, timeperiod=200, price='close'))  
# print("Anzahl Datensaetze lowma nach lowma setzen = " + str(idflen))

# Indikator MA50 in Spalte lowma speichern
df['fastma'] = np.nan_to_num(ta.MA(df, timeperiod=50, price='close'))  
# print("Anzahl Datensaetze lowma nach lowma setzen = " + str(idflen))

listGD50c200 = GetGD50cross200(df, pair)
print(listGD50c200)

dfgd = pd.DataFrame([listGD50c200], columns=["startDate", "pairs", "startClose", "endDate", \
                                                   "endClose", "endDiff"])

# Daten in binance_gd200 speichern
dfgd.to_sql("binance_gd50cross200", engine, if_exists='append')