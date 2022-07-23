from backup.modul_db import connect_db_engine
from backup.modul_db import getPairsPrice
from modul_rsima import GetRSIMAData

import pandas as pd
import talib.abstract as ta
import numpy as np


from datetime import datetime as dt, timedelta

    
pair = "ALGOUSDT"

# Datenbankverbindung aufbauen
engine = connect_db_engine()


# Daten aus DB.binance_price  holen
df_pairsprice = getPairsPrice(pair, engine, "ASC")

# Indikatoren in df_pairsprice setzen

# Indikator in Spalte lowma speichern
df_pairsprice['lowma'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=200, price='close'))  
# print("Anzahl Datensaetze lowma nach lowma setzen = " + str(idflen))

# Indikator RSI aus close ermitteln
df_pairsprice['rsi'] = np.nan_to_num(ta.RSI(df_pairsprice, timeperiod=14, price='close'))

# ma aus Indikator RSI ermitteln
df_pairsprice['rsima'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=14, price='rsi'))

# df_first = df_pairsprice.tail(1).index.strftime('%Y-%m-%d %H:%M')
# print(df_first[0])

GetRSIMAData(df_pairsprice, pair, engine)

# UPDATE binance_rsisignal SET binance_rsisignal.actTimeCET="2022-05-02 14:00:00", binance_rsisignal.actClose=1.067 WHERE `pairs`="MATICUSDT"

"""
UPDATE binance_rsisignal 
SET binance_rsisignal.diffClose=(binance_rsisignal.actClose - binance_rsisignal.close) * 100 / binance_rsisignal.Close 
WHERE `pairs`="MATICUSDT"
"""