from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData

import pandas as pd
from pandas.io import sql
import talib.abstract as ta
import numpy as np


import sys
from datetime import date, datetime

from backup.modul_db import getPairsPrice


def connect_db_engine():
    engine = create_engine(
        'mysql+mysqlconnector://jf:mariaDB#5031@194.13.80.214:3306/BinanceData')

    return engine



# Verbindung mit Datenbank aufbauen
engine = connect_db_engine()

pair = "ETHUSDT"

df_pairsprice = getPairsPrice(pair, engine, "")

# df_csv.TimeCET = pd.to_datetime(df_csv.TimeCET)

# df_csv.set_index("TimeCET", inplace=True)

# Indikatoren in df_pairsprice setzen

# Indikator MA200 in Spalte lowma speichern
df_pairsprice['lowma'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=200, price='close'))  
# print("Anzahl Datensaetze lowma nach lowma setzen = " + str(idflen))

# Indikator MA50 in Spalte lowma speichern
df_pairsprice['fast100ma'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=100, price='close'))  
# print("Anzahl Datensaetze lowma nach lowma setzen = " + str(idflen))

# Indikator MA50 in Spalte lowma speichern
df_pairsprice['fast50ma'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=50, price='close'))  
# print("Anzahl Datensaetze lowma nach lowma setzen = " + str(idflen))

# Indikator RSI aus close ermitteln
df_pairsprice['rsi'] = np.nan_to_num(ta.RSI(df_pairsprice, timeperiod=14, price='close'))

# ma aus Indikator RSI ermitteln
df_pairsprice['rsima'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=14, price='rsi'))


print(df_pairsprice.tail(5))
print(len(df_pairsprice))

# df_pairsprice['high1h']
highclose = df_pairsprice.close.rolling(3).max()
print(highclose)

df_pairsprice['max'] = df_pairsprice.close.shift(1).rolling(3).max()

# Spalte close mit lowma vergleichen, Ergebnis = True = 1 setzen
df_pairsprice.loc[
    (
        (df_pairsprice['lowma'] < df_pairsprice['close']) &
        (df_pairsprice['lowma'] > df_pairsprice['close'].shift(1))
    ),
    'buy'] = 1

# dfrow = df_pairsprice[df_pairsprice.index == '2022-07-17 12:15:00']['rsi']
# dfrowres = dfrow * 2

#print(dfrowres)

df_pairsprice.to_csv("pairprice.csv", sep=';')

sys.exit()
