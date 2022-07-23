from modify_gd200 import GetGD200Data, GetGD200Vol
from backup.modul_db import connect_db_engine
from backup.modul_db import getPairsPrice


import pandas as pd
import talib.abstract as ta
import numpy as np

from datetime import datetime
import sys


# Execute the following code only when executing main.py (not when importing it) ----------------------------------------------------------------------------------
if __name__ == '__main__':

    pair = "IOTAUSDT"

    # Datenbankverbindung aufbauen
    engine = connect_db_engine()

    # Daten aus DB.binance_price  holen
    df_pairsprice = getPairsPrice(pair, engine, "ASC")

    # Indikator MA200 in Spalte lowma speichern
    df_pairsprice['lowma'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=200, price='close'))  
    # print("Anzahl Datensaetze lowma nach lowma setzen = " + str(idflen))

    listGD = GetGD200Data(df_pairsprice, pair)
    print(listGD)

    sys.exit()

    if len(listGD) > 0:
        # CrossDatum aus Liste holen,Volumen nach gd200 und vor gd200 ermitteln ---------------------------------------------
        # AVG in DB.tabelle speichern 
        listGD = GetGD200Vol(df_pairsprice, listGD[0], listGD)
            
        dfgd = pd.DataFrame([listGD], columns=["startDate", "pairs", "startClose", "endDate", \
                                                   "endClose", "endDiff", "gdlowerCnt", "Volbefore", "Volafter"])

        # Daten in binance_gd200 speichern
        dfgd.to_sql("binance_gd200", engine, if_exists='append')

