from modul_3candleup import Get3CandleUp
from backup.modul_db import connect_db_engine
from backup.modul_db import getPairsPrice


import pandas as pd
import talib.abstract as ta
import numpy as np

from datetime import datetime
import sys


# Execute the following code only when executing main.py (not when importing it) ----------------------------------------------------------------------------------
if __name__ == '__main__':

    # pair = "IOTAUSDT"

    # Datenbankverbindung aufbauen
    engine = connect_db_engine()


    # Abruf der cryptos aus DB
    df_pairs = pd.read_sql('SELECT * FROM binance_pairs WHERE Prioritaet > 0', engine)
    # df_pairs.to_csv("./csv/" + "pairs" + "_full.csv", decimal=", ")
    df_len = len(df_pairs)


    # Dataframe durchlaufen um die Daten abzurufen ------------------------------------------------------------
    forcnt = 1
    listNull = []
    listColumns = ["symbol", "C1close", "C1open", "C2close", "C2open", "C3close", "C3open", "C1lowma", "Date3c"]     


    for index, row in df_pairs.iterrows():
        pair = row["pairs"]
            
            
        # Daten aus DB.binance_price  holen
        df_pairsprice = getPairsPrice(pair, engine, "ASC")

        if(len(df_pairsprice) > 0):
            # Indikator MA200 in Spalte lowma speichern
            df_pairsprice['lowma'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=200, price='close'))  
            # print("Anzahl Datensaetze lowma nach lowma setzen = " + str(idflen))

            list3c = Get3CandleUp(df_pairsprice, pair)
            if(len(list3c) > 0):
                # df_3c = pd.DataFrame(listGD, columns=['points', 'rebounds'])
                # df_3c = pd.DataFrame.from_dict(dict3c, orient='columns')
                df_3c = pd.DataFrame([list3c], columns=listColumns)
                df_3c.set_index('Date3c', inplace=True)
                
                print(df_3c)
                
                # Daten in binance_3candle speichern
                df_3c.to_sql("binance_3candle", engine, if_exists='append')
        
    sys.exit()





    if len(listGD) > 0:
        # CrossDatum aus Liste holen,Volumen nach gd200 und vor gd200 ermitteln ---------------------------------------------
        # AVG in DB.tabelle speichern 
        listGD = GetGD200Vol(df_pairsprice, listGD[0], listGD)
            
        dfgd = pd.DataFrame([listGD], columns=["startDate", "pairs", "startClose", "endDate", \
                                                   "endClose", "endDiff", "gdlowerCnt", "Volbefore", "Volafter"])

        # Daten in binance_gd200 speichern
        dfgd.to_sql("binance_gd200", engine, if_exists='append')

