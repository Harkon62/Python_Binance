# Auswertung Price Veränderung in Stufen z.B.15 30 45 60 min
from modul_interval import pairs_interval_change
from backup.modul_db import connect_db_engine, getPairs, volume_getdata, getPairsPrice
import pandas as pd
from datetime import datetime
import sys
import time
from pandas.io import sql



def pairs_auswertung(pair, engine):
    print("Modul m_eval_inter_tgl gestartet mit Symbol=" + pair + " ###########################################")
    
    #Daten aus DB.binance_price  holen
    df_pairsprice = getPairsPrice(pair, engine, "")

    if(len(df_pairsprice) > 0):
         # 1. Daten auswerten Interval quarter -----------------------------------------------------------------------       
        df = pairs_interval_change(df_pairsprice, pair, 4, 15)

        # DataFrame in DB speichern
        df.to_sql("binance_profit_quarter", engine, if_exists='append')

        # 2. Daten auswerten Interval tgl ----------------------------------------------------------------------------
        df = pairs_interval_change(df_pairsprice, pair, 2, 1440)

        df.to_sql("binance_profit_tgl", engine, if_exists='append')

    else:
        print(" Modul m_eval_inter_tgl, keine Datensaetze gefunden")

    print("Modul m_eval_inter_tgl beendet  ###########################################")



def start_Auswertung():
    # Datenbankverbindung aufbauen
    engine = connect_db_engine()
        
    # DB.binance_profit_quarter löschen
    sqlstr = "DROP TABLE IF EXISTS binance_profit_quarter"
    sql.execute(sqlstr, engine)

    # DB.binance_profit löschen
    sqlstr = "DROP TABLE IF EXISTS binance_profit_tgl"
    sql.execute(sqlstr, engine)

    # DB.binance_volume erzeugen
    # für jedes pair das Volumen des heutigen Tages ermitteln
    volume_getdata(engine)
        

    # DB.binance_pairs Daten in Dataframe zurueckgeben
    df_pairs = getPairs(engine)


    # Dataframe durchlaufen um die Auswertung für den Crypto abzurufen
    for index, row in df_pairs.iterrows():
        # Name des pair aus der Zeile holen
        pair = row["pairs"]
        #pair = "CFXUSDT"
            
        pairs_auswertung(pair, engine)    
    
    

# Execute the following code only when executing main.py (not when importing it) ----------------------------------------------------------------------------------
if __name__ == '__main__':
    
    #Startzeit in Variable festhalten
    dBeginDown = datetime.now()
    
    # Table binance_profit_tgl auswerten
    start_Auswertung()

    print("Beginn Auswertung Intervall tgl " + str(dBeginDown))   
    print("Ende Auswertung Intervall tgl" + str(datetime.now()))
