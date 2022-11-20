# https://stackoverflow.com/questions/67439379/how-can-i-get-all-coins-in-usd-parity-to-the-binance-api/69423323#69423323
import sys
import requests
import pandas as pd
from pandas.io import sql
# from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData
from modul_db import connect_db_engine



# ----------------------------------------------------------------------------------------------------------------------   
# Vergleich der abgerufenen Daten von der Exchange mit denen in der DB gespeicherten Pairs
# Staus in der Spalte DB.binance_pairs.binance festhalten, 0 = nicht in der Exchange vorhanden, 
#                                                          1 = in der Exchange und in DB, 
#                                                          2 = neu hinzugefuegt
#
# in der Python Datei main_get_exchange_info.py werden die Pairs von der Exchange heruntergeladen


if __name__ == '__main__': 
    
    # Verbindung mit Datenbank aufbauen
    engine = connect_db_engine()
    
    # Spalte DB.binance_pairs.binance auf 0 setzen
    sqlstr = "UPDATE `binance_pairs` SET `binance`= 0"
    sql.execute(sqlstr, engine)
            
    # Abfrage definieren zum Vergleich der vorhandenen Pairs mit denen aus der Exchange
    sqlstrExch = "SELECT `pairs` FROM `binance_pairs_exchange`"
    df_exchange = pd.read_sql(sqlstrExch, engine)
    
    sqlstrDB = "SELECT `pairs` FROM `binance_pairs`"
    df_db = pd.read_sql(sqlstrDB, engine)
    
    
    print(df_exchange)
    df_exchange.to_csv("./csv/df_exchange.csv")
    
    print(df_db)
    df_db.to_csv("./csv/df_db.csv")
    
    # Durchlauf der abgerufenen Crypto-Pairs aus DB.binance_pairs_exchange
    # Pruefen, ob das Pair in der aktuellen DB.binance_pairs vorhanden ist
    recCnt = 0
    for index, row in df_exchange.iterrows():
        recCnt += 1 
    
        # print(row)
        
        pair = []
        pair.append(row[0])
        
        # resDF = df_db.isin(pair[0])
        # print(resDF)
    
        # Abfrage in DB.binance_pairs -----------------
        # print(df_db.eq(df_exchange))
        sQuery = 'pairs == "' + row[0] + '"'
        resQ = df_db.query(sQuery)
        
        
        # resDB = df_db.loc[df_exchange.pairs == row[0]]
        if len(resQ) == 1:
            # gefunden = Spalte binance = 1
            sqlstr = "UPDATE `binance_pairs` SET `binance`= 1 WHERE pairs = '" + row[0] + "'"
            sql.execute(sqlstr, engine)        
            print(row[0] + " gefunden")
        else:
            sqlstr = "INSERT INTO `binance_pairs` (pairs, Prioritaet, binance) VALUES ('" + row[0] + "', 1, 2)"
            sql.execute(sqlstr, engine)        
            print(row[0] + " neu in Table")        
    
    
