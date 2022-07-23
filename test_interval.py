import mysql.connector
import pymysql
import mariadb
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData
from backup.modul_db import connect_db_engine, getPairs, getPairsPrice
from modul_interval import pairs_interval_change_quarter
from pandas.io import sql
import sys
from datetime import timedelta, datetime


import pandas as pd



# Bestenliste der einzelnen Intervalle ermitteln
# Uebergabeparameter:
# tableName Tabelle, in die die Intervaldaten bereits gespeichert sind
# stepCnt Anzahl der Intervalle
# Rueckgabe:
# Dataframe mit Spalten fuer Ranglisten
def pairs_interval_rangliste(engine, tableName, stepCnt):

    print("Modul pairs_interval_rangliste gestartet ###########################################")

    # DB.binance_profit_quarter in DataFrame laden
    sqlstr = "SELECT * FROM " + tableName
    df_q = pd.read_sql_query(sqlstr, engine) 

    # print(df_q.head())
    
    for x in range(2,6):

        # nach der 1.proz.Veraenderung sortieren ---------------------------------
        # Spalte fuer Rangliste-Nr anlegen
        df_q["rangProz" + str(x)] = 0

        # Sortieren nach der Spalte Prozent
        df_q.sort_values(inplace=True, ascending=False,  by="proz" + str(x))

        # Schleife fuer Ranglistenstand setzen
        rowcnt = 1
        for index, row in df_q.iterrows():
            df_q.at[index,"rangProz" + str(x)] = rowcnt
            rowcnt += 1


    # nach der Gesamt.proz.Veraenderung sortieren ---------------------------------
    df_q["rangProzGes"] = 0
    df_q.sort_values(inplace=True, ascending=False,  by='prozges')

    rowcnt = 1
    for index, row in df_q.iterrows():

        # Name des pair aus der Zeile holen
        df_q.at[index,"rangProzGes"] = rowcnt
        rowcnt += 1

    # Sortierreihefolge der Spalten zuruecksetzen auf index
    df_q.sort_index(inplace=True)

    # specify the columns to sum
    cols = ['rangProz2', 'rangProz3', 'rangProz4', 'rangProz5']

    # sum of columns specified
    df_q["rangList"] = df_q[cols].sum(axis=1)

    df_q.sort_values(inplace=True,  by='rangList')

    print("Modul pairs_interval_rangliste beendet ###########################################")

    return df_q


# Execute the following code only when executing main.py (not when importing it) ----------------------------------------------------------------------------------
if __name__ == '__main__':


    #Ergebnis in DB-Tabelle einfügen
    # Writing Dataframe to Mysql and replacing table if it already exists
    engine = connect_db_engine()



    """
    # DB.binance_profit_quarter löschen
    sqlstr = "DROP TABLE IF EXISTS binance_profit_quarter"
    sql.execute(sqlstr, engine)

    # DB.binance_pairs Daten in Dataframe zurueckgeben
    df_pairs = getPairs(engine)

    # Dataframe durchlaufen um die Auswertung für den Crypto abzurufen
    for index, row in df_pairs.iterrows():

        # Name des pair aus der Zeile holen
        pair = row["pairs"]
        print("pair = " + pair)
        #pair = "CFXUSDT"

        #Daten aus DB.binance_price  holen
        df_pairsprice = getPairsPrice(pair, engine, "")
        # Es sind Datensaetze vorhanden
        if(len(df_pairsprice) > 0):
            df_interval = pairs_interval_change_quarter(df_pairsprice, pair, 4, 15)

            df_interval.to_sql("binance_profit_quarter", engine, if_exists='append')
    sys.exit()

    """


    #Rangliste der pairs erstellen
    df = pairs_interval_rangliste(engine, "binance_profit_quarter", 4)
    df.to_sql(name='binance_profit_test', con=engine, if_exists='replace', index=False)
