from backup.modul_db import connect_db_engine
from backup.modul_db import getPairsPrice, checkTableExists

import pandas as pd
from pandas.io import sql

import talib.abstract as ta
import numpy as np


from datetime import datetime, timedelta



# Auswertung, RSI und MA(RSI), cross von RSI nach oben durch den MA(RSI)
# 
def GetRSIMAData(df, pair, engine):
    print("Modul GetRSIMAData gestartet mit Symbol=" + pair + " ###########################################")
    
    if checkTableExists(engine, 'binance_rsisignal'):
        ActRSIMAData(df, pair, engine)

    listProfit = []
    
    # Pruefen, ob dataframe die minimal-Groesse von 400 erreicht, sonst return leere Liste
    dflen = len(df)
    if dflen < 200:
        return listProfit


    # Signal erzeugen aus RSIma < RSI und RSIma > Vortag RSI
    df.loc[(df['rsi'].shift(1) < df['rsima'] ) & (df['rsi'] > df['rsima']),	'buy' ] = 1
    # print(df.tail(30))

    # Dataframe mit nur den Signalen erzeugen
    df_rsima = df[df['buy'] == 1]

    # Sind Signale vorhanden?
    if df_rsima.empty:
        return

    """
    # Datum, des ersten Signal in Variable speichern
    seriesdatetime = df_rsima.iloc[len(df_rsima) - 1].name

    # Zeitraum um 30 min zuruecksetzen
    firstDate = seriesdatetime - timedelta(minutes=30)
    # print(firstDate)

    # Signale der letzten 30 min filtern
    df_rsima = df_rsima[df_rsima.index >= firstDate]
    """
    # df_rsima.to_csv("_rsima.csv", decimal=",")
    # print(pair)
    # print(df_rsima.tail(1))
    

    # in DB.binance_rsima anfuegen
    df_rsima = df_rsima.tail(1)
    df_rsima.to_sql("binance_rsima", engine, if_exists='append')


    # rsi < 45 + close < gd200 in DB.rsisignal anfuegen -----------------------------------------------------------------
    df_signal = df_rsima[(df_rsima['rsi'] < 45)   &   (df_rsima['close'] < df_rsima['lowma'] )]


    # Daten aus aktuellen df_price holen, um bestehende Datensaetze in DB.rsisignal zu aktualisieren --------------------
    df_first = df.tail(1)
    # print(df_first)

    # df_first['actTimeCET'] = df_first.index
    # print(df_first)

    # aus dem aktuellen Datensatz den TimeCET in Variable speichern
    # actTimeCET = df_first.iat[0, 11]
    tmpSer = df_first.index.strftime('%Y-%m-%d %H:%M')
    actTimeCET = tmpSer[0]
    print(actTimeCET)

    # aus dem aktuellen Datensatz den Close in Variable speichern
    actClose = df_first.iat[0, 5]
    print(actClose)


    # Werte in DB.binance_rsisignal aktualisieren
    if checkTableExists(engine, 'binance_rsisignal'):
        sqlstr = "UPDATE binance_rsisignal " + \
        "SET binance_rsisignal.actTimeCET='" + actTimeCET + "', binance_rsisignal.actClose=" + str(actClose) +  \
        " WHERE `pairs`='" + pair + "'"
        sql.execute(sqlstr, engine)

        sqlstr = "UPDATE binance_rsisignal " + \
        "SET binance_rsisignal.diffClose=(binance_rsisignal.actClose - binance_rsisignal.close) * 100 / binance_rsisignal.Close " + \
        "WHERE `pairs`='" + pair + "'"
        sql.execute(sqlstr, engine)

    if not df_signal.empty:
        # df_first = df.tail(1)
        
        # print(df_signal)

        df_signal['actTimeCET'] = df_first.index
        df_signal['actClose'] = df_first.iat[0, 5]

        # Aktuell Close
        firstclose = df_first.iat[0, 5]
        # bei Signal Close
        signalclose = df_signal.iat[0, 5]

        df_signal['diffClose'] = (firstclose - signalclose) * 100 / signalclose

        # print(df_signal)

        df_signal.to_sql("binance_rsisignal", engine, if_exists='append')

    # df.to_excel('testData.xlsx')


    # print(df_rsima.tail(3))

    print("                   beendet mit Symbol=" + pair + " ###########################################")



def ActRSIMAData(df, pair, engine):
    print("Modul ActRSIMAData gestartet mit Symbol=" + pair + " ###########################################")

    # Daten aus aktuellen df_price holen, um bestehende Datensaetze in DB.rsisignal zu aktualisieren --------------------
    df_first = df.tail(1)
    # print(df_first)


    # aus dem aktuellen Datensatz den TimeCET in Variable speichern
    tmpSer = df_first.index.strftime('%Y-%m-%d %H:%M')
    actTimeCET = tmpSer[0]
    print(actTimeCET)


    # aus dem aktuellen Datensatz den Close in Variable speichern
    actClose = df_first.iat[0, 5]
    print(actClose)


    # Werte in DB.binance_rsisignal aktualisieren
    sqlstr = "UPDATE binance_rsisignal " + \
        "SET binance_rsisignal.actTimeCET='" + actTimeCET + "', binance_rsisignal.actClose=" + str(actClose) +  \
        " WHERE `pairs`='" + pair + "'"
    sql.execute(sqlstr, engine)


    sqlstr = "UPDATE binance_rsisignal " + \
        "SET binance_rsisignal.diffClose = (binance_rsisignal.actClose - binance_rsisignal.close) * 100 / binance_rsisignal.Close " + \
        "WHERE `pairs`='" + pair + "'"
    sql.execute(sqlstr, engine)

    print("                   beendet mit Symbol=" + pair + " ###########################################")