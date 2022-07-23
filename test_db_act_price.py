from backup.modul_db import connect_db_engine
from backup.modul_db import getPairsPrice


import pandas as pd
from datetime import datetime


# price in Tabelle DB.binance_gd200 aktualisieren
def ActPriceInTableTest(engine, df, pair):
    
    df['TimeCET'] = df.index

    lastDate = df.iloc[len(df) - 1]['TimeCET']
    # print(lastDate)

    lastClose = df.iloc[len(df) - 1]['close']
    # print(lastClose)

    # aktuellen price in DB setzen ----------------------------------------------------------
    
    # Update DB.binance_gd50cross200
    sqlstr = "UPDATE binance_gd50cross200 " + \
                            "SET endClose = " + str(lastClose) + ", endDate = '" + str(lastDate) + "', " + \
                            "endDiff = (" + str(lastClose) + " - startClose) * 100 / startClose " + \
                             "WHERE pairs = '" + pair + "'"
    engine.execute(sqlstr)

    # print(sqlstr)


    # price in DB. binance_gd200 aktualisieren
    sqlstr02 = "UPDATE binance_gd200 " + \
                            "SET endClose = " + str(lastClose) + ", endDate = '" + str(lastDate) + "', " + \
                            "endDiff = (" + str(lastClose) + " - startClose) * 100 / startClose " + \
                            "WHERE pairs = '" + pair + "'"
    engine.execute(sqlstr02)
    

    # price in DB. binance_gd200 aktualisieren
    sqlstr03 = "UPDATE binance_rsisignal " + \
                            "SET actClose = " + str(lastClose) + ", actTimeCET = '" + str(lastDate) + "', " + \
                            "diffClose = (" + str(lastClose) + " - close) * 100 / close " + \
                            "WHERE pairs = '" + pair + "'"
    engine.execute(sqlstr03)

    # print(sqlstr03)




# ----------------------------------------------------------------------------------------------------------------------    
if __name__ == '__main__':  # Execute the following code only when executing main.py (not when importing it)
    pair = "C98USDT"

    # Datenbankverbindung aufbauen
    engine = connect_db_engine()

    # Daten aus DB.binance_price  holen
    df = getPairsPrice(pair, engine, "ASC")

    ActPriceInTableTest(engine, df, pair)

