from backup.modul_db import getPairsPrice
from backup.modul_db import connect_db_engine, getMaxDateInDB

import pandas as pd
from pandas.io import sql

def set_DB_AVG_Volumen(df_pairsprice, pair, engine):
    # DataFrame mit dem aktuellsten Datum
    df_MaxDate = getMaxDateInDB(engine)

    # Convert Dataframe to dictionary
    pairDatedict = df_MaxDate.set_index('pairs').T.to_dict("list")
    
    maxdate = str(pairDatedict[pair][0])

    # AVG-Volumen der Cryptos aus DB holen ---------------------------------------
    
    sqlstr = "SELECT pairs, AVG(volume) AS VolAVG FROM binance_price " + \
        "WHERE TimeCET > CURDATE() AND TimeCET < '" + maxdate + \
            "' AND pairs='" + pair + "' GROUP BY `pairs`"
    df_pairsVol = pd.read_sql_query(sqlstr,engine)
    
    if len(df_pairsVol) > 0:
        return df_avgvol.iat[0, 1]
    else:
        return ""
    
    
    
    
pair = "1INCHUSDT"

   # Datenbankverbindung aufbauen
engine = connect_db_engine()

#Daten aus DB.binance_price  holen
df_pairsprice = getPairsPrice(pair, engine, "DESC")

df_avgvol = set_DB_AVG_Volumen(df_pairsprice, pair, engine)

print(df_avgvol.iat[0,1])