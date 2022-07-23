# pairs ohne Daten aus Download in DB.pairs löschen

from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData
import pandas as pd
import sys

def connect_db_engine():
    engine = create_engine(
        'mysql+mysqlconnector://mydb:mariaJF#5031@192.168.178.150:3307/myFiNews')

    return engine


# Verbindung mit Datenbank aufbauen
engine = connect_db_engine()


# Abruf der cryptos in DB.binance_pairsNoPrice
df_noprice = pd.read_sql('SELECT pair_noPrice FROM binance_pairsNoPrice', engine)

# wenn kein Ergebnis, dann Programmende
if len(df_noprice) == 0:
    print("Ende von m_clear_null_price, kein Datensaetze vorhanden")


# Durchlauf der Datensaetze fuer die Pruefung, ob Datensaetze in DB.binance_price vorhanden sind
for index, row in df_noprice.iterrows():
    # pruefen, ob Datensaetze in DB.binance_price
    df_noprice = pd.read_sql("SELECT COUNT(*) AS pricecnt FROM binance_price WHERE pairs='" + row[0] + "'", engine)
    
    # wenn nein, dann Datensatz in binance_pairs löschen
    if df_noprice.loc[0, "pricecnt"] == 0:
        print("keine Daten fuer " + row[0])
        rs = engine.execute("DELETE FROM binance_pairs WHERE pairs='" + row[0] + "'")
    
print("Ende von m_clear_null_price, Datensaetze enfernt")    
    
sys.exit()