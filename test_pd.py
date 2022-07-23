from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData
import pandas as pd
import sys
from datetime import date, datetime

from backup.modul_db import getPairsPrice


def connect_db_engine():
    engine = create_engine(
        'mysql+mysqlconnector://jf:mariaDB#5031@194.13.80.214:3306/BinanceData')

    return engine

listNull = []
"""
df_csv = pd.read_csv("pairsNull.txt")

for index, row in df_csv.iterrows():
    print(index)
    print(row)

f = open("pairsNull.txt", "r")
f.writelines
for x in f:
    if len(x) > 0:
        print(len(x))
        print(x)
        listNull.append(x[:len(x)-1])
"""   

# df_csv = pd.read_csv("anyusdt.csv")

# Verbindung mit Datenbank aufbauen
engine = connect_db_engine()

pair = "ETHUSDT"

df_csv = getPairsPrice(pair, engine, "")

# df_csv.TimeCET = pd.to_datetime(df_csv.TimeCET)

# df_csv.set_index("TimeCET", inplace=True)

print(df_csv.head())

df_vol = df_csv["volume"]

my_date = date.today()
my_time = datetime.min.time()
my_datetime = datetime.combine(my_date, my_time)
print(my_datetime)

df_now = df_vol[df_vol.index > my_datetime]

print(df_now.head())

pairVol = int(df_now.sum())
print(pairVol)

sql = "INSERT INTO binance_volume(`pairs`, `SUM_volume`) VALUES ('" + pair + "'," + str(pairVol) + ") " + \
        "ON DUPLICATE KEY UPDATE `SUM_volume`=" + str(pairVol)
rs = engine.execute(sql)

sys.exit()

rs = engine.execute("""
                    INSERT INTO binance_volume(`pairs`, `SUM_volume`)
                    SELECT * FROM binance_price_temp t
                    ON DUPLICATE KEY UPDATE `open`=t.`open`, `high`=t.`high`, `low`=t.low, `close`=t.close, `volume`=t.volume
                            """)



lstc = df_csv.columns
print(lstc[0][0])

lstv = df_csv.values
print(lstv[0][0])


# Verbindung mit Datenbank aufbauen
# engine = connect_db_engine()
    
# pairs ohne price in DB-Tabelle speichern
# df_Null = pd.DataFrame(listNull, columns=["pair_noPrice"])



# df_Null.to_sql("binance_pairsNoPrice", engine, if_exists='replace')

"""[summary]
# pairs ohne price in CSV speichern
f = open("pairsNull.txt", "w")
for pairNull in listNull:
    f.writelines(pairNull)
f.close()
"""

