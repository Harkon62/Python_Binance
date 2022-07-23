import mysql.connector
import pymysql
import mariadb
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData
#from sqlalchemy.ext.declarative import declarative_base
import sys

import pandas as pd

from backup.modul_db import connect_db_engine,  getMaxDateInDB


def connect_db():
  mydb = mysql.connector.connect(

      host="192.168.178.150",
      port=3307,
      user="mydb",
      passwd="mariaJF#5031",

      database="myFiNews",
      use_unicode=True,
      charset="utf8"

  )
  mydb.set_charset_collation('utf8')

  return mydb


def connect_db_maria():
    try:
        mydb = mariadb.connector.connect(

            host="192.168.178.150",
            port=3307,
            user="mydb",
            passwd="mariaJF#5031",
            database="myFiNews",
            #use_unicode=True,
            #charset="utf8"

        )
        #mydb.set_charset_collation('utf8')
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    return mydb


engine = connect_db_engine()

dfmax = getMaxDateInDB(engine)

dfmaxdate = dfmax[dfmax['pairs'] == 'UTKUSDT']

if dfmaxdate.empty:
    print("df leer")
    sys.exit()

print(dfmaxdate)
print(dfmaxdate.iat[0,1])

sys.exit()

# Daten speichern in csv
symbol = "RADUSDT"
df = pd.read_csv(symbol + ".csv", decimal=",")

#Ergebnis in DB-Tabelle einfügen
# Writing Dataframe to Mysql and replacing table if it already exists


df.to_sql("binance_data", engine)

#DF_test.to_sql(name='newsindizes', con=engine, if_exists='append', index=False)
