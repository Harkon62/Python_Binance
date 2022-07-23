import mysql.connector
import pymysql
import mariadb
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData
from pandas.io import sql
import sys
from datetime import timedelta, datetime, date


from backup.modul_db import connect_db_engine
from modul_interval import pairs_interval_sumliste





# Execute the following code only when executing main.py (not when importing it) ----------------------------------------------------------------------------------
if __name__ == '__main__':


    """
    today = date.today()
    print(today)

    print(today.day)
    
    print(today.month)

    datetimeshort = datetime(today.year, today.month, today.day, 0, 0, 0)
    print(datetimeshort)
    """


    #Ergebnis in DB-Tabelle einfügen
    # Writing Dataframe to Mysql and replacing table if it already exists
    engine = connect_db_engine()

    pairs_interval_sumliste(engine)
