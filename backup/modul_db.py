# Auswertung Price Veränderung in Stufen z.B.15 30 45 60 min
from sqlalchemy import create_engine
import pandas as pd
from pandas.io import sql
from datetime import date, datetime


# Datenbankverbindung aufbauen
def connect_db_engine():
    engine = create_engine(
        # 'mysql+mysqlconnector://mydb:mariaJF#5031@192.168.178.150:3307/BinanceData')
        'mysql+mysqlconnector://jf:mariaDB#5031@194.13.80.214:3306/BinanceData')

    return engine

# für jedes pair das Volumen des heutigen Tages ermitteln
def getPairsVolume(df, pair):

    # Volumen von heute aus dataframe holen
    df_vol = df["volume"]

    my_date = date.today()
    my_time = datetime.min.time()
    my_datetime = datetime.combine(my_date, my_time)
    # print(my_datetime)

    df_today = df_vol[df_vol.index > my_datetime]

    # print(df_today.head())

    return int(df_today.sum())

def volume_getdata(engine):

    # Auswertung  taegliche Volumen erstellen ----------------------------------------------------
    sqlstr ="DROP TABLE IF EXISTS binance_volume"
    sql.execute(sqlstr, engine)
    
    sqlstr = "CREATE TABLE binance_volume SELECT pairs, SUM(volume) as SUM_volume FROM binance_price " + \
                    "WHERE TimeCET>CURDATE() GROUP BY pairs"
    sql.execute(sqlstr, engine)
    
    
def getPairs(engine):
   
    # Daten der Cryptos aus DB holen --------------------------------------------------------------
    sqlstr = "SELECT pairs, volume FROM binance_price GROUP BY `pairs` ORDER BY `volume` DESC"
    df_pairs = pd.read_sql_query(sqlstr, engine) 
       
    return df_pairs


def getPairsPrice(pair, engine, sortTime):
    print("Modul getPairsPrice gestartet ###########################################")

    # Variable fuer Anzahl Tage setzen
    timeFrame = 8

    #Daten aus DB.binance_price  holen
    sqlstr = "SELECT * FROM `binance_price` WHERE `pairs`='" + pair + "' " + \
                "AND `TimeCET` > DATE_SUB(CURDATE(),INTERVAL " + str(timeFrame) + " DAY) " + \
                "ORDER BY `TimeCET` " + sortTime
    """
    sqlstr = "SELECT * FROM `binance_price` WHERE `pairs`='{0}' " + \
                "AND `TimeCET` > DATE_SUB(CURDATE(),INTERVAL {1} DAY) " + \
                "ORDER BY `TimeCET` {2}".format(pair, str(timeFrame), sortTime)
    """

    df_pairsprice = pd.read_sql_query(sqlstr, engine)

    df_pairsprice = df_pairsprice.set_index("TimeCET")
    print("                    getPairsPrice: Anzahl pairs " + str(len(df_pairsprice)))

    print("                    beendet ###########################################")

    return df_pairsprice


def empty_DB_Table(engine):

    print("Modul empty_DB_Table gestartet  ###########################################")

    # DB.binance_volSignal löschen
    # sqlstr = "DROP TABLE IF EXISTS binance_volSignal"
    # sql.execute(sqlstr, engine)

    # DB.binance_gd200 aeltere Datensaetze loeschen
    if checkTableExists(engine, 'binance_gd200'):
        sqlstr = "DELETE FROM binance_gd200 WHERE startdate < DATE_SUB(CURDATE(),INTERVAL 4 DAY)"
        sql.execute(sqlstr, engine)

    # DB.binance_rsima aeltere Datensaetze loeschen
    if checkTableExists(engine, 'binance_rsima'):
        sqlstr = "DELETE FROM binance_rsima WHERE TimeCET < DATE_SUB(CURDATE(),INTERVAL 2 DAY)"
        sql.execute(sqlstr, engine)

    # DB.binance_rsima aeltere Datensaetze loeschen
    if checkTableExists(engine, 'binance_rsisignal'):
        sqlstr = "DELETE FROM binance_rsisignal WHERE TimeCET < CURDATE()"
        sql.execute(sqlstr, engine)

    # DB.binance_profit löschen
    # sqlstr = "DROP TABLE IF EXISTS binance_profit_std"
    # sql.execute(sqlstr, engine)

    # DB.binance_profit löschen
    # sqlstr = "DROP TABLE IF EXISTS binance_profit_quarter"
    # sql.execute(sqlstr, engine)

    # DB.binance_profit löschen
    # sqlstr = "DROP TABLE IF EXISTS binance_profit_tgl"
    # sql.execute(sqlstr, engine)

    
    print("                     beendet    ###########################################")
    
    
def getMaxDateInDB(engine):
    # Daten der Cryptos aus DB holen --------------------------------------------------------------
    sqlstr = "SELECT pairs, MAX(TimeCET) AS maxdate  FROM binance_price WHERE TimeCET>CURDATE() GROUP BY pairs"
    df_pairs = pd.read_sql_query(sqlstr, engine)
    
    return df_pairs

def checkTableExists(engine, tableName):
    # DB.binance_rsima aeltere Datensaetze loeschen
    sqlstr = """SELECT * 
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_NAME = 
            """
    sqlstr = sqlstr + '"' + tableName + '";'
    result = sql.execute(sqlstr, engine)
    if result.rowcount == 1:
        return True
    else:
        return False


# price in Tabelle DB.binance_gd200 aktualisieren
def ActPriceInTable(engine, df, pair):
    # price in binance_gd200 aktualisieren
    # aktuellen price aus binance_price ermitteln 
    # und in DB.binance_gd200 aktualisieren
    
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