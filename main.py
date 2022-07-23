from datetime import datetime
import getopt
import sys
from connector.binance_api import GetHistoricalData
import modify_gd200
from binance.client import Client
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData 
import pandas as pd
from pandas.io import sql
import sys
import os
import time

from backup.modul_db import connect_db_engine, getPairsPrice, ActPriceInTable 
from modul_TradingView import GetTAfromTV
    

    
def main(argv):

   inputfile = ''
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv, "hi:o:", ["ifile=", "ofile="])
   except getopt.GetoptError:
      print('test.py -i <inputfile> -o <outputfile>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print('test.py -i <inputfile> -o <outputfile>')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-o", "--ofile"):
         outputfile = arg
         
   print('Input file is "', inputfile)
   print('Output file is "', outputfile)


# ----------------------------------------------------------------------------------------------------------------------    
if __name__ == '__main__':  # Execute the following code only when executing main.py (not when importing it)
    # Argumente bei Programmstart auswerten
    
    #main(sys.argv[1:])
 
      
    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv))
    
    #"""
    args = sys.argv
    if len(sys.argv) > 1:
        
        print(args[1])
        if int(args[1]) in range(1,100):
            paraHours = args[1]
            print(paraHours)
            
    else:
        # Verbindung mit Datenbank aufbauen
        engine = connect_db_engine()

        df_max = pd.read_sql("SELECT MAX(TimeCET) FROM binance_price", engine)
        print(df_max)

        # Datenbankverbindung loesen
        engine.dispose()

        sys.exit()
        
        # print("Standard = 30")
    #"""

    # paraHours = 2
    
    while True:

        # Verbindung mit Datenbank aufbauen
        engine = connect_db_engine() 

        # Startzeit der Verarbeitung in Variable speichern
        dBeginDown = datetime.now()
        
        api_key = "YRh7OHf8IUjumPzc27pVsE4VJKdR8kT7a9oRxDtREpMsivQ6wZ6XwXH3eVcFDHpc"
        api_secret = "9CCnqUKby6t6PDbIYj7vBBXg7WNXsUIXnrxpA3vIttKfXGuVmiLpTsqsFZu1fdkH"

        # Verbindung mit Binance-API herstellen
        client = Client(api_key, api_secret)
        
        # Setup der Variablen
        howLong = 1
        dayAgo = str(paraHours) + " hours ago UTC"
        print(dayAgo)
        
        #symbol = "SOLUSDT"
        interval = Client.KLINE_INTERVAL_5MINUTE
        intervalDB = 5
        
        # DB Tabellen löschen
        #empty_DB_Table(engine)
        
        # Status in binance_pairs auf Prioritaet = 0 setzen
        #rs = engine.execute("UPDATE binance_pairs SET Prioritaet=0")
        
        # Abruf der cryptos aus DB
        df_pairs = pd.read_sql('SELECT pairs FROM binance_pairs WHERE Prioritaet=1', engine)
        # df_pairs.to_csv("./csv/" + "pairs" + "_full.csv", decimal=", ")
        df_len = len(df_pairs)
        
        forcnt = 1
        
        listNull = []     
            
        # Dataframe durchlaufen um die Daten abzurufen
        for index, row in df_pairs.iterrows():
            pair = row["pairs"]

            # df = GetSelectedData(client, row["pairs"], interval, howLong)
            df = GetHistoricalData(client, row["pairs"], interval, dayAgo, "")
            
            # wenn kein price geholt, dann pair in Liste speichern und for Schleife 
            # auf den naechsten Wert springen
            if type(df) is bool:
                # rs = engine.execute("UPDATE binance_pairs SET Prioritaet=2 WHERE pairs='" + 
                #                         row["pairs"] + "'")

                print(row["pairs"] + " keine Datensaetze !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                # pair ohne Daten in Liste speichern
                listNull.append(row["pairs"])
                continue 

            # Zusatz-Info in df einfügen: pair,interval
            df.insert(0, "pairs", row["pairs"])
            df.insert(1, "interval", intervalDB)
            
            # Daten temporaer in binance_price_temp speichern
            df.to_sql("binance_price_temp", engine, if_exists='replace')
            
            # Daten von binance_price_temp nach binance_price verschieben
            # rs = engine.execute('INSERT IGNORE INTO binance_price SELECT * FROM binance_price_temp')
            rs = engine.execute("""
                    INSERT INTO binance_price(`TimeCET`, `pairs`, `interval`, `open`, `high`, `low`, `close`, `volume`)
                    SELECT * FROM binance_price_temp t
                    ON DUPLICATE KEY UPDATE `open`=t.`open`, `high`=t.`high`, `low`=t.low, `close`=t.close, `volume`=t.volume
                            """)
            

            # price in Tabelle DB.binance_gd200 aktualisieren
            print("price in Tabelle DB.binance_gd200 aktualisieren")
            df_pairsprice = getPairsPrice(pair, engine, "")


            # Technische Analyse von TradingView holen und in DB.binance_pairs speichern
            pairTA = GetTAfromTV(pair)
            sqlstr = "UPDATE `binance_pairs` SET `TechnAnalyse`='" + pairTA + "' WHERE pairs = '" + pair + "'"
            sql.execute(sqlstr, engine)            

            # Sind Datensaetze vorhanden
            idflen = len(df_pairsprice)
            if(idflen > 0) or not df_pairsprice.empty:
                ActPriceInTable(engine, df_pairsprice, pair)


            # Auswertung fuer diesen pair starten, Status in binance_pairs.Prioritaet=1
            # rs = engine.execute("UPDATE binance_pairs SET Prioritaet=1 WHERE pairs='" + 
            #                             row["pairs"] + "'")
            
            # pair in DB.binance_price_down speichern fuer Auswertung m_evaluation_price
            rs = engine.execute("INSERT INTO binance_price_down(pairs, TechnAnalyse) Values('" + pair + "', '" + pairTA + "')")

            print(str(forcnt) + " von " + str(df_len) + " " + row["pairs"] + " = " + str(len(df)) + " Datensaetze")
            forcnt += 1

        # Abschlussarbeiten nach Download ------------------------------------------------
        

        # Update des aktuellen pair_price mit Auswertungstabellen
        # binance_gd200, binance_rsima, binance_gd50vross200

        


        # pairs ohne price aus download in DB-Tabelle und CSV speichern
        print("pairs ohne price aus download in DB-Tabelle und CSV speichern")
        df_Null = pd.DataFrame(listNull, columns=["pair_noPrice"])
        if len(df_Null) > 0:
            df_Null.to_sql("binance_pairsNoPrice", engine, if_exists='replace')
            
            # pairs ohne price in CSV speichern
            f = open("pairsNull.txt", "w")
            for pairNull in listNull:
                f.writelines(pairNull)
            f.close()
        else:
            rs = engine.execute("DROP TABLE IF EXISTS binance_pairsNoPrice")
            
            if os.path.exists("pairsNull.txt"):
                os.remove("pairsNull.txt")
        
        # Datenbankverbindung loesen
        engine.dispose()

        # Loop Zeit nach indivueller Eingabe auf 1Std zurücksetzen
        paraHours = 1
        print("naechster Durchlauf mit 1 Std")

        print("Beginn Download Price " + str(dBeginDown))
        print("Ende Download Price " + str(datetime.now()))
        
        print("Sleep 1min")
        time.sleep(60)
