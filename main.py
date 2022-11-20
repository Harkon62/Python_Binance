from datetime import datetime
import getopt
import sys
from binance_api import GetHistoricalData

from binance.client import Client
from secretsAPI import API_KEY, API_SECRET

import pandas as pd
from pandas.io import sql
import sys
import os

from modul_db import connect_db_engine, getPairsPrice, ActPriceInTable 
from modul_TradingView import GetTAfromTV
    

# Pairs DB.binance_pairs nach Rangliste in DB.binance_gd200 sortieren
def setPairsPrioritaet(dfpairs, dfpairs200):

    dfpairs.loc[:, 'Prioritaet'] = 9999


    # Dataframe durchlaufen um  Daten abzurufen
    for index, row in df_pairs200.iterrows():
        pair = row["pairs"]
        RecPos = index

        # Spalte close mit lowma vergleichen, Ergebnis = True = 1 setzen
        dfpairs.loc[
        (
            (dfpairs['pairs'] == pair)
        ),
        'Prioritaet'] = RecPos + 1

    dfpairs.sort_values('Prioritaet', inplace=True)


    return dfpairs


def getPairsDataFromExchange(engine):
    return None


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
    
    # """
    args = sys.argv
    if len(sys.argv) > 1:
        
        print(args[1])
        if int(args[1]) in range(1,150):
            paraHours = args[1]
            print(paraHours)
        else:
            print("Parameter zu groß")
            sys.exit(0)
            
    else:
        # Verbindung mit Datenbank aufbauen
        engine = connect_db_engine()

        df_max = pd.read_sql("SELECT MAX(TimeCET) FROM binance_price", engine)

        # Datenbankverbindung loesen
        engine.dispose()

        sys.exit()
    # """
    
    # paraHours = 12
    
    while True:

        # Verbindung mit Datenbank aufbauen
        engine = connect_db_engine() 


        # Startzeit der Verarbeitung in Variable speichern
        dBeginDown = datetime.now()
        

        # Verbindung mit Binance-API herstellen
        client = Client(API_KEY, API_SECRET)


        # Setup der Variablen
        howLong = 1
        dayAgo = str(paraHours) + " hours ago UTC"
        print(dayAgo)
        

        #symbol = "SOLUSDT"

        interval = Client.KLINE_INTERVAL_5MINUTE
        intervalDB = 5
        
        
        # Abruf der cryptos aus DB
        df_pairs = pd.read_sql('SELECT * FROM binance_pairs WHERE Prioritaet > 0', engine)
        # df_pairs.to_csv("./csv/" + "pairs" + "_full.csv", decimal=", ")
        df_len = len(df_pairs)


        # Abruf der cryptos aus DB GD200
        df_pairs200 = pd.read_sql('SELECT pairs FROM `binance_gd200` ORDER BY `startDate` DESC', engine)
        

        # pairs aus DB nach Prioritaet fuer den Downloads setzen
        df_pairs = setPairsPrioritaet(df_pairs, df_pairs200)

            
        # Dataframe durchlaufen um die Daten abzurufen ------------------------------------------------------------
        forcnt = 1
        listNull = []     

        for index, row in df_pairs.iterrows():
            pair = row["pairs"]

            print("--- " + pair + "---- von der Exchange herunterladen")
            
            # Download der Kurse von Exchange
            df, df_Ok = GetHistoricalData(client, row["pairs"], interval, dayAgo, "")
            
            # wenn kein price geholt, dann pair in Liste speichern und for Schleife 
            # auf den naechsten Wert springen
            if (not df_Ok):

                print(row["pairs"] + " keine Datensaetze !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                # pair ohne Daten in Liste speichern
                listNull.append(row["pairs"])
                print(str(forcnt) + " von " + str(df_len) + " " + row["pairs"] + " = 0 Datensaetze")
                forcnt += 1
                
                continue 


            # Technische Analyse von TradingView holen und in DB.binance_pairs speichern
            pairTA = GetTAfromTV(pair, '5m')
            sqlstr = "UPDATE `binance_pairs` SET `TechnAnalyse`='" + pairTA + "' WHERE pairs = '" + pair + "'"
            sql.execute(sqlstr, engine) 


            # Zusatz-Info in df einfügen: pair,interval,TrendAnalyse
            df.insert(0, "pairs", row["pairs"])
            df.insert(1, "interval", intervalDB)
            df.insert(2, "trendTA", '')
            idflen = len(df)
            df.iloc[idflen -1, 2] = pairTA
            
            # Daten temporaer in binance_price_temp speichern
            df.to_sql("binance_price_temp", engine, if_exists='replace')
            
            
            # Daten von binance_price_temp nach binance_price verschieben
            # rs = engine.execute('INSERT IGNORE INTO binance_price SELECT * FROM binance_price_temp')
            rs = engine.execute("""
                    INSERT INTO binance_price(`TimeCET`, `pairs`, `interval`, `trendTA`, `open`, `high`, `low`, `close`, `volume`)
                    SELECT * FROM binance_price_temp t
                    ON DUPLICATE KEY UPDATE `open`=t.`open`, `high`=t.`high`, `low`=t.low, `close`=t.close, `volume`=t.volume
                            """)
            

            # Sind Datensaetze vorhanden
            idflen = len(df)
            if(idflen > 0) or not df.empty:
                # price in Auswertungstabellen anpassen
                ActPriceInTable(engine, df, pair)

            
            # pair in DB.binance_price_down speichern fuer Auswertung m_evaluation_price.py
            rs = engine.execute("INSERT INTO binance_price_down(pairs, TechnAnalyse) Values('" + pair + "', '" + pairTA + "')")

            print(str(forcnt) + " von " + str(df_len) + " " + row["pairs"] + " = " + str(len(df)) + " Datensaetze")
            forcnt += 1

        # Abschlussarbeiten nach Download ------------------------------------------------
       


        # pairs ohne price aus download in DB-Tabelle und CSV speichern
        print("pairs ohne price aus download in DB-Tabelle und CSV speichern")
        df_Null = pd.DataFrame(listNull, columns=["pair_noPrice"])
        if len(df_Null) > 0:
            df_Null.to_sql("binance_pairsNoPrice", engine, if_exists='replace')
            
            # pairs ohne price in CSV speichern
            sDate = dBeginDown.strftime("%Y%m%d_%H%M")
            
            f = open("./log/pairsNull_" + sDate + ".txt", "w")
            for pairNull in listNull:
                f.write(pairNull + '\n')
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
