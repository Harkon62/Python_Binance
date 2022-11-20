# Import Pairs aus Datei im Verzeichnis ./log
# Datei enthaelt pairs ohne price-Daten
# Durchlauf der Dateien und zaehlen der pairs, wie oft keine Daten gekommen sind
import os, sys
from pandas.io import sql

from modul_db import connect_db_engine



# Path fuer Einlesen der Dateien setzen
logPath = r"D:\userData\SynologyDrive\Projekte\Python\Projekt_Python_Binance\log"

# Dateien in Liste speichern
listFiles = os.listdir(logPath)

# Dictionary anlegen fuer Ergebnisse des Einlesens
dicPairsCnt = {}

# Durchlauf der Liste
for listFile in listFiles:

    # Pruefen auf .txt File
    if not listFile.endswith(".txt"):
        continue
    
    # Pairs aus Datei lesen und Dictionary suchen und Zaehler hochsetzen
    print(listFile + "------------------------------------")
    f = open(logPath + "\\" + listFile, "r")
    for x in f:
        strPair = f.readline()
        strPair = strPair[0:-1]
        print(strPair) 
        
        if strPair in dicPairsCnt:
            dicPairsCnt[strPair] = dicPairsCnt[strPair] + 1
        else:
            dicPairsCnt[strPair] = 1
    f.close()
    
# Verbindung mit Datenbank aufbauen
engine = connect_db_engine()
    
# Durchlauf des Dictionary: Status in DB.binance_pairs auf 0 setzen, 
#                           Daten aus DB.binance_price loeschen
for dicPairs in dicPairsCnt:
    if dicPairsCnt[dicPairs] > 5:
        print("Pairs " + dicPairs + " in DB.binance_pairs auf inaktiv setzen") 
        
        # Spalte DB.binance_pairs.binance auf 0 setzen
        sqlstr = "UPDATE binance_pairs SET binance = 0, Prioritaet = 0 WHERE pairs = '" + dicPairs + "'"
        sql.execute(sqlstr, engine)
        
        print("Pairs " + dicPairs + " in DB.binance_price loeschen")
        
        # pairs-kurse aus DB.binance_price loeschen
        sqlstr = "DELETE FROM binance_price WHERE pairs = '" + dicPairs + "'"
        sql.execute(sqlstr, engine)
        
print("Programmende") 
       
sys.exit(0)

# CREATE TABLE binance_price_202206_tmp
# SELECT * FROM `binance_price` WHERE TimeCET >= "2022-06-01 00:00" AND TimeCET < "2022-07-01 00:00"
# DELETE FROM `binance_price` WHERE TimeCET >= "2022-06-01 00:00" AND TimeCET < "2022-07-01 00:00"