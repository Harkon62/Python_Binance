# Auswertung Price Veränderung in Stufen z.B.15 30 45 60 min
from sqlalchemy import true

from modify_gd200 import GetGD200Data, GetGD200Vol
from modify_gd50crross200 import GetGD50cross200
from modul_interval import pairs_interval_change_quarter, pairs_interval_rangliste, pairs_interval_positiv
from modify_volume import set_DB_AVG_Volumen, setVolumeSignal
from modul_rsima import GetRSIMAData

from modul_freqtrade import SetPairsInFreqtradeFromDB
from modul_TradingView import savePairsGD200inTV
from modul_telegram import telegram_bot_sendtext
from modul_db import connect_db_engine, getPairs, getPairsVolume, volume_getdata, empty_DB_Table, \
                     getPairsPrice, checkTableExists


import pandas as pd
from pandas.io import sql
import talib.abstract as ta
import numpy as np
from datetime import datetime, timedelta
import sys
import time
import subprocess


def pairs_auswertung(pair, pairTA, engine):
    print("\nAuswertung starten mit " + pair + " ###############################################\n")
    
    # pair = "NEOUSDT"

    # Daten aus DB.binance_price  holen
    print("Daten aus DB.binance_price  holen")
    df_pairsprice = getPairsPrice(pair, engine, "")
    idflen = len(df_pairsprice)

    # Sind Datensaetze vorhanden
    if(idflen == 0) or df_pairsprice.empty:
        print(" Modul m_evaluation_price, keine Datensaetze gefunden")
        return


    # Pair-Volumen pro Tag ermitteln
    print("Pair-Volumen pro Tag ermitteln")
    pairVol = getPairsVolume(df_pairsprice, pair)

    # Volume in DB.binance_volume eintragen bzw aktualisieren
    sqlstrdel = "DELETE FROM binance_volume WHERE pairs = '" + pair + "'"
    engine.execute(sqlstrdel)

    sqlstr = "INSERT INTO binance_volume(`pairs`, `SUM_volume`) VALUES ('" + pair + "'," + str(pairVol) + ") "
    # + \
    #             "ON DUPLICATE KEY UPDATE `SUM_volume`=" + str(pairVol)

    engine.execute(sqlstr)



    # Indikatoren in df_pairsprice setzen
    print("Indikatoren in df_pairsprice setzen")

    # Indikator MA200 in Spalte lowma speichern
    df_pairsprice['lowma'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=200, price='close'))  
    # print("Anzahl Datensaetze lowma nach lowma setzen = " + str(idflen))

    # Indikator MA50 in Spalte lowma speichern
    df_pairsprice['fast100ma'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=100, price='close'))  
    # print("Anzahl Datensaetze lowma nach lowma setzen = " + str(idflen))

    # Indikator MA50 in Spalte lowma speichern
    df_pairsprice['fast50ma'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=50, price='close'))  
    # print("Anzahl Datensaetze lowma nach lowma setzen = " + str(idflen))

    # Indikator RSI aus close ermitteln
    df_pairsprice['rsi'] = np.nan_to_num(ta.RSI(df_pairsprice, timeperiod=14, price='close'))

    # ma aus Indikator RSI ermitteln
    df_pairsprice['rsima'] = np.nan_to_num(ta.MA(df_pairsprice, timeperiod=14, price='rsi'))

    # df_pairsprice.to_csv("pairprice.csv")



    """
     # 1. Daten auswerten price Interval, Parameter Anzahl und Minuten ----------------------------------------------
    df = pairs_interval_change_quarter(df_pairsprice, pair, 4, 60)

    # DB.binance_profit_std von pair leeren
    sqlstr = "DELETE FROM binance_profit_std WHERE pairs = '" + pair + "'"
    sql.execute(sqlstr, engine)

    if not df.empty:
        df.to_sql("binance_profit_std", engine, if_exists='append')

    df = pairs_interval_change_quarter(df_pairsprice, pair, 4, 15)

    # DB.binance_profit_quarter von pair leeren
    sqlstr = "DELETE FROM binance_profit_quarter WHERE pairs = '" + pair + "'"
    sql.execute(sqlstr, engine)

    if not df.empty:    
        df.to_sql("binance_profit_quarter", engine, if_exists='append')

    df = pairs_interval_change_quarter(df_pairsprice, pair, 4, 1440)

    # DB.binance_profit_tgl von pair leeren
    sqlstr = "DELETE FROM binance_profit_tgl WHERE pairs = '" + pair + "'"
    sql.execute(sqlstr, engine)

    if not df.empty:
        df.to_sql("binance_profit_tgl", engine, if_exists='append')
    """


    # 2. Daten auswerten, ob price cross GD200, Rückgabewert = Python Liste -------------------------------------
    # Auswertung mit aktuellen price ueber dem GD200
    # Rueckgabe Liste: startDate, pairs, startClose, endDate, endClose, endDiff

    # DB.binance_gd200 von pair leeren
    sqlstr = "DELETE FROM binance_gd200 WHERE (pairs = '" + pair + "')"
                                            #  OR (`endDiff` < 0.5)"
    sql.execute(sqlstr, engine)

    listGD = GetGD200Data(df_pairsprice, pair)

    if len(listGD) > 0:
        # CrossDatum aus Liste holen,Volumen nach gd200 und vor gd200 ermitteln ---------------------------------
        # AVG in DB.tabelle speichern 
        listGD = GetGD200Vol(df_pairsprice, listGD[0], listGD)
        lenGD = len(listGD)
        if lenGD != 9:
            print("Differenz GD200 bei Spalten")
            
        dfgd = pd.DataFrame([listGD], columns=["startDate", "pairs", "startClose", "endDate", \
                                                   "endClose", "endDiff", "gdlowerCnt", "Volbefore", "Volafter"])

        # Daten in binance_gd200 speichern
        dfgd.to_sql("binance_gd200", engine, if_exists='append')


        print("Telegram Nachricht senden ?")
        # Aktualitaet der Nachricht pruefen, nicht aelter als ?? min und Trendanalyse
        iprice = listGD[2]

        ddatenow = datetime.now()
        ddatepair = listGD[0]
        ddateborder = ddatenow - timedelta(minutes=30)
        
        if (ddatepair > ddateborder) and ((pairTA == "STRONG_BUY") or (pairTA == "BUY")) and (iprice > 0.0001):
            # Send message to telegram
            telegram_bot_sendtext(pair + " " + str(listGD[0]))
            
            # Export in freqtrade: config.json
            SetPairsInFreqtradeFromDB(engine, 'USDT')

            # Copy config.json to wsl.freqtrade
            subprocess.call('Bat_Copy_config.bat', timeout=5)

            savePairsGD200inTV(engine, 'USDT')


    """
    # 3. Daten auswerten Volumen-Anstieg---------------------------------------------------------------------------------------------------------
    # Veraenderung im Volumen sehr gross
    listVol = setVolumeSignal(df_pairsprice)

    # dlen = len(listVol)
    # print(dlen)
    # print(listVol)        
    if len(listVol) > 0:
        # tgl. Durchschnittliche Volumen ermitteln und in Liste hinzufuegen
        avgVol = set_DB_AVG_Volumen(df_pairsprice, pair, engine)
        listVol.append(avgVol)
                    
        dfvol = pd.DataFrame([listVol], columns=["Date00", "Volume00", "Close00", "pairs", "Date01","Volume01", "Close01", "buysell", "AvgVolumen"])

        # DB.binance_volSignal von pair leeren
        sqlstr = "DELETE FROM binance_volSignal WHERE pairs = '" + pair + "'"
        sql.execute(sqlstr, engine)

        dfvol.to_sql("binance_volSignal", engine, if_exists='append')
    """

    """
    # 4. Daten auswerten, RSI cross-above RSIma
    # 
    # DB.binance_rsima + DB.binance_rsisignal von pair leeren
    if checkTableExists(engine, 'binance_rsima'):
        sqlstr = "DELETE FROM binance_rsima WHERE pairs = '" + pair + "'"
        sql.execute(sqlstr, engine)
        if checkTableExists(engine, 'binance_rsisignal'):
            sqlstr = "DELETE FROM binance_rsisignal WHERE pairs = '" + pair + "'"
            sql.execute(sqlstr, engine)
    GetRSIMAData(df_pairsprice, pair, engine)
    """

    """
    # 5. Rangliste der pairs aus DB.binance_profit_quarter erstellen
    df = pairs_interval_rangliste(engine, "binance_profit_quarter", 4)
    df.to_sql(name='binance_profit_rang', con=engine, if_exists='replace', index=False)


    # 6. aus den interval-Tabellen Werte mit den Anzahl-maeßig meisten Steigerungen
    if checkTableExists(engine, 'binance_rsima'):
        dfMsg = pairs_interval_positiv(engine)
    """

    """
    # 7. MA50 cross den MA200
    # DB.binance_rsima + DB.binance_rsisignal von pair leeren
    if checkTableExists(engine, 'binance_gd50cross200'):
        sqlstr = "DELETE FROM binance_gd50cross200 WHERE pairs = '" + pair + "'"
        sql.execute(sqlstr, engine)
    listGD50c200 = GetGD50cross200(df_pairsprice, pair)

    if len(listGD) > 0:
        dfgd = pd.DataFrame([listGD50c200], columns=["startDate", "pairs", "startClose", "endDate", \
                                                   "endClose", "endDiff"])

        # Daten in binance_gd50cross200 speichern
        dfgd.to_sql("binance_gd50cross200", engine, if_exists='append')
    """


    print("----Auswertung beendet mit " + pair + " ###############################################")



# Execute the following code only when executing main.py (not when importing it) ----------------------------------------------------------------------------------
if __name__ == '__main__':
    # uebergebene Parameter auswerten -------------------------------------------
    # Parameter 1 = loop Durchlauf nach ? Sek. wiederholen
    # Parameter 2 = onlyGD = nur GD200 auswerten
    
    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv))

    args = sys.argv
    loop = False
    onlyGD = False
    
    if len(sys.argv) > 1:
        print(args[1])
        if args[1].lower() == "loop":
            loop = True
            print("Parameter = loop")      
    else:
        loop = False
        print("Parameter = no loop")

    # temporaer gesetzte Variablen    
    loop = True
    #onlyGD = True
    

    dBeginDown = datetime.now()
    
    if loop:
        print("Loop starten bei " + str(datetime.now()))


        # Datenbankverbindung aufbauen ------------------------------------------
        engineEmpty = connect_db_engine()


        # Tabellen leeren
        empty_DB_Table(engineEmpty)

        
        # zu Testzwecken alle pairs in die DB.binance_price_down schieben
        # sqlstr = "INSERT INTO `binance_price_down` SELECT pairs FROM `binance_pairs`"
        # engineEmpty.execute(sqlstr)


        # Datenbankverbindung schliessen
        engineEmpty.dispose()



        # SQL Abfragen fuer Daten aus DB.binance_pairs vorbelegen --------------------------------------------------------------
        #sqlstrEins = "SELECT pairs, Prioritaet FROM binance_pairs WHERE Prioritaet=1"
        sqlstrEins = "SELECT pairs, TechnAnalyse FROM binance_price_down LIMIT 1"

        sqlstrNull = "SELECT pairs, Prioritaet FROM binance_pairs WHERE Prioritaet=0"

        # Datenbankverbindung aufbauen ------------------------------------------
        print("_main_: " + str(datetime.now(tz=None)) + " Datenbankverbindung aufbauen")
        engine = connect_db_engine()

        while loop:
            # Datenbankverbindung aufbauen ------------------------------------------
            # print("_main_: " + str(datetime.now(tz=None)) + " Datenbankverbindung aufbauen")
            # engine = connect_db_engine()
            
            # einen Datensatz pair mit Prioritaet = 1 = Download abgeschlossen
            df_pairs = pd.read_sql_query(sqlstrEins, engine)
            
            if len(df_pairs) > 0:
                # Name des pair aus der Zeile holen
                pair = df_pairs.loc[0, "pairs"]
                pairTA = df_pairs.loc[0, "TechnAnalyse"]

                # pair = "UTKUSDT"
                
                #Auswertung starten
                print("_main_: " + str(datetime.now(tz=None)) + " Auswertung starten")
                pairs_auswertung(pair, pairTA, engine)
                

                # pair aus DB.binance_price_down löschen, Bearbeitung fertig
                print("_main_: pair aus DB.binance_price_down löschen")
                engine.execute("DELETE FROM binance_price_down WHERE pairs='" + pair + "'")

                print("_main_: " + str(datetime.now(tz=None)) + " Datensatz " + pair + " beendet")
            else:
                # sind noch Daetensatze mit Prioritaet = 0 = Download offen
                # wenn ja, Warteschleife beginnen, nein 
                df_pairsNull = pd.read_sql_query(sqlstrNull, engine)

                if len(df_pairsNull) > 0:
                    # Warten
                    print("Sleep 2, warte")
                    time.sleep(2)
                else:
                    # Warten
                    print("Sleep 30, warte auf naechsten Durchlauf")
                    time.sleep(30)
                    # break

            # Datenbankverbindung schliessen
            # print("_main_: " + str(datetime.now(tz=None)) + " Datenbankverbindung schliessen")
            # engine.dispose()
            
        print("Loop Ende bei " + str(datetime.now()))
    else:
        # keine Schleife, einfache Bearbeitung

        # Datenbankverbindung aufbauen ------------------------------------------
        engine = connect_db_engine()

        # Tabellen leeren
        empty_DB_Table(engine)
        
        # DB.binance_volume erzeugen
        volume_getdata(engine)
        
        # DB.binance_pairs Daten in Dataframe zurueckgeben
        df_pairs = getPairs(engine)

        # Dataframe durchlaufen um die Auswertung für den Crypto abzurufen
        for index, row in df_pairs.iterrows():
            # Name des pair aus der Zeile holen
            pair = row["pairs"]
            #pair = "CFXUSDT"
            
            # Table binance_profit, binance_gd200, binance_volSignal auswerten
            pairs_auswertung(pair, engine)

            # Table binance_profit_tgl auswerten
            # start_Auswertung
    
    # Datenbankverbindung schliessen
    engine.dispose()

    print("Beginn Auswertung GD und Intervalle " + str(dBeginDown))
    print("Ende Auswertung GD und Intervalle" + str(datetime.now()))