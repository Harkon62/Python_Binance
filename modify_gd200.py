# https://stackoverflow.com/questions/66295187/how-do-i-get-all-the-prices-history-with-binance-api-for-a-crypto-using-python

import pandas as pd
import talib.abstract as ta
import numpy as np


# 2. Daten auswerten, ob price cross GD200, Rückgabewert = Python Liste -------------------------------------------------------------
# Auswertung mit aktuellen price ueber dem GD200
# Rueckgabe Liste: startDate, pairs, startClose, endDate, endClose, endDiff
def GetGD200Data(df, symbol):
    print("Modul GetGD200Data gestartet mit Symbol=" + symbol + " ###########################################")
    
    listProfit = []
    
    # Pruefen, ob dataframe die minimal-Groesse von 400 erreicht, sonst return leere Liste
    idflen = len(df)
    if idflen < 400:
        return listProfit

    
    #print("Anzahl Datensaetze lowma nach lowma setzen = " + str(dflen))
    # df.to_csv("./csv/" + symbol + "_0lowna01.csv", decimal=",")
    #df.to_sql("binance_gd200_temp", engine, if_exists='replace')
    
    # nur Werte, wo der Indikator lowma groesser 0
    df = df[df.lowma > 0]
    
    # print("Anzahl Datensaetze lowma nach lowma > 0 = " + str(len(df)))
    # df.to_csv("./csv/" + symbol + "_1lowna02.csv", decimal=",")
    
    # Spalte buy mit dem Wert 0 vorbelegen
    pd.set_option('mode.chained_assignment', None)
    df.loc[:, 'buy'] = 0
    
    # df.to_csv("./csv/" + symbol + "_2buy.csv", decimal=",")
    # df.to_sql("binance_gd200_temp", engine, if_exists='replace')

    # Spalte close groesser lowma , Ergebnis = True = 1 setzen
    df.loc[
        (
            (df['lowma'] < df['close'])
        ),
        'buy'] = 1
    # df.to_csv("./csv/" + symbol + "_3buy1.csv", decimal=",")
    
    # Daten speichern in csv
    # df.to_csv("./csv/" + symbol + "_gd.csv", decimal=",")
    #df.to_sql("binance_gd200_temp", engine, if_exists='replace')
    
    # neuesten Datensatz pruefen, ob der GD200 unter dem close
    df_len = len(df)

    if df_len > 0:
        # print("neuesten Datensatz, Spalte buy ------------------------")
        signal = df.iloc[len(df) - 1]["buy"]
        # print("Signal = " + str(signal))
    else:
        signal = 0

    # df.to_excel("gd200_df.xlsx", sheet_name=symbol)

    # nur wenn GD200 < close, dann Auswertung erstellen, ab wann der GD200 den close gekreuzt hat   
    if signal == 1:
        
        # print("df buy=1 gefunden, GD200 unter close")
        nulldf = df[df.buy == 0]   

        # nur wenn Datensaetze mit buy = 0 vorhanden
        if len(nulldf) > 0:
            # print("MAX Funktion auf den Index  ---------------------------")
            maxDate = nulldf.index.max()
            # print("neuestes Datum mit buy=0 Datum=" + str(maxDate))
            
           # print("df mit Datum und buy=1 filtern----------------")
            df_buy = df[df.index > maxDate]
            # print("Anzahl Datensaetze=" + str(len(df_buy)))
        
        # print("MAX, MIN Funktion auf den Index  ---------------------------")
        # das erste Datum aus GD200=buy=1 in Liste speichern
        minDate = df_buy.index.min()
        listProfit.append(minDate)

        # Symbol in Liste
        listProfit.append(symbol)

        # das erste Close aus GD200=buy=1 in Liste speichern
        minDatePrice = df_buy.iloc[0,5]   #CFX
        listProfit.append(minDatePrice)
        
        # das letzte Datum aus GD200=buy=1 in Liste speichern
        maxDate = df_buy.index.max()
        listProfit.append(maxDate)
        
        # das letzte Close aus GD200=buy=1 in Liste speichern
        maxDatePrice = df_buy.iloc[len(df_buy)-1, 5]                             
        listProfit.append(maxDatePrice)
        
        # Profit in Prozent aus Differenz von max_close zu min_close ermitteln
        ProzPrice = (maxDatePrice - minDatePrice) *100 / minDatePrice

        listProfit.append(ProzPrice)

        # if ProzPrice < 0:
        #    df.to_excel("gd200_df.xlsx", sheet_name=symbol)

        # Anzahl der Werte close > lowma ermitteln
        # 7 Tage x 24Std x 12Einheiten(5m) = 2016
        # 3 Tage x 24Std x 12Einheiten(5m) = 864
        # 2 Tage x 24Std x 12Einheiten(5m) = 576
        #          12Std x 12Einheiten(5m) = 144

        # print(df.tail(5))
        # print(df.head(5))

        # Wieviele Tage befand sich das Close ueber dem GD ?
        if df_len >= 2016:
            # Werte aus ?? Zeit-Intervallen
            df_gd = df.tail(2016)
            
            # nur die Werte mit close > gd200
            df_buy_len = df_gd.query("buy == 1").shape[0]
            listProfit.append(df_buy_len)
        else:
            listProfit.append(0)

        # Wieviele Tage befand sich das Close ueber dem GD ?
        if df_len >= 144:
            # Werte aus ?? Zeit-Intervallen
            df_gd = df.tail(144)
            
            # nur die Werte mit close > gd200
            df_buy_len = df_gd.query("buy == 1").shape[0]
            listProfit.append(df_buy_len)
        else:
            listProfit.append(0)
                    
    print("                   beendet ###########################################")    
         
    return listProfit


# durchschnittliches Volumen nach/vor dem GD cross ermitteln
# eine uebergebne Liste wird mit weiteren Daten gefuellt

# df_pairsprice = Dataframe mit den Daten eines pairs aus der DB.finance_price
# crossdate = Datum an dem der GD das close gekreuzt hat
# listVol = Liste mit Daten aus einer vorigen Berechnung (GD200Data)
def GetGD200Vol(df_pairsprice, crossDate, listVol):

    print("Modul GetGD200Vol gestartet  ###########################################")

    # durchschnittliche Volumen-Werte nach dem GD cross ermitteln -------------------
    # Dataframe nach dem Cross selektieren
    dfpast = df_pairsprice[df_pairsprice.index >= crossDate]

    # Anzahl der TimeFrame ermitteln
    dflen = len(dfpast)
    # groesser 12 eingrenzen
    if dflen > 12:
        dflen = 12

    # Durchschnitt aus 12 TimeFrame ermitteln
    dfpastvol = dfpast.head(dflen).volume.mean()
    #print(dfpast.head(dflen))
    #print(dfpastvol)

    # durchschnittliche Volumen-Werte vor dem GD cross ermitteln ----------------------
    dfbefore = df_pairsprice[df_pairsprice.index < crossDate]
    #print(dfbefore)

    #
    dflenb = len(dfbefore)

    if dflenb > 12:
        dflenb = 12

    dfbeforevol = dfbefore.tail(dflenb).volume.mean()
    # print(dfbefore.tail(dflenb))
    # print(dfbeforevol)

    listVol.append(dfbeforevol)
    listVol.append(dfpastvol)
    
    print("                  beendet    ###########################################")

    return listVol
    