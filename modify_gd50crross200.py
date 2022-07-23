# https://stackoverflow.com/questions/66295187/how-do-i-get-all-the-prices-history-with-binance-api-for-a-crypto-using-python

import pandas as pd
import talib.abstract as ta
import numpy as np


# 2. Daten auswerten, ob price cross GD200, Rückgabewert = Python Liste -------------------------------------------------------------
# Auswertung mit aktuellen price ueber dem GD200
# Rueckgabe Liste: startDate, pairs, startClose, endDate, endClose, endDiff
def GetGD50cross200(df, symbol):
    print("Modul GetGD50cross200 gestartet mit Symbol=" + symbol + " ###########################################")
    
    listProfit = []
     
    # nur Werte, wo der Indikator lowma groesser 0
    # df.to_excel("output.xlsx", sheet_name=symbol)
    df = df[df.lowma > 0]
    df = df[df.fast50ma > 0]

    # Pruefen, ob dataframe die minimal-Groesse von 400 erreicht, sonst return leere Liste
    idflen = len(df)
    if idflen < 400:
        return listProfit

    # df_json = df.iloc[len(df) - 1].to_json()
    # print(df_json)

    df['index'] = df.index

    endDate = df.iloc[len(df) - 1]['index']
    # print(endDate)
    endClose = df.iloc[len(df) - 1]["close"]
    # print(endClose)

    
    # Spalte buy mit dem Wert 0 vorbelegen
    pd.set_option('mode.chained_assignment', None)
    df.loc[:, 'buy'] = 0


    # Spalte close mit lowma vergleichen, Ergebnis = True = 1 setzen
    df.loc[
        (
            (df['lowma'] < df['fast50ma']) & (df['lowma'].shift(1) > df['fast50ma'].shift(1))
        ),
        'buy'] = 1

    
    # print("df buy=1 gefunden, 50 cross 200")
    df_signal = df[df.buy == 1] 
    # df.to_excel("output.xlsx", sheet_name='df_signal')

    # print(df_signal)

    dflen = len(df_signal)
    if dflen > 0:
        startDate = df_signal.iloc[dflen - 1]['index'].strftime('%Y-%m-%d %H:%M')
        listProfit.append(startDate)

        listProfit.append(symbol)
        listProfit.append(df_signal.iloc[dflen - 1]["close"])

        listProfit.append(endDate)
        listProfit.append(endClose)

        startClose = df_signal.iloc[dflen - 1]["close"]
        endDiff = (endClose - startClose) * 100 / startClose
        listProfit.append(endDiff)

    print("                      beendet mit Symbol=" + symbol + " ###########################################")
    
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
    