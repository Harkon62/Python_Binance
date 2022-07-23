# https://stackoverflow.com/questions/66295187/how-do-i-get-all-the-prices-history-with-binance-api-for-a-crypto-using-python

from datetime import datetime, timezone, timedelta
import pandas as pd
import pytz
from tzlocal import get_localzone
import matplotlib.pyplot as plt
import talib.abstract as ta
import numpy
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData


def GetGD200Data(df, symbol):
    print("Modul GetGD200Data gestartet ###########################################")
    
    listProfit = []
    
    # Indikator in Spalte lowsma speichern
    df['lowma'] = numpy.nan_to_num(ta.MA(df, timeperiod=200, price='close'))
    print("Anzahl Datensaetze lowma " + str(len(df)))
    
    # nur Werte mit dem Indikator > 0
    df = df[df.lowma > 0]
    print("Anzahl Datensaetze lowma>0 " + str(len(df)))
     
    # Spalte buy mit dem Wert 0 vorbelegen
    df['buy'] = 0

    # Spalte close mit lowma vergleichen, Ergebnis = True = 1 setzen
    df.loc[
        (
            (df['lowma'] < df['close'])
        ),
        'buy'] = 1

    # Daten speichern in csv
    df.to_csv("./csv/" + symbol + "_gd.csv", decimal=",")

    print("neuesten Datensatz, Spalte buy ------------------------")
    signal = df.iloc[len(df) - 1]["buy"]
    print("Signal = " + str(signal))
    
    if (signal == 1):
        

        print("df buy=1 gefunden, GD200 unter close")
        nulldf = df[df.buy == 0]   

        # nur wenn Datensaetze mit buy = 0 vorhanden
        if len(nulldf) > 0:
            print("MAX Funktion auf den Index  ---------------------------")
            maxDate = nulldf.index.max()
            print("neuestes Datum mit buy=0 Datum=" + str(maxDate))
            
            print("df mit Datum und buy=1 filtern----------------")
            df = df[df.index > maxDate]
            print("Anzahl Datensaetze=" + str(len(df)))
        
        print("MAX, MIN Funktion auf den Index  ---------------------------")
        
        minDate = df.index.min()
        listProfit.append(minDate)
        
        listProfit.append(symbol)
        
        minDatePrice = df.iloc[0,5]
        listProfit.append(minDatePrice)
        
        maxDate = df.index.max()
        print(str(maxDate))
        listProfit.append(maxDate)
        
        maxDatePrice = df.iloc[len(df)-1, 5]                             
        print(str(maxDatePrice))
        listProfit.append(maxDatePrice)
        
        ProzPrice = (maxDatePrice - minDatePrice) *100 / minDatePrice
        listProfit.append(ProzPrice)
         
    return listProfit


def GetChangePrice(isBack, df, minStep):
    print("Modul GetChangePrice gestartet ###########################################")
    if isBack:
        print("df kürzen")
        
        # Minuten in der Liste zählen
        minSum = 0
        for singlestep in minStep:
            minSum = minSum + singlestep
            
        ilenStep = len(minStep)
        minSum = minStep[0] + minSum   #minStep[ilenStep-1]
        
        # df kürzen
        lastDate = df.index.max()

        firstDate = lastDate - timedelta(minutes=minSum)
        #firstDate = datetime.strptime(lastDate, '%Y-%m-%d %H:%M:%S') - timedelta(minutes=minSum)
        #print(firstDate)

        df = df[(df.index >= firstDate)]

        #print(df)

    stepProfit = []

    startDate = df.index.min()
    stepProfit.append(startDate)

    startClose = df.loc[startDate]['close']
    stepProfit.append(startClose)

    # Zeitreihe berechnen
    for singlestep in minStep:
        stepDate = startDate - timedelta(minutes=-singlestep)
        stepProfit.append(stepDate)

        stepClose = df.loc[stepDate]['close']
        stepProfit.append(stepClose)

        diffProz = (stepClose - startClose) * 100 / startClose
        stepProfit.append(diffProz)
        
        startDate = stepDate
        startClose = stepClose
        
    endDate = df.index.max()

    stepProfit.append(endDate)

    endClose = df.loc[endDate]['close']
    stepProfit.append(endClose)

    diffend = (endClose - startClose) * 100 / startClose
    stepProfit.append(diffend)

    return stepProfit

def setVolumeSignal(df):
    print("Test")

# Datenbankverbindung aufbauen
def connect_db_engine():
    engine = create_engine(
        'mysql+mysqlconnector://mydb:mariaJF#5031@192.168.178.150:3307/myFiNews')

    return engine

# Datenbankverbindung aufbauen
engine = connect_db_engine()

#Daten aus DB.binance_price  holen
sqlstr = "SELECT * FROM `binance_price` WHERE `pairs`='ATAUSDT' AND TimeCET < '2022-02-11 07:10' ORDER BY `TimeCET` DESC"
df_pairsprice = pd.read_sql_query(sqlstr, engine)
# df_pairsprice = df_pairsprice.set_index("TimeCET")

df_Volume = df_pairsprice.head(12)
print(df_Volume)

listVol = []

for x in range(11):
    pairVol0 = df_Volume.loc[x]["volume"]
    pairTime0 = df_Volume.loc[x]["TimeCET"]
    pairPair0 = df_Volume.loc[x]["pairs"]

    pairVol1  = df_Volume.loc[x + 1]["volume"]
    pairTime1 = df_Volume.loc[x + 1]["TimeCET"]
    
    if (pairVol0 / 4) > pairVol1:
        print("Alarm")
        listVol.clear()
        listVol.append(pairTime0)
        listVol.append(pairVol0)
        
        listVol.append(pairPair0)
        
        listVol.append(pairTime1)
        listVol.append(pairVol1)
    
        dfvol = pd.DataFrame([listVol], columns=["Date00", "Volume00", "pairs", "Date01", "Volume01"])

        dfvol.to_sql("binance_volSignal", engine, if_exists='append')

    print("------------------------------------")
