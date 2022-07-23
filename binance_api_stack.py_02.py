# https://stackoverflow.com/questions/66295187/how-do-i-get-all-the-prices-history-with-binance-api-for-a-crypto-using-python

from binance.client import Client
from datetime import datetime, timezone, timedelta
import pandas as pd
import pytz
from tzlocal import get_localzone
import matplotlib.pyplot as plt
import talib.abstract as ta
import numpy
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData

def connect_db_engine():
    engine = create_engine(
        'mysql+mysqlconnector://mydb:mariaJF#5031@192.168.178.150:3307/myFiNews')

    return engine

def GetHistoricalData(symbol, interval, fromDate, toDate):
    
    klines = client.get_historical_klines(symbol, interval, '18 hours ago UTC')  #fromDate, toDate)
    
    df = pd.DataFrame(klines, columns=['dateTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime', 'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol', 'ignore'])
    
    df.dateTime = pd.to_datetime(df.dateTime, unit='ms')
    
    df = df.set_index("dateTime")
    
    europe = pytz.timezone('Europe/Berlin')
    df.index = df.index.tz_localize(pytz.utc).tz_convert(europe)

    df["TimeCET"] = df.index.strftime('%Y-%m-%d %H:%M:%S')
    df["TimeCET"] = pd.to_datetime(df["TimeCET"])
    
    df = df.set_index("TimeCET")

    df = df.astype(float)
    
    df = df.drop(['closeTime', 'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol', 'ignore'], axis=1)
    column_names = ["open", "high", "low", "close", "volume"]
    df = df.reindex(columns=column_names)
    
    return df

def GetSelectedData(symbol, interval, howLong):
    toDateNow = datetime.now()

    toDate = toDateNow.strftime('%d/%m/%Y %H:%M:%S')

    fromDatePast = toDateNow - timedelta(days=howLong)
    fromDate = fromDatePast.strftime('%d/%m/%Y %H:%M:%S')

    df = GetHistoricalData(symbol, interval, fromDate, toDate)
    
    # Daten speichern in csv
    df.to_csv(symbol + "_full.csv", decimal=",")
    
    return df

def GetGD200Data(df):

    # Indikator in Spalte lowsma speichern
    df['lowma'] = numpy.nan_to_num(ta.MA(df, timeperiod=200, price='close'))

    # nur Werte mit dem Indikator > 0
    df = df[df.lowma > 0]

    # Spalte buy mit dem Wert 0 vorbelegen
    df['buy'] = 0

    # Spalte close mit lowsma vergleichen, Ergebnis = True = 1 setzen
    #df = df[df['buy'] == 0]
    df.loc[
        (
            (df['lowma'] < df['close'])
        ),
        'buy'] = 1

    # Daten speichern in csv
    df.to_csv(symbol + "_gd.csv", decimal=",")

    print("neuesten Datensatz, Spalte buy ------------------------")
    signal = df.iloc[len(df) - 1]["buy"]
    
    if (signal == 1):
        print("df buy=1 gefunden, GD200 unter close")
        nulldf = df[df.buy == 0]    

        print("MAX Funktion auf den Index  ---------------------------")
        maxDate = nulldf.index.max()
        print("neuestes Datum mit buy=0 Datum=" + str(maxDate))
        
        print("df mit neuesten Datum und buy=0 filtern----------------")
        df = df[df.index > maxDate]
        print("Anzahl Datensaetze=" + str(len(df)))
         
    return df

def GetChangePrice(df):

    gd200Profit = []

    startDate = df.index.min()
    print(startDate)
    gd200Profit.append(startDate)

    startClose = df.loc[startDate]['close']
    print(startClose)
    gd200Profit.append(startClose)

    step15Date = startDate - timedelta(minutes=-15)
    print(step15Date)
    gd200Profit.append(step15Date)

    step15Close = df.loc[step15Date]['close']
    print(step15Close)
    gd200Profit.append(step15Close)

    diff15 = (step15Close - startClose) * 100 / startClose
    print(diff15)
    gd200Profit.append(diff15)

    step30Date = startDate - timedelta(minutes=-30)
    print(step30Date)
    gd200Profit.append(step30Date)

    step30Close = df.loc[step30Date]['close']
    print(step30Close)
    gd200Profit.append(step30Close)

    diff30 = (step30Close - startClose) * 100 / startClose
    print(diff30)
    gd200Profit.append(diff30)

    endDate = df.index.max()
    print(endDate)
    gd200Profit.append(endDate)

    endClose = df.loc[endDate]['close']
    print(endClose)
    gd200Profit.append(endClose)

    diffend = (endClose - startClose) * 100 / startClose
    print(diffend)
    gd200Profit.append(diffend)

    return gd200Profit
    
     
#api_key = "nVwQY57hr4SulGdUodkd6arYsq2TuWJ3du2kGMxd7OGdvkN0aA88dthfjDGNRUuf"
#api_secret = "4ZtMWIk0mWqBHs1YynLHR6f5aJsZzGRFzcE8DVuAfACIlPKrVnEeb6gn6fUXBM8F"
api_key = "YRh7OHf8IUjumPzc27pVsE4VJKdR8kT7a9oRxDtREpMsivQ6wZ6XwXH3eVcFDHpc"
api_secret = "9CCnqUKby6t6PDbIYj7vBBXg7WNXsUIXnrxpA3vIttKfXGuVmiLpTsqsFZu1fdkH"

client = Client(api_key, api_secret)

howLong = 1
symbol = "SOLUSDT"
interval = Client.KLINE_INTERVAL_1MINUTE

df = GetSelectedData(symbol, interval, howLong)

# df in DB oder Datei sichern ?? -------------------------------

df = GetGD200Data(df)

gdProfit = GetChangePrice(df)
gdProfit.insert(1, symbol)

# Daten speichern in csv
df.to_csv(symbol + ".csv", decimal=",")

dfgd = pd.DataFrame([gdProfit], columns=['startDate', 'pairs', 'startClose', 'step15Date',
                    'step15Close', 'diff15', 'step30Date', 'step30Close', 'diff30', 'endDate', 'endClose', 'diffend'])

print(dfgd)

engine = connect_db_engine()

dfgd.to_sql("binance_gd200", engine, if_exists='append')
