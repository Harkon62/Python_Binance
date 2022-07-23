# https://stackoverflow.com/questions/66295187/how-do-i-get-all-the-prices-history-with-binance-api-for-a-crypto-using-python

from binance.client import Client
from datetime import datetime, timezone, timedelta
import pandas as pd
import pytz
from tzlocal import get_localzone
import talib.abstract as ta
import numpy
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData


def GetHistoricalData(client, symbol, interval, fromDate, toDate):
    
    try:
        klines = client.get_historical_klines(symbol, interval, fromDate)  # fromDate, toDate)
    except Exception as e:
        print("Connection error while making request", e)
        return False

    df = pd.DataFrame(klines, columns=['dateTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime', 'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol', 'ignore'])
    
    df.dateTime = pd.to_datetime(df.dateTime, unit='ms')
    df = df.set_index("dateTime")
    
    europe = pytz.timezone('Europe/Berlin')
    df.index = df.index.tz_localize(pytz.utc).tz_convert(europe)

    df["TimeCET"] = df.index.strftime('%Y-%m-%d %H:%M:%S')
    df["TimeCET"] = pd.to_datetime(df["TimeCET"])
    
    df = df.set_index("TimeCET")

    df = df.astype(float)
    
    # Daten speichern in csv
    # df.to_csv("./csv/" + symbol + "_full.csv", decimal=",")
    
    df = df.drop(['closeTime', 'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol', 'ignore'], axis=1)
    column_names = ["open", "high", "low", "close", "volume"]
    df = df.reindex(columns=column_names)
    
    # df.to_csv("./csv/" + symbol + "_full00.csv", decimal=",")
    
    return df

def GetSelectedData(client, symbol, interval, howLong):
    toDateNow = datetime.now()

    toDate = toDateNow.strftime('%d/%m/%Y %H:%M:%S')

    fromDatePast = toDateNow - timedelta(days=howLong)
    fromDate = fromDatePast.strftime('%d/%m/%Y %H:%M:%S')

    df = GetHistoricalData(client, symbol, interval, fromDate, toDate)
    
    return True, df
