from datetime import datetime, timezone, timedelta
from binance.client import Client
import pandas as pd
from connector.binance_api import GetHistoricalData

api_key = "YRh7OHf8IUjumPzc27pVsE4VJKdR8kT7a9oRxDtREpMsivQ6wZ6XwXH3eVcFDHpc"
api_secret = "9CCnqUKby6t6PDbIYj7vBBXg7WNXsUIXnrxpA3vIttKfXGuVmiLpTsqsFZu1fdkH"

client = Client(api_key, api_secret)
#print(client.get_account())

howLong = 1
symbol = "TRXUSDT"
interval = Client.KLINE_INTERVAL_5MINUTE

toDateNow = datetime.now()

toDate = toDateNow.strftime('%d/%m/%Y %H:%M:%S')

fromDatePast = toDateNow - timedelta(days=howLong)
fromDate = fromDatePast.strftime('%d/%m/%Y %H:%M:%S')

#klines = client.get_historical_klines(symbol, interval, fromDate, toDate)

#klines = client.get_historical_klines('TRXUSDT', '5m', '9 hours ago UTC')

df = GetHistoricalData(client, symbol, interval, '9 hours ago UTC', toDate)

#df = pd.DataFrame(klines, columns=['dateTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime', 'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol', 'ignore'])

#df.dateTime = pd.to_datetime(df.dateTime, unit='ms')

#df = df.set_index("dateTime")
