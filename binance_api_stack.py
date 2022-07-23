#https://stackoverflow.com/questions/66295187/how-do-i-get-all-the-prices-history-with-binance-api-for-a-crypto-using-python
import os
from binance.client import Client
import pandas as pd
import datetime, time

def GetHistoricalData(self, howLong):
    self.howLong = howLong
    # Calculate the timestamps for the binance api function
    self.untilThisDate = datetime.datetime.now()
    self.sinceThisDate = self.untilThisDate - datetime.timedelta(days = self.howLong)
    # Execute the query from binance - timestamps must be converted to strings !
    self.candle = self.client.get_historical_klines("BNBBTC", Client.KLINE_INTERVAL_1MINUTE, str(self.sinceThisDate), str(self.untilThisDate))

    # Create a dataframe to label all the columns returned by binance so we work with them later.
    self.df = pd.DataFrame(self.candle, columns=['dateTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime', 'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol', 'ignore'])
    # as timestamp is returned in ms, let us convert this back to proper timestamps.
    self.df.dateTime = pd.to_datetime(self.df.dateTime, unit='ms').dt.strftime(Constants.DateTimeFormat)
    self.df.set_index('dateTime', inplace=True)

    # Get rid of columns we do not need
    self.df = self.df.drop(['closeTime', 'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol','takerBuyQuoteVol', 'ignore'], axis=1)

    print(self.df)
