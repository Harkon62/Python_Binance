# https://python.plainenglish.io/how-to-set-up-and-use-binance-api-with-python-3e33a51210d9
# How to Set up and Use Binance API with Python

import pandas as pdasset="BTCUSDT"
start="2021.10.1"
end="2021.11.1"
timeframe="1d"

df =pd.DataFrame(assert,timeframe,start,end)

df= pd.DataFrame(client.get_historical_klines(asset, timeframe,start,end))df=df.iloc[:,:6]
df.columns=["Date","Open","High","Low","Close","Volume"]
df=df.set_index("Date")
df.index=pd.to_datetime(df.index,unit="ms")
df=df.astype("float")print(df)

pip install mplfinance

import mplfinance as mpl
#Insert the code for the creation of the DataFrame that we see earliermpl.plot(df, type='candle')

mpl.plot(df, type='candle')

mpl.plot(df, type='candle', volume=True, mav=7)
