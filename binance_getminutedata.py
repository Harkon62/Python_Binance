from binance_connector import Client
import pandas as pd
import matplotlib.pyplot as plt
import talib.abstract as ta
import numpy
import pytz
import datetime as dt
from tzlocal import get_localzone

# Daten von Binance als Dataframe speichern und Index, Datentypen setzen
def getminutedata(symbol, interval, lookback):
    frame = pd.DataFrame(client.get_historical_klines(symbol, interval, lookback))
    frame = frame.iloc[:, :6]
    frame.columns = ["Time", "Open", "High", "Low", "close", "Volume"]
    
    mytz = get_localzone()
    #print(mytz)
    
    frame["Time"] = pd.to_datetime(frame["Time"], unit="ms")
  
    frame = frame.set_index("Time")

    europe = pytz.timezone('Europe/Berlin')
    frame.index = frame.index.tz_localize(pytz.utc).tz_convert(europe)
    
    frame["TimeCET"] = frame.index.strftime('%Y-%m-%d %H:%M:%S')
    frame["TimeCET"] = pd.to_datetime(frame["TimeCET"])
    frame = frame.set_index("TimeCET")
    
    frame = frame.astype(float)

    return frame

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    client = Client("nVwQY57hr4SulGdUodkd6arYsq2TuWJ3du2kGMxd7OGdvkN0aA88dthfjDGNRUuf",
                    "4ZtMWIk0mWqBHs1YynLHR6f5aJsZzGRFzcE8DVuAfACIlPKrVnEeb6gn6fUXBM8F")

    client.get_account()
    
    pair = "ETHUSDT"

    # Daten von Binance abrufen
    dataframe = getminutedata(pair, "5m", "3h")
    
    # Ueberfluessige Spalten loeschen
    dataframe = dataframe.drop('High', 1)
    dataframe = dataframe.drop('Low', 1)
    dataframe = dataframe.drop('Volume', 1)

    # Indikator in Spalte lowsma speichern
    dataframe['lowsma'] = numpy.nan_to_num(ta.SMA(dataframe, timeperiod=7))

    # Spalte buy mit dem Wert 0 vorbelegen
    dataframe['buy'] = 0

    # Spalte close mit lowsma vergleichen, Ergebnis 0 oder 1 setzen
    dataframe.loc[
        (
                (dataframe['lowsma'] > dataframe['close'])
        ),
        'buy'] = 1

    df = dataframe[dataframe['buy'] == 0]

    # Daten speichern in csv
    df.to_csv(pair + ".csv")
    
    #print(type(df.dtypes))
    print(df)

    # test.Open.plot()
