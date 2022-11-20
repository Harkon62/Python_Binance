import pandas as pd
import talib.abstract as ta
import numpy as np


# ?. Daten auswerten, ob eine dritte grüne Kerzen erzeugt wird und der close am hoechsten der letzten 2 Kerzen

def Get3CandleUp(df, symbol, pairTA):
    print("Modul Get3CandleUp gestartet mit Symbol=" + symbol + " ###########################################")
    
    list3c = []
    
    C1close = df.iloc[len(df) - 1]["close"]
    C1open = df.iloc[len(df) - 1]["open"]
    C1high = df.iloc[len(df) - 1]["high"]
    C1ma50 = df.iloc[len(df) - 1]["fast50ma"]
    
    C2close = df.iloc[len(df) - 2]["close"]
    C2open = df.iloc[len(df) - 2]["open"]
    
    C3close = df.iloc[len(df) - 3]["close"]
    C3open = df.iloc[len(df) - 3]["open"]
    C3low = df.iloc[len(df) - 3]["low"]
    
    C1lowma = df.iloc[len(df) - 1]["lowma"]
    
    candleTrue = False
    maxTrue = False
    gd200True = False
    break50True = False
    
    # nur gruene Kerzen
    if (C1close > C1open) and (C2close > C2open) and (C3close > C3open):
        candleTrue = True
    
    # last close = max
    if (C1close > C2close) and (C1close > C3close):
        maxTrue = True
    
    # close > gd200
    if C1close > C1lowma:
        gd200True = True
        
    # break gd50
    if (C3low < C1ma50) and (C1high > C1ma50):
        break50True = True
    
    CDiff = C1close - C3close
    CDiffProz = CDiff * 100 / C3close
    
    # candleTrue = True
    # maxTrue = True
    # gd200True = True
        
    if candleTrue & maxTrue & gd200True & break50True:
        
        # dictProfit = {}
        # dictProfit["Symbol"] = symbol
        # dictProfit["C1close"] = C1close
        # dictProfit["C2close"] = C2close
        # dictProfit["C3close"] = C3close
        # dictProfit["C1lowma"] = C1lowma
        # dictProfit["Date"] = df.index.max()
        
        
        list3c.append(symbol)
        list3c.append(pairTA)
        list3c.append(C1close)
        list3c.append(C1open)
        list3c.append(C2close)
        list3c.append(C2open)
        list3c.append(C3close)
        list3c.append(C3open)
        list3c.append(C1lowma)
        list3c.append(C3low)
        list3c.append(C1ma50)
        list3c.append(C1high)
        list3c.append(CDiff)
        list3c.append(CDiffProz)
        
        list3c.append(df.index.max())
        
    print("                   beendet ###########################################")    
         
    return list3c    #, dictProfit
    