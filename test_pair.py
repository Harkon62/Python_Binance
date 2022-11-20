from modul_db import connect_db_engine
from modul_db import getPairsPrice
from modify_volume import getVolumeChange

import pandas as pd
import talib.abstract as ta
import numpy as np
import sys
   
    
    
# Execute the following code only when executing main.py (not when importing it) ----------------------------------------------------------------------------------
if __name__ == '__main__':

    pair = "IOTAUSDT"

    # Datenbankverbindung aufbauen
    engine = connect_db_engine()

    # Daten aus DB.binance_price  holen
    df_pairsprice = getPairsPrice(pair, engine, "ASC")


    listVolTime = ['30m', '1h', '2h']
    steps = 3
    
    stepValue, stepName = getVolumeChange(df_pairsprice, listVolTime[0], steps)
    print(stepValue, stepName)

    sys.exit()


