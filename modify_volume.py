# https://stackoverflow.com/questions/66295187/how-do-i-get-all-the-prices-history-with-binance-api-for-a-crypto-using-python

import pandas as pd
from datetime import timedelta

from backup.modul_db import getMaxDateInDB
    


    
def setVolumeSignal(df_pairsprice):

    print("Modul etVolumeSignal gestartet  ###########################################")
    
    # die ? neuesten Zeilen aus dem df nehmen
    df_Volume = df_pairsprice.tail(12)
    df_Volume = df_Volume.reset_index()
    
    # print(df_Volume)

    listVol = []

    for x in range(11, 0, -1):
        
        pairVol0 = df_Volume.iloc[x]["volume"]
        pairTime0 = df_Volume.iloc[x]["TimeCET"]
        pairClose0 = df_Volume.iloc[x]["close"]
        
        pairPair0 = df_Volume.iloc[x]["pairs"]

        pairVol1  = df_Volume.iloc[x - 1]["volume"]
        pairTime1 = df_Volume.iloc[x - 1]["TimeCET"]
        pairClose1 = df_Volume.iloc[x - 1]["close"]

        if (pairVol0 / 6) > pairVol1:
            # print("Alarm")
            listVol.clear()
            listVol.append(pairTime0)
            listVol.append(pairVol0)
            listVol.append(pairClose0)

            listVol.append(pairPair0)

            listVol.append(pairTime1)
            listVol.append(pairVol1)
            listVol.append(pairClose1)
            if pairClose0 > pairClose1:
                listVol.append(1)
            else:
                listVol.append(0)
                
    print("                     beendet mit " + str(len(listVol)) + "###########################################")
    
    return listVol



def set_DB_AVG_Volumen(df_pairsprice, pair, engine):

    print("Modul set_DB_AVG_Volumen gestartet  ###########################################")

    # DataFrame mit dem aktuellsten Datum
    df_MaxDate = getMaxDateInDB(engine)

    dfmaxdate = df_MaxDate[df_MaxDate['pairs'] == pair]

    if dfmaxdate.empty:
        print("df leer")
        return ""

    maxdate = str(dfmaxdate.iat[0,1])

    # Convert Dataframe to dictionary
    #pairDatedict = df_MaxDate.set_index('pairs').T.to_dict("list")

    #maxdate = str(pairDatedict[pair][0])

    # AVG-Volumen der Cryptos aus DB holen ---------------------------------------

    sqlstr = "SELECT pairs, AVG(volume) AS VolAVG FROM binance_price " + \
        "WHERE TimeCET > CURDATE() AND TimeCET < '" + maxdate + \
        "' AND pairs='" + pair + "' GROUP BY `pairs`"
    df_pairsVol = pd.read_sql_query(sqlstr, engine)

    print("                         beendet  ###########################################")

    if len(df_pairsVol) > 0:
        return df_pairsVol.iat[0, 1]
    else:
        return ""



def getVolumeChange(df, timeStep, stepCnt):
    
    # TimeStep in Minuten umrechnen
    if timeStep == '15m':
        minStep = 15
    if timeStep == '30m':
        minStep = 30
    elif timeStep == '1h':
        minStep = 60
    elif timeStep == '2h':
        minStep = 120
    
    # list fuer Werte und Column-Namen
    stepValue = []
    stepName = []
    
    # neuestes Datum ermitteln
    beginDate = df.index.max()
    
    for i in range(0, stepCnt):
        endDate = beginDate - timedelta(minutes=minStep)
    
        df_tmp =  df[(df.index <= beginDate) & (df.index >= endDate)]
        
        Vol = df_tmp.volume.sum()
        
        stepValue.append(beginDate)
        stepName.append('Step' + str(i + 1))
        
        stepValue.append(Vol)
        stepName.append(timeStep + str(i + 1))
        
        beginDate = endDate
        
    return stepValue, stepName