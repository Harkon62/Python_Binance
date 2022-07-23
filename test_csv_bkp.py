from datetime import datetime, timezone, timedelta
import pandas as pd
import mysql.connector
import pymysql
import mariadb
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData


def connect_db_engine():
    engine = create_engine(
        'mysql+mysqlconnector://mydb:mariaJF#5031@192.168.178.150:3307/myFiNews')

    return engine

def GetChangePrice(isBack, df, minstep):
    
    if isBack:
        print("df kürzen")
        # Minuten in der Liste zählen
        minSum = 0
        for singlestep in minstep:
            minSum = minSum + singlestep
            
        # df kürzen
        lastDate = df.index.max()
        
        firstDate = lastDate - timedelta(minutes=minSum)
        #firstDate = datetime.strptime(lastDate, '%Y-%m-%d %H:%M:%S') - timedelta(minutes=minSum)
        #print(firstDate)

        df = df[(df.index >= firstDate)]

        print(df)
        
    else:
        print("nicht kürzen")
        
    gd200Profit =  []
    
    startDate = df.index.min()
    print(startDate)
    gd200Profit.append(startDate)
    
    startClose = df.loc[startDate]['close']
    print(startClose)
    gd200Profit.append(startClose)
    
    # Minuten in der Liste zählen
    for singlestep in minstep:
        stepDate = startDate - timedelta(minutes=-singlestep)
        print(stepDate)
        gd200Profit.append(stepDate)
    
        stepClose = df.loc[stepDate]['close']
        print(stepClose)
        gd200Profit.append(stepClose)
    
    diff15 = (step15Close - startClose) * 100 / startClose
    print(diff15)
    gd200Profit.append(diff15)
    
    step30Date = str(datetime.strptime(
        startDate, '%Y-%m-%d %H:%M:%S') - timedelta(minutes=-30))
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


symbol = "RADUSDT"

# Daten speichern in csv
df = pd.read_csv(symbol + "_gd.csv", decimal=",")

df = df.set_index("TimeCET")
df.index = pd.to_datetime(df.index)

minStep = [15,30,45,60]
gdProfit = GetChangePrice(True, df, minStep)

print("------------------------------------")
#print(df["close"])
print("------------------------------------")
#print(df["close"].shift(1)) # Daten speichern in csv
print("------------------------------------")
ilen = len(df)
#print(df.iat[ilen - 1, 1])
print("------------------------------------")

#gdProfit.insert(1, symbol)

#print(gdProfit)
#print(len(gdProfit))

#dfgd = pd.DataFrame([gdProfit], columns=['startDate', 'pairs', 'startClose', 'step15Date', 'step15Close', 'diff15', 'step30Date', 'step30Close', 'diff30', 'endDate', 'endClose', 'diffend'])

#print(dfgd)

#engine = connect_db_engine()

#dfgd.to_sql("binance_gd200", engine)
