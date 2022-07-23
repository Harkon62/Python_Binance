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
        
    stepProfit =  []
    
    startDate = df.index.min()
    print(startDate)
    stepProfit.append(startDate)
    
    startClose = df.loc[startDate]['close']
    print(startClose)
    stepProfit.append(startClose)
    
    # Minuten in der Liste zählen
    for singlestep in minstep:
        stepDate = startDate - timedelta(minutes=-singlestep)
        print(stepDate)
        stepProfit.append(stepDate)
    
        stepClose = df.loc[stepDate]['close']
        print(stepClose)
        stepProfit.append(stepClose)
        
        diffProz = (stepClose - startClose) * 100 / startClose
        print(diffProz)
        stepProfit.append(diffProz)
    
    endDate = df.index.max()
    print(endDate)
    stepProfit.append(endDate)

    endClose = df.loc[endDate]['close']
    print(endClose)
    stepProfit.append(endClose)

    diffend = (endClose - startClose) * 100 / startClose
    print(diffend)
    stepProfit.append(diffend)
    
    return stepProfit


symbol = "RADUSDT"

# Daten speichern in csv
df = pd.read_csv(symbol + "_gd.csv", decimal=",")

df = df.set_index("TimeCET")
df.index = pd.to_datetime(df.index)

minStep = [15,30,45,60]
gdProfit = GetChangePrice(True, df, minStep)

gdProfit.insert(1, symbol)

icnt = 1
dbColumn = ["startDate", "pairs", "startClose"]

for lcnt in minStep:
    dbColumn.append("date" + str(icnt))
    dbColumn.append("close" + str(icnt))
    dbColumn.append("diff" + str(icnt))
    icnt = icnt + 1
    
dbColumn.append("endDate")
dbColumn.append("endClose")
dbColumn.append("enddiff")

dfgd = pd.DataFrame([gdProfit], columns=dbColumn)
#, columns=['startDate', 'pairs', 'startClose', 'step15Date', 'step15Close', 'diff15', 'step30Date', 'step30Close', 'diff30', 'endDate', 'endClose', 'diffend'])

print(dfgd)

engine = connect_db_engine()

dfgd.to_sql("binance_profit", engine)
