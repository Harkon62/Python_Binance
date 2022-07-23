# https://stackoverflow.com/questions/66295187/how-do-i-get-all-the-prices-history-with-binance-api-for-a-crypto-using-python

from datetime import timedelta
import pandas as pd
import talib.abstract as ta


# Function, um in Zeitintervallen die Veränderung des close-price darstellen
# df_pairsprice = Dataframe mit den OHLC Kursen des pairs
# pair          = Name des pairs
# stepCnt       = Anzahl der Zeitintervalle
# stepTime      = Zeit zwischen den Intervallen

# Rueckgabe:
# Dataframe mit den werten Zeitstempel, Close,Diff * Intervallen

def pairs_interval_change_quarter(df_pairsprice, pair, stepCnt, stepTime):
    
    print("Modul pairs_interval_change gestartet mit stepcnt=" + str(stepCnt) + " und stepTime=" + str(stepTime) + " ###########################################")

    # letztes Datum des pair ermitteln
    seriesdf = df_pairsprice.iloc[len(df_pairsprice) - 1]

    # Liste mit Datumswerten der Intervalle anlegen
    datelist = []
    
    # letztes Datum in Liste anfuegen 
    last_time_data = seriesdf.name
    datelist.append(last_time_data)
    
    # Liste mit TimeCET fuer Zeitinterval anfuegen 
    for x in range(1, stepCnt + 1):
        firstDate = last_time_data - timedelta(minutes=stepTime)    
        datelist.append(firstDate)
        last_time_data = firstDate

    # Reihenfolge der Liste umkehren
    datelist.reverse()
    # print(datelist)

    # Loop der  Datumsliste, fuellen der Datenliste und der Columnliste
    dataList = []
    columnList = []
    icnt = 1

    for dateDf in datelist:

        # Spaltennamen in Liste speichern'
        columnList.append("date" + str(icnt))
        columnList.append("close" + str(icnt))
        columnList.append("proz" + str(icnt))
        icnt = icnt + 1

        # Werte aus Dataframe in Liste speichern
        # Zeitstempel in Liste
        dataList.append(dateDf)
        # row mit Zeitstempel selektieren
        df_ser = df_pairsprice[df_pairsprice.index == dateDf]
        if not df_ser.empty:
            df_close = df_ser.iloc[0]["close"]
        else:
            df = pd.DataFrame()
            return df

        # close in Liste speichern
        dataList.append(df_close)
        dataList.append(0)

    columnList.append("prozges")
    dataList.append(0)

    # Prozentwerte Veraenderung in Datenliste aendern (Vorbelegung war mit 0)
    ipositiv = ""
    proz1 = dataList[4]
    proz2 = dataList[1]
    diff2 = (proz1- proz2) * 100 / proz1 
    dataList[5] = diff2
    if diff2 > 0:
        ipositiv = ipositiv + "1"
    else:
        ipositiv = ipositiv + "0"
    
    proz1 = dataList[7]
    proz2 = dataList[4]
    diff2 = (proz1 - proz2) * 100 / proz1 
    dataList[8] = diff2
    if diff2 > 0:
        ipositiv = ipositiv + "1"
    else:
        ipositiv = ipositiv + "0"

    proz1 = dataList[10]
    proz2 = dataList[7]
    diff2 = (proz1 - proz2) * 100 / proz1 
    dataList[11] = diff2
    if diff2 > 0:
        ipositiv = ipositiv + "1"
    else:
        ipositiv = ipositiv + "0"

    proz1 = dataList[13]
    proz2 = dataList[10]
    diff2 = (proz1 - proz2) * 100 / proz1 
    dataList[14] = diff2
    if diff2 > 0:
        ipositiv = ipositiv + "1"
    else:
        ipositiv = ipositiv + "0"


    # Profit vom 1. Date bis jetzt
    proz1 = dataList[13]
    proz2 = dataList[1]
    diff2 = (proz1 - proz2) * 100 / proz1 
    dataList[15] = diff2


    # print("dataList -------------------")
    dataList.insert(1, pair)
    del dataList[3]
    #print(dataList)

    # print("columnList -------------------")
    columnList.insert(1, "pairs")
    del columnList[3]
    #print(columnList)

    # Positiv-Merkmal der Intervalle in Liste speichern
    dataList.append(ipositiv)
    columnList.append("PosMerkmal")

    df = pd.DataFrame([dataList], columns=columnList)

    print("                            beendet  ###########################################")

    return df




def pairs_interval_change(df_pairsprice, pair, stepCnt, stepTime):
    
    print("Modul pairs_interval_chang gestartet  ###########################################")

    # Liste mit Minuten fuer Zeitinterval anlegen 
    minStep = []
    for x in range(1, stepCnt + 1):
        minStep.append(stepTime)
        
    # Funktion für Veränderung des Preises in Zeitintervallen holen
    listProfit = GetChangePrice(True, df_pairsprice, minStep)
    listProfit.insert(1, pair)

    # Spaltennamen für das Startdatum
    icnt = 1
    dbColumn = ["startDate", "pairs", "startClose"]

    # die Spalten für die Zeiten hinzufügen
    for lcnt in minStep:
        dbColumn.append("date" + str(icnt))
        dbColumn.append("close" + str(icnt))
        dbColumn.append("diff" + str(icnt))
        icnt = icnt + 1

    # Spaltennamen für das Enddatum hinzufügen
    dbColumn.append("endDate")
    dbColumn.append("endClose")
    dbColumn.append("enddiff")
    
    dbColumn.append("gesamtProfit")

    df = pd.DataFrame([listProfit], columns=dbColumn)

    print("                           beendet  ###########################################")

    return df



# Bestenliste der einzelnen Intervalle ermitteln
# Uebergabeparameter:
# tableName Tabelle, in die die Intervaldaten bereits gespeichert sind
# stepCnt Anzahl der Intervalle
# Rueckgabe:
# Dataframe mit Spalten fuer Ranglisten
def pairs_interval_rangliste(engine, tableName, stepCnt):

    print("Modul pairs_interval_rangliste gestartet ###########################################")

    # DB.binance_profit_quarter in DataFrame laden
    sqlstr = "SELECT * FROM " + tableName
    df_q = pd.read_sql_query(sqlstr, engine) 

    # print(df_q.head())
    
    for x in range(2,6):

        # nach der 1.proz.Veraenderung sortieren ---------------------------------
        # Spalte fuer Rangliste-Nr anlegen
        df_q["rangProz" + str(x)] = 0

        # Sortieren nach der Spalte Prozent
        df_q.sort_values(inplace=True, ascending=False,  by="proz" + str(x))

        # Schleife fuer Ranglistenstand setzen
        rowcnt = 1
        for index, row in df_q.iterrows():
            df_q.at[index,"rangProz" + str(x)] = rowcnt
            rowcnt += 1


    # nach der Gesamt.proz.Veraenderung sortieren ---------------------------------
    df_q["rangProzGes"] = 0
    df_q.sort_values(inplace=True, ascending=False,  by='prozges')

    rowcnt = 1
    for index, row in df_q.iterrows():

        # Name des pair aus der Zeile holen
        df_q.at[index,"rangProzGes"] = rowcnt
        rowcnt += 1

    # Sortierreihefolge der Spalten zuruecksetzen auf index
    df_q.sort_index(inplace=True)

    # specify the columns to sum
    cols = ['rangProz2', 'rangProz3', 'rangProz4', 'rangProz5']

    # sum of columns specified
    df_q["rangList"] = df_q[cols].sum(axis=1)

    df_q.sort_values(inplace=True,  by='rangList')

    print("                               beendet ###########################################")

    return df_q



def pairs_interval_positiv(engine):
    print("  Modul pairs_interval_positiv gestartet ###########################################")

    # DB sqlstr temp Tabelle erstellen mit den 3 Ranglistentabellen
    sqlstr = """
        SELECT p.pairs, s.date5 AS sdate5, q.proz2 AS qproz2, q.proz3 AS qproz3, q.proz4 AS qproz4, q.proz5 AS qproz5, 
        s.proz2 AS sproz2, s.proz3 AS sproz3, s.proz4 AS sproz4, s.proz5 AS sproz5,
        r.TimeCET AS rDate, r.rsi AS rRSI, r.rsima AS rRSIMA, r.buy AS rBuy,
        v.SUM_volume   
        FROM `binance_pairs` p 
        LEFT JOIN binance_profit_quarter q USING(pairs)
        LEFT JOIN binance_profit_std s USING(pairs)
        LEFT JOIN binance_rsima r USING(pairs)
        LEFT JOIN binance_volume v USING(pairs)
        WHERE v.SUM_volume > 300000
    """

    sqlstrNone = """
        SELECT p.pairs, q.proz2 AS qproz2, q.proz3 AS qproz3, q.proz4 AS qproz4, q.proz5 AS qproz5, 
        s.proz2 AS sproz2, s.proz3 AS sproz3, s.proz4 AS sproz4, s.proz5 AS sproz5,
        t.proz2 AS tproz2, t.proz3 AS tproz3, t.proz4 AS tproz4, t.proz5 AS tproz5,
        v.SUM_volume   
        FROM `binance_pairs` p 
        LEFT JOIN binance_profit_quarter q USING(pairs)
        LEFT JOIN binance_profit_std s USING(pairs)
        LEFT JOIN binance_profit_tgl t USING(pairs)
        LEFT JOIN binance_volume v USING(pairs)
        WHERE v.SUM_volume > 300000
    """

    # Abfrage ín Dataframe speichern
    df_sum = pd.read_sql_query(sqlstr, engine)

    # NA aus Dataframe loeschen
    df_sum.dropna()

    # Sind Datensaetze vorhanden
    idflen = len(df_sum)
    if(idflen > 0):
        tmpBool =  df_sum.empty
        print(tmpBool)
        if df_sum.empty:
            return "kein Datensatz"
         

    # Column-Namen aus Dataframe holen und nur die %-Spalten selektieren
    colListnames = df_sum.columns
    # alle Spaltennamen mit ?proz?Cnt
    colListnames = colListnames[2:10]
    # alle Spaltennamen mit qproz?Cnt
    colListnamesq = colListnames[0:4]

    # Spalte prozcnt mit dem Wert 0 vorbelegen
    pd.set_option('mode.chained_assignment', None)
    df_sum.loc[:, 'prozcnt'] = 0

    # Spalten durchlaufen, welche Proz-Veränderung positiv sind und Spalte ?prozCnt mit 1 setzen
    for colName in colListnames:
        df_sum.loc[:, colName + "Cnt"] = 0
        df_sum.loc[
            (
                (df_sum[colName] > 0)
            ),
            colName + "Cnt"] = 1

    # Spalten durchlaufen, welche Proz-Veränderung positiv sind und Zaehler in Spalte prozcnt hochsetzen
        df_sum.loc[
            (
                (df_sum[colName] > 0)
            ),
            'prozcnt'] = df_sum['prozcnt'] + 1


    # qproz?Cnt Werte zaehlen und in Spaltte speichern 
    df_sum['qprozsum'] = df_sum[colListnamesq + "Cnt"].sum(axis=1)


    # Dataframe in DB.binance_profit_positiv speichern
    df_sum.to_sql("binance_profit_positiv", engine, if_exists='replace')

    print("                                 beendet ###########################################")

    return "fertig" 



def GetChangePrice(isBack, df, minStep):
    print("  Modul GetChangePrice gestartet ###########################################")
    
    
    if isBack:
        # print("df kürzen")

        # Minuten in der Liste summieren
        minSum = 0
        for singlestep in minStep:
            minSum = minSum + singlestep
        
        # letztes Datum fuer Step ermitteln
        lastDate = df.index.max()

        # erstes Datum fuer Step ermitteln
        minSum = minStep[0] + minSum
        firstDate = lastDate - timedelta(minutes=minSum)

        # df mit dem eingegrenzten Zeitraum
        # df.to_csv("./csv/_test01.csv", decimal=",")
        df = df[(df.index >= firstDate)]
        # df.to_csv("./csv/_test02.csv", decimal=",")

    stepProfit = []
    gesamtProfit = 0
    

    # ersten Step in Liste speichern
    startDate = df.index.min()
    stepProfit.append(startDate)

    startClose = df.loc[startDate]['close']
    tmpClose = startClose
    stepProfit.append(startClose)

    # Steps zwischen Anfang und Ende in Liste speichern
    for singlestep in minStep:
        stepDate = startDate - timedelta(minutes=-singlestep)
        stepProfit.append(stepDate)

        stepClose = df.loc[stepDate]['close']
        stepProfit.append(stepClose)

        diffProz = (stepClose - startClose) * 100 / startClose
        stepProfit.append(diffProz)

        startDate = stepDate
        startClose = stepClose

    # letzten Step in Liste speichern
    endDate = df.index.max()

    stepProfit.append(endDate)

    endClose = df.loc[endDate]['close']
    stepProfit.append(endClose)

    diffend = (endClose - stepClose) * 100 / stepClose
    stepProfit.append(diffend)

    # Profit vom ersten Datum bis aktuell ermitteln und in Liste speichern
    gesamtProfit = (endClose - tmpClose) * 100 / tmpClose
    stepProfit.append(gesamtProfit)
    
    print("                       beendet ###########################################")

    return stepProfit
