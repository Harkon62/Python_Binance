# https://stackoverflow.com/questions/67439379/how-can-i-get-all-coins-in-usd-parity-to-the-binance-api/69423323#69423323
import requests
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Float, MetaData
from modul_db import connect_db_engine



def get_response(url):
    response = requests.get(url)
    response.raise_for_status()  # raises exception when not a 2xx response
    if response.status_code != 204:
        return response.json()

def get_exchange_info():
    base_url = 'https://api.binance.com'
    endpoint = '/api/v3/exchangeInfo'
    
    return get_response(base_url + endpoint)

def create_symbols_list(filter='USDT'):
    rows = []
    info = get_exchange_info()
    pairs_data = info['symbols']
    full_data_dic = {s['symbol']: s for s in pairs_data if filter in s['symbol']}
    
    return full_data_dic.keys()



# ----------------------------------------------------------------------------------------------------------------------    
if __name__ == '__main__':  # Execute the following code only when executing main.py (not when importing it)
    # pairs von Binance mit Filter (USDT) holen
    # Rückgabe als Dictionary
    pairs = create_symbols_list('USDT')

    print(type(pairs))

    # Umwandlung von Dictionary nach Liste
    keys = list(pairs)

    lskey = []
    lsPri = []
    ls_black = ["BULLUSDT", "BEARUSDT", "DOWNUSDT", "UPUSDT", "USDC", "ICPUSDT","PERP", "USSDCUSDT", "USDPUSDT", "USDSBUSDT", "USDSUSDT"]

    for key in pairs:
        print(key)
        for nopairs in ls_black:
            if nopairs in key:
                key = ""
        
        if len(key) > 0 and key.endswith("USDT"):
            lskey.append(key)
            lsPri.append(0)
        
    lsdic = {
        "pairs" : lskey,
        "Prioritaet" : lsPri
    }    

    df = pd.DataFrame(lsdic)

    # Verbindung mit Datenbank aufbauen
    engine = connect_db_engine()

    df.to_sql("binance_pairs_exchange", engine, if_exists='replace')