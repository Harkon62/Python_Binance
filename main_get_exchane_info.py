# https://stackoverflow.com/questions/67439379/how-can-i-get-all-coins-in-usd-parity-to-the-binance-api/69423323#69423323
import requests
import pandas as pd
from modul_db import connect_db_engine



# ----------------------------------------------------------------------------------------------------------------------
# Pairs von der Exchange herunterladen und in DB.binance_pairs_exchange speichern
#
# in der Python Datei main_pairs_activ.py werden die Pairs in der DB.binance_pairs mit der DB.binance_pairs_exchange abgeglichen



# Aufruf der Exchange, Uebergabe der Url mit Endpoint
def get_exchange_info():
    base_url = 'https://api.binance.com'
    endpoint = '/api/v3/exchangeInfo'
    
    return get_response(base_url + endpoint)


# Pruefen, ob Daten von der Exchange geliefert wurden
# Rueckgabe als json
def get_response(url):
    response = requests.get(url)
    response.raise_for_status()  # raises exception when not a 2xx response
    if response.status_code != 204:
        return response.json()
    
 # json filtern, nur pairs mit BaseCurrency = USDT   
def create_symbols_list(filter='USDT'):
    info = get_exchange_info()  
      
    pairs_data = info['symbols']
    full_data_dic = {s['symbol']: s for s in pairs_data if filter in s['symbol']}
    
    return full_data_dic.keys()

# ------------------------------------------------------------------------------------    
if __name__ == '__main__': 
    
    # pairs von Binance mit Filter (USDT) holen
    # Rückgabe als Dictionary
    pairs = create_symbols_list('USDT')

    print(type(pairs))

    # Umwandlung von Dictionary nach Liste
    # keys = list(pairs)

    lskey = []
    lsPri = []
    ls_black = ["BULLUSDT", "BEARUSDT", "DOWNUSDT", "UPUSDT", "USDC", "ICPUSDT","PERP", "USSDCUSDT", "USDPUSDT", "USDSBUSDT", "USDSUSDT"]

    for key in pairs:
        print(key)
        # Durchlauf der Blacklist ls_black, gefunden = ""
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
