from tradingview_ta import TA_Handler, Interval


def tahandler(sml):
    output = TA_Handler(symbol=sml,
                    screener='Crypto',
                    exchange='Binance',
                    interval=Interval.INTERVAL_1_HOUR)
                    #interval=Interval.INTERVAL_1_MINUTE)
    return output

# Signale der Indicatoren Buy/Sell
#print(output.get_analysis().summary)

# Ausgabe der Indikatoren
#print(output.get_analysis().indicators)

symbols = ['BTCUSDT', 'BNBUSDT', 'XECUSDT',
             'MATICUSDT', 'SOLUSDT', 'EOSUSDT', 
             'BCHUSDT', 'ONEUSDT', 'GRTUSDT', 'AVAXUSDT']

for symbol in symbols:
    data = tahandler(symbol)

    print('Symbol: ' + symbol)
    print(data.get_analysis().summary)
