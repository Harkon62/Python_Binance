from binance_connector import BinanceClient



if __name__ == '__main__':

binance = BinanceClient("bf2e5bd00429bc9bfbb010ee9f37ddadf2dd5d20cf48a1b30fba1ff61de2a44d",
                           "13d4456c0ee39ba5cc801418489df2184e068fb5d8090b8dd8e64ee6dffebd97",
                           testnet=True, futures=True)

