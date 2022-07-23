
    def _generate_signature(self, data: typing.Dict) -> str:
    
        """
        Generate a signature with the HMAC-256 algorithm.
        :param data: Dictionary of parameters to be converted to a query string
        :return:
        """

        return hmac.new(self._secret_key.encode(), urlencode(data).encode(), hashlib.sha256).hexdigest()
    
    def _make_request(self, method: str, endpoint: str, data: typing.Dict):
    
        """
        Wrapper that normalizes the requests to the REST API and error handling.
        :param method: GET, POST, DELETE
        :param endpoint: Includes the /api/v1 part
        :param data: Parameters of the request
        :return:
        """

        if method == "GET":
            try:
                response = requests.get(self._base_url + endpoint, params=data, headers=self._headers)
            except Exception as e:  # Takes into account any possible error, most likely network errors
                logger.error("Connection error while making %s request to %s: %s", method, endpoint, e)
                return None
            
    def get_historical_candles(self, contract: Contract, interval: str) -> typing.List[Candle]:
    
        """
        Get a list of the most recent candlesticks for a given symbol/contract and interval.
        :param contract:
        :param interval: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
        :return:
        """

        data = dict()
        data['symbol'] = contract.symbol
        data['interval'] = interval
        data['limit'] = 1000  # The maximum number of candles is 1000 on Binance Spot

        if self.futures:
            raw_candles = self._make_request("GET", "/fapi/v1/klines", data)
        else:
            raw_candles = self._make_request("GET", "/api/v3/klines", data)

        candles = []

        if raw_candles is not None:
            for c in raw_candles:
                candles.append(Candle(c, interval, self.platform))

        return candles
    
    

public_key = public_key
secret_key = secret_key
       
headers = {'X-MBX-APIKEY': public_key}
       
base_url = "https://api.binance.com"
wss_url = "wss://stream.binance.com:9443/ws"

