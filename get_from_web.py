import requests
import pandas as pd
import datetime
from datetime import timedelta, timezone
import time


class BinanceRecorder:
    def __init__(self, proxies, base_url, require_url):
        self.proxies = proxies
        self.base_url = base_url
        self.require_url = require_url
        self.tz = timezone(timedelta(hours=8))

    def _get_data(self, query):
        result = pd.DataFrame()
        for key in self.require_url.keys():
            url = self.base_url + self.require_url[key] + query
            r = requests.get(url, proxies=self.proxies).json()
            r_tmp = pd.DataFrame(r)
            if 'symbol' in r_tmp.columns:
                r_tmp = r_tmp.drop('symbol', axis=1)
            r_tmp.columns = [f"{key}_{name}" if name != 'timestamp' else name for name in r_tmp.columns]
            if key == list(self.require_url.keys())[0]:
                result = r_tmp
            else:
                result = pd.merge(result, r_tmp, on=['timestamp'])
        return result

    def get_and_save(self, symbol, startTime, endTime, save_path, period='1h'):
        query = f'?symbol={symbol}&period={period}&startTime={startTime}&endTime={endTime}'
        result = self._get_data(query)
        save_date = datetime.datetime.utcfromtimestamp(endTime / 1000).replace(tzinfo=self.tz).strftime('%Y-%m-%d')
        result.to_csv(f"{save_path}/{symbol}/{save_date}.csv", index=False)
        st = (datetime.datetime.utcfromtimestamp(result.timestamp.iloc[0] / 1000) + timedelta(hours=8)).replace(
            tzinfo=self.tz).strftime('%Y-%m-%d %H:%M:%S')
        et = (datetime.datetime.utcfromtimestamp(result.timestamp.iloc[-1] / 1000) + timedelta(hours=8)).replace(
            tzinfo=self.tz).strftime('%Y-%m-%d %H:%M:%S')
        return result.shape, st, et

    def init_get_and_save(self, symbol, limit, endTime, save_path, period='1h'):
        query = f'?symbol={symbol}&period={period}&limit={limit}&endTime={endTime}'
        result = self._get_data(query)
        save_date = datetime.datetime.utcfromtimestamp(endTime / 1000).replace(tzinfo=self.tz).strftime('%Y-%m-%d')
        result.to_csv(f"{save_path}/{symbol}/{save_date}.csv", index=False)
        st = (datetime.datetime.utcfromtimestamp(result.timestamp.iloc[0] / 1000) + timedelta(hours=8)).replace(
            tzinfo=self.tz).strftime('%Y-%m-%d %H:%M:%S')
        et = (datetime.datetime.utcfromtimestamp(result.timestamp.iloc[-1] / 1000) + timedelta(hours=8)).replace(
            tzinfo=self.tz).strftime('%Y-%m-%d %H:%M:%S')
        return result.shape, st, et


if __name__ == '__main__':
    proxies = {
        'http': 'http://127.0.0.1:10887',
        'https': 'http://127.0.0.1:10887'
    }
    base_url = 'https://fapi.binance.com'
    require_url = {'openInterestHist': '/futures/data/openInterestHist',
                   # "sumOpenInterest": "20403.12345678", // ???????????????
                   # "sumOpenInterestValue": "176196512.12345678", // ???????????????
                   'topLongShortAccountRatio': '/futures/data/topLongShortAccountRatio',
                   # "longShortRatio":"1.8105",// ???????????????????????????
                   # "longAccount": "0.6442", // ???????????????????????????
                   # "shortAccount":"0.3558", // ???????????????????????????
                   'topLongShortPositionRatio': '/futures/data/topLongShortPositionRatio',
                   # "longShortRatio":"1.4342",// ???????????????????????????
                   # "longAccount": "0.5344", // ???????????????????????????
                   # "shortAccount":"0.4238", // ???????????????????????????
                   'globalLongShortAccountRatio': '/futures/data/globalLongShortAccountRatio',
                   # "longShortRatio": "0.1960", // ??????????????????
                   # "longAccount": "0.6622", // ??????????????????
                   # "shortAccount": "0.3378", // ??????????????????
                   'takerlongshortRatio': '/futures/data/takerlongshortRatio',
                   # "buySellRatio": "1.5586",
                   # "buyVol": "387.3300", // ???????????????
                   # "sellVol": "248.5030", // ???????????????
                   }
    symbol = 'BTCUSDT'
    period = '1h'
    limit = 450
    endTime = datetime.date.today()
    endTime = int(time.mktime(endTime.timetuple())) * 1000
    save_path = 'data'
    recorder = BinanceRecorder(proxies, base_url, require_url)
    recorder.init_get_and_save(symbol=symbol, endTime=endTime, save_path=save_path, limit=limit, period='1h')
