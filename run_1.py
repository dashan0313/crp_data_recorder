from base import mkdir
import datetime
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import requests
import json
from get_from_web import BinanceRecorder
from loguru import logger
logger.add('run.log', rotation='1 week', retention='10 days')
logger.info('Start')

# proxies = {
#     'http': 'http://127.0.0.1:10887',
#     'https': 'http://127.0.0.1:10887'
# }
base_url = 'https://fapi.binance.com'
require_url = {'openInterestHist': '/futures/data/openInterestHist',
               # "sumOpenInterest": "20403.12345678", // 持仓总数量
               # "sumOpenInterestValue": "176196512.12345678", // 持仓总价值
               'topLongShortAccountRatio': '/futures/data/topLongShortAccountRatio',
               # "longShortRatio":"1.8105",// 大户多空账户数比值
               # "longAccount": "0.6442", // 大户多仓账户数比例
               # "shortAccount":"0.3558", // 大户空仓账户数比例
               'topLongShortPositionRatio': '/futures/data/topLongShortPositionRatio',
               # "longShortRatio":"1.4342",// 大户多空持仓量比值
               # "longAccount": "0.5344", // 大户多仓持仓量比例
               # "shortAccount":"0.4238", // 大户空仓持仓量比例
               'globalLongShortAccountRatio': '/futures/data/globalLongShortAccountRatio',
               # "longShortRatio": "0.1960", // 多空人数比值
               # "longAccount": "0.6622", // 多仓人数比例
               # "shortAccount": "0.3378", // 空仓人数比例
               'takerlongshortRatio': '/futures/data/takerlongshortRatio',
               # "buySellRatio": "1.5586",
               # "buyVol": "387.3300", // 主动买入量
               # "sellVol": "248.5030", // 主动卖出量
               }

recorder = BinanceRecorder(None, base_url, require_url)

period = '1h'
save_path = 'data'
symbol_list = ['BTCUSDT', 'ETHUSDT']


@logger.catch
def push_to_feishu(symbol, shape, st, et):
    webhook_add = 'https://open.feishu.cn/open-apis/bot/v2/hook/9f9dad33-8e9b-4cbb-a870-5572b3550211'
    payload_message = {
        "msg_type": "text",
        "content": {
            "text": f"成功记录{symbol}的昨日数据! \nshape:{shape}, \nstartTime:{st}, \nendTime:{et}"
        }
    }
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", webhook_add, headers=headers, data=json.dumps(payload_message))
    logger.info("feishu response :" + response.text)


@logger.catch
def init():
    endTime = datetime.date.today()
    endTime = int(time.mktime(endTime.timetuple())) * 1000
    for symbol in symbol_list:
        mkdir(f"{save_path}/{symbol}")
        shape, st, et = recorder.init_get_and_save(symbol=symbol, endTime=endTime, save_path=save_path, limit=450,
                                                   period=period)
        push_to_feishu(symbol, shape=shape, st=st, et=et)


@logger.catch
def main():
    _startTime = datetime.date.today() + datetime.timedelta(days=-1)
    startTime = int(time.mktime(_startTime.timetuple())) * 1000  # 本地时间
    _endTime = datetime.date.today()
    endTime = int(time.mktime(_endTime.timetuple())) * 1000  # 本地时间
    logger.debug(f'get time start: {_startTime}, end: {_endTime}')
    for symbol in symbol_list:
        logger.debug('get and save data')
        shape, st, et = recorder.get_and_save(symbol=symbol, startTime=startTime, endTime=endTime, save_path=save_path,
                                              period=period)
        logger.debug('push to feishu')
        push_to_feishu(symbol, shape=shape, st=st, et=et)


if __name__ == '__main__':
    init()
    logger.debug('build scheduler')
    scheduler = BlockingScheduler()
    logger.debug('scheduler add and run job')
    scheduler.add_job(func=main, trigger='cron', hour='9')
    scheduler.start()
