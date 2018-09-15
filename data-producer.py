# write data to any kafka cluster
# write data to any kafka topic
# scheduled fetch price from yahoo finance
# configurable stock symbol

# parse command line argument
import argparse
import schedule
import time
import logging
import json
import random
import datetime
import requests

# atexit can be used to register shutdown_hook
import atexit

from kafka import KafkaProducer

# here we capture stock price from alpha_vantage from alpha_vantage.timeseries import TimeSeries

# from yahoo_finance import Share

logging.basicConfig()
logger = logging.getLogger('data-producer')

# debug, info, warn, error, fatal
logger.setLevel(logging.DEBUG)

symbol = ''
topic_name = ''
kafka_broker = ''


def shutdown_hook(producer):
    logger.info('closing kafka producer')
    producer.flush(10)
    producer.close(10)
    logger.info('kafka producer closed!!')


def fetch_price_and_sent(producer, symbol):
    logger.debug('about to fetch price')
    # stock.refesh()
    payload = {'symbols': symbol}
    r = requests.get('https://ws-api.iextrading.com/1.0/tops', params=payload)

    if (r.status_code == 200):
        pjson = r.json()
        price = pjson[0].get('lastSalePrice')
    # price = stock.get_price()
    # trade_time = stock.get_trade_datetime()

    # price = random.randint(80, 120)
    trade_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # trade_time = int(round(time.time() * 1000))

    print(trade_time)
    print(price)

    # data = {
    #     'symbol': symbol,
    #     'last_trade_time': trade_time,
    #     'price': price
    # }
    # data = json.dumps(data)
    # logger.info('retrieved stock price %s', data)
    #
    # try:
    #     producer.send(topic=topic_name, value=data)
    #     logger.debug('sent data to kafka %s', data)
    # except Exception as e:
    #     logger.warn('failed to send price to kafka')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help=' the symbol of the stock')
    parser.add_argument('topic_name', help='the name of the topic')
    # parser.add_argument('kafka_broker', help='the location of the kafka')

    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    # kafka_broker = args.kafka_broker

    # producer = KafkaProducer(
    #     # if we have kafka cluster with 1000 nodes, what do we pass to kafka broker
    #     bootstrap_servers=kafka_broker
    # )

    # stock = Share(symbol)

    # Get json object with the intraday data and another with  the call's metadata

    fetch_price_and_sent("", symbol)

    # schedule.every(1).seconds.do(fetch_price_and_sent, producer, symbol)
    # atexit.register(shutdown_hook, producer)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)