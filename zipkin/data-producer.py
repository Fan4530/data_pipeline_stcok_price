import argparse
import schedule
import time
import logging
import json
import random
import datetime
import requests

from py_zipkin.zipkin import zipkin_span
from py_zipkin.thread_local import get_zipkin_attrs
from py_zipkin.util import generate_random_64bit_string
# atexit can be used to register shutdown_hook
import atexit

from kafka import KafkaProducer

# here we capture stock price from alpha_vantage
from alpha_vantage.timeseries import TimeSeries
#from yahoo_finance import Share

logging.basicConfig()
logger = logging.getLogger('data-producer')

#debug, info, warn, error, fatal
logger.setLevel(logging.DEBUG)

symbol = ''
topic_name = ''
kafka_broker = ''

def shutdown_hook(producer):
	logger.info('closing kafka producer')
	producer.flush(10)
	producer.close(10)
	logger.info('kafka producer closed!!')

#this function shoud before dumps json
def enrich_with_zipkin_data(data):
    # enrich == add key - value pair for data dict
    # the following data will be sent to kafka, the next span
    # so the kafka is the parent span of the next span
    zipkin_attr = get_zipkin_attrs()
    data['trace_id'] = zipkin_attr.trace_id
    data['parent_span_id'] = zipkin_attr.parent_span_id
    data['is_sampled'] = True if zipkin_attr.is_sampled else False
    return data;

def http_transport_handler(span):
    body = '\x0c\x00\x00\x00\x01' + span
    requests.post(
        'http://localhost:9411/api/v1/spans',
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
    )

@zipkin_span(service_name = 'data-producer', span_name = 'fetch_price')
def fetch_price(symbole):
    logger.debug('about to fetch price')
    #price
    # payload = {'symbols': symbol}
    # r = requests.get('https://ws-api.iextrading.com/1.0/tops', params=payload)
    # if(r.status_code == 200):
    #     pjson = r.json()
    #     price = pjson[0].get('lastSalePrice')
    price = 1
    #timestamp
    trade_time = int(round(time.time() * 1000))

    # data obj
    data = {
        'symbol': symbol,
        'last_trade_time': trade_time,
        'price': price
    }
    data = enrich_with_zipkin_data(data)
    data = json.dumps(data)
    logger.info('get stock price %s', data)
    return data

@zipkin_span(service_name = 'data-producer', span_name = 'send_to_kafka')
def send_to_kafka(producer, data):
    try:
        
        producer.send(topic = topic_name, value = data)
        logger.debug('sent data to kafka %s', data)
    except Exception as e:
        logger.warn('failed to send price to kafka')
        print e



def fetch_price_and_sent(producer, symbol):
    with zipkin_span(service_name = 'data-producer', span_name = 'fetch_price_and_sent', transport_handler = http_transport_handler, sample_rate = 100.0):
        data = fetch_price(symbol)
        send_to_kafka(producer, data)




if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol', help = ' the symbol of the stock')
	parser.add_argument('topic_name', help = 'the name of the topic')
	parser.add_argument('kafka_broker', help = 'the location of the kafka')

	args = parser.parse_args()
	symbol = args.symbol
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker

    

	producer = KafkaProducer(
        # if we have kafka cluster with 1000 nodes, what do we pass to kafka broker
        bootstrap_servers = kafka_broker
	)

	#stock = Share(symbol)

	# Get json object with the intraday data and another with  the call's metadata

	fetch_price_and_sent(producer, symbol)

	schedule.every(1).seconds.do(fetch_price_and_sent, producer, symbol)
	atexit.register(shutdown_hook,producer)

	while True:
		schedule.run_pending()
		time.sleep(1)