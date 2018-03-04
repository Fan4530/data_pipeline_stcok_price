<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 004d9798008df5045b1eb47fe141014688d6d862
# write data to kafka cluster, topic
# schedule fetch p[rice from yahoo finance
# configurable stock symbole
import json
#
import time
import random
import argparse
import schedule 
import time
import logging
from kafka import KafkaProducer
from yahoo_finance import Share
#atexit can be used to register shudown_hook
import atexit
# initialize the logging
logging.basicConfig()
#  get one logger instance of data_producer
logger = logging.getLogger('data-producer') # logging package  ??? 
# debug info warn error fetal
logger.setLevel(logging.DEBUG)


symbole = ''
topic_name = ''
kafka_broker = ''
def shutdown_hook(producer):
	logger.info('closing kafka producer')
	#kafka will batch the data, it is possible that we let send, but the message has not been sent
	#so we need to flush these current message to database or other places
	producer.flush(10)
	producer.close(10)
	logger.info('kafka producer closed')


def fetch_price_and_send(producer):
	#fetch the stock data
	#logger of data-producer here 
	logger.debug('about to fetch price')
	#trade_time = stock.get_trade_datetime()
	trade_time = int(round(time.time() * 1000))#???
	price = random.randint(30, 120)
	data = {
		#can use python help(stock) get what API has of this instance
		'symbole': symbole,
		'last_trade_time': trade_time,
		'price': price
	} 
	#'' -> " "
	# python --> string ""
	data = json.dumps(data)
	#send data to kafka
	try:
		#API: producer -> kafka : topic, data
		producer.send(topic = topic_name, value = data)
		logger.debug('finish sending data to kafka %s', data)
	except Exception as e:
		#it is not reliabe in the network!! so use try except .. ???  learn it 
		logger.warn('fail to send price to kafka')


	logger.info('retrieve stock price %s', data)

"""
def fetch_price_and_send(producer, stock):
	#fetch the stock data
	#logger of data-producer here 
	logger.debug('about to fetch price')
	stock.refresh()
	price = stock.get_price()
	trade_time = stock.get_trade_datetime()
	data = {
		#can use python help(stock) get what API has of this instance
		'symbole': symbole,
		'last_trade_time': trade_time,
		'price': price
	} 
	#'' -> " "
	# python --> string ""
	data = json.dumps(data)
	#send data to kafka
	try:
		#API: producer -> kafka : topic, data
		producer.send(topic = topic_name, value = data)
		logger.debug('sent data to kafka %s', data)
	except Exception as e:
		#it is not reliabe in the network!! so use try except .. ???  learn it 
		logger.warn('fail to send price to kafka')


	logger.info('retrieve stock price %s', data)
"""

if __name__ == '__main__': # ???
	parser = argparse.ArgumentParser()
	parser.add_argument("symbole", help = 'the symbole of the stock')
	parser.add_argument("topic", help = 'the name of the topic')
	parser.add_argument("kafka_broker", help = 'the location of the kafka')
	args = parser.parse_args() # args = [symbole, topic, kafka_broker]

	symbole = args.symbole
	topic = args.topic
	kafka_broker = args.kafka_broker


	# kafka producer, read the document: kafka python

	producer = KafkaProducer (

		bootstrap_servers = kafka_broker # what ??? is the mearning 
	)

	#stock = Share(symbole) # yahoo finance, get the price according to the symbole


	# every 1 second, trigger fetch_price_and_send function, producer and stock are two parameters of it
	# endless loop
	schedule.every(1).second.do(fetch_price_and_send, producer)
	# close the producer before we run the the code
	atexit.register(shutdown_hook, producer)##??? what is the order

	while True:
		schedule.run_pending() # ??? 
<<<<<<< HEAD
		time.sleep(1) # time package   ????? 
=======
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


def fetch_price_and_sent(producer, symbol):
    logger.debug('about to fetch price')
    #stock.refesh()
    payload = {'symbols': symbol}

    r = requests.get('https://ws-api.iextrading.com/1.0/tops', params=payload)

    if(r.status_code == 200):
        pjson = r.json()
        price = pjson[0].get('lastSalePrice')
    #price = stock.get_price()
    #trade_time = stock.get_trade_datetime()

    #price = random.randint(80, 120)
    #trade_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  
    trade_time = int(round(time.time() * 1000))

    data = {
        'symbol': symbol,
        'last_trade_time': trade_time,
        'price': price
    }

    data = json.dumps(data)
    logger.info('retrieved stock price %s', data)


    try:
        producer.send(topic = topic_name, value = data)
        logger.debug('sent data to kafka %s', data)
    except Exception as e:
        logger.warn('failed to send price to kafka')
        logger.warn(e);


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
>>>>>>> a
=======
		time.sleep(1) # time package   ????? 
>>>>>>> 004d9798008df5045b1eb47fe141014688d6d862
