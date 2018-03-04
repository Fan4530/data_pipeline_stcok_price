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


symbol = ''
topic_name = ''
kafka_broker = ''
def shutdown_hook(producer):
	logger.info('closing kafka producer')
	#kafka will batch the data, it is possible that we let send, but the message has not been sent
	#so we need to flush these current message to database or other places
	producer.flush(10)
	producer.close(10)
	logger.info('kafka producer closed')


def fetch_price_and_send(producer, symbol):
	#fetch the stock data
	#logger of data-producer here 
	logger.debug('about to fetch price')
	#trade_time = stock.get_trade_datetime()
	trade_time = int(round(time.time() * 1000))#???
	price = random.randint(30, 120)
	data = {
		#can use python help(stock) get what API has of this instance
		'symbol': symbol,
		'last_trade_time': trade_time,
		'price': price
	} 
	#'' -> " "
	# python --> string ""
	data = json.dumps(data)
	logger.info('retrieve stock price %s', data)
	#send data to kafka
	try:
		#API: producer -> kafka : topic, data
		producer.send(topic = topic_name, value = data)
		logger.debug('finish sending data to kafka %s', data)
	except Exception as e:
		#it is not reliabe in the network!! so use try except .. ???  learn it 
		logger.warn('fail to send price to kafka')


	

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

	parser.add_argument('symbol', help = 'the symbol of the stock')
	parser.add_argument('topic_name', help = 'the name of the topic')
	parser.add_argument('kafka_broker', help = 'the location of the kafka')
	args = parser.parse_args() # args = [symbole, topic, kafka_broker]

	symbol = args.symbol
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker


	# kafka producer, read the document: kafka python

	producer = KafkaProducer (

		bootstrap_servers = kafka_broker # what ??? is the mearning 
	)

	#stock = Share(symbole) # yahoo finance, get the price according to the symbole
	fetch_price_and_send(producer, symbol)

	# every 1 second, trigger fetch_price_and_send function, producer and stock are two parameters of it
	# endless loop
	schedule.every(1).second.do(fetch_price_and_send, producer, symbol)
	# close the producer before we run the the code
	atexit.register(shutdown_hook, producer)##??? what is the order

	while True:
		schedule.run_pending() # ??? 
		time.sleep(1) # time package   ????? 