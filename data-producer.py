# write data to kafka cluster, topic
# schedule fetch p[rice from yahoo finance
# configurable stock symbole

import argparse
import schedule 
import time
import logging
from kafka import KafkaProducer
from yahoo_finance import Share

# initialize the logging
logging.basicConfig()
#  get one logger instance of data_producer
logger = logging.getLogger('data-producer') # logging package  ??? 
# debug info warn error fetal
logger.setLevel(logging.DEBUG)

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
	logger.info('retrieve stock price %s', data)


if __name__ == '__main__': # ???
	parser = argparse.ArgumentParser()
	parser.add_argument("symbole", help = 'the symbole of the stock')
	parser.add_argument("topic", help = 'the name of the topic')
	parser.add_argument("kafka_broker", help = 'the location of the kafka')
	args = parser.parse_args() # args = [symbole, topic, kafka_broker]

	symbole = args.symbole
	topic = args.topic
	kafka_broker = args.kafka_broker

	print args;

	# kafka producer, read the document: kafka python

	producer = KafkaProducer (

		bootstrap_servers = kafka_broker # what ??? is the mearning 
	)

	stock = Share(symbole) # yahoo finance, get the price according to the symbole


	# every 1 second, trigger fetch_price_and_send function, producer and stock are two parameters of it
	# endless loop
	schedule.every(1).second.do(fetch_price_and_send, producer, stock)

	while True:
		schedule.run_pending() # ??? 
		time.sleep(1) # time package   ????? 