#1: read from kafka
#2: write to cassandra

import argparse
import logging

import json
#cassandra.cluster.cluster
from cassandra.cluster import Cluster
#kafka.laflaConsumer
from kafka import KafkaConsumer 


topic_name = ''
kafka_broker = ''
cassandra_broker = ''
keyspace = ''
table = ''

# set longgings 
logging.basicConfig()
logger = logging.getLogger('data_storage')
logger.setLevel(logging.DEBUG);

#save data to the cassandra
#stock_data: get from kafka, one line 
def sava_data(stock_data, session):
	try:
		logger.debug('start to save data %s', stock_data)
		parsed = json.loads(stock_data) #??
		symbol = parsed.get('symbol')
		price = parsed.get('price')
		timestamp = parsed.get('last_trade_time')

		statement = "INSERT INTO %s (symbol, trade_time, price) VALUES ('%s', '%s', %f)" % (table, symbol, timestamp, price)
		session.execute(statement)
		logger.info('finish saving data into cassandra %s', stock_data)
	except Exception as e:
		logger.error('cannot save data %s', stock_data)


# keyspace 
if __name__ == '__main__':#???
	parser = argparse.ArgumentParser();
	parser.add_argument('topic_name', help = 'the kafka topic topic name to subscribe from')
	parser.add_argument('kafka_broker', help = 'the kafka broker address')
	parser.add_argument('cassandra_broker', help = 'the cassandra broker location')
	parser.add_argument('keyspace', help = 'the keyspace')#it like a database in mongodb
	parser.add_argument('table', help = 'the table in cassandra')

	#parse agument initialize 
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	cassandra_broker = args.kafka_broker
	keyspace = args.keyspace
	table = args.table


	#create kafka consumer 
	consumer = KafkaConsumer (#???  data structure python???
		topic_name,
		bootstrap_servers = kafka_broker
	)

	#create a cassandra session
	cassandra_cluster = Cluster (
		contact_points = cassandra_broker.split(',')
	)
	session = cassandra_cluster.connect()
	#user pass the argument: keyspace and table here
	#if the keyspace and table are not existed, create new ones  
	session.excute("CREATE KEYSPACE IF NOT EXIST %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}" % keyspace)
	session.set_keyspace(keyspace)
	session.execute("CREATE TABLE IF NOT EXISTS %s (symbol text, trade_time timestamp, price float, PRIMARY KEY(symbol, trade_time))" % table)
	#get msg from kafka 
	for msg in consumer:
		#logger.debug(msg)
		save_data(msg, session)