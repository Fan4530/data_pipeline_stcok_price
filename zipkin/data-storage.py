#1: read from kafka
#2: write to cassandra

import argparse
import logging
import atexit
import json
import requests

#cassandra.cluster.cluster
from cassandra.cluster import Cluster
#kafka.laflaConsumer
from kafka import KafkaConsumer 
from py_zipkin.zipkin import ZipkinAttrs
from py_zipkin.zipkin import zipkin_span
from py_zipkin.thread_local import get_zipkin_attrs
from py_zipkin.util import generate_random_64bit_string

topic_name = ''
kafka_broker = ''
cassandra_broker = ''
keyspace = ''
table = ''
#
def shutdown_hook(consumer, session):
	logger.info('closing resource')
	consumer.close()
	session.shutdown()
	logger.info('release resource')
# set longgings 
logging.basicConfig()
logger = logging.getLogger('data_storage')
logger.setLevel(logging.DEBUG);

def construct_zipkin_attrs(data):
    parsed = json.loads(data)
    return ZipkinAttrs(
        trace_id=parsed.get('trace_id'),
        span_id= generate_random_64bit_string(),
        parent_span_id=parsed.get('parent_span_id'),
        is_sampled=parsed.get('is_sampled'),
        flags='0',
    )

#save data to the cassandra
#stock_data: get from kafka, one line 
def http_transport_handler(span):
    body = '\x0c\x00\x00\x00\x01' + span
    requests.post(
        'http://localhost:9411/api/v1/spans',
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
    )
def save_data(stock_data, session):
	zipkin_attrs = construct_zipkin_attrs(stock_data)
	with zipkin_span(service_name = 'data-storage', span_name = 'save_data', transport_handler = http_transport_handler, zipkin_attrs=  zipkin_attrs):

#	with zipkin_span(service_name = 'data-storage', span_name = 'save_data', transport_handler = http_transport_handler, sample_rate = 100.0):
		parsed = json.loads(stock_data)
		try:
			logger.debug('start to save data %s', stock_data)
			parsed = json.loads(stock_data)
			# parsed = json.loads(stock_data) #??


			symbol = parsed.get('symbol')

			price = float(parsed.get('price'))
			timestamp = parsed.get('last_trade_time')

			statement = "INSERT INTO %s (symbol, trade_time, price) VALUES ('%s','%s',%f)" %(table, symbol,timestamp,price)
			session.execute(statement)
			logger.info('saved data into cassandra111 %s', stock_data)
		except Exception as e:
			logger.error('cannot save data %s', stock_data)
			# print stock_data
			# logger.error(e)



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
	cassandra_broker = args.cassandra_broker
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
	session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}" % keyspace)
	session.set_keyspace(keyspace)
	session.execute("CREATE TABLE IF NOT EXISTS %s (symbol text, trade_time timestamp, price float, PRIMARY KEY(symbol, trade_time))" % table)

	atexit.register(shutdown_hook, consumer, session)

	for msg in consumer:
		# logger.debug(msg)
		save_data(msg.value, session)


