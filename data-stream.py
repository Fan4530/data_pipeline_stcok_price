#read from kafka
#write to any kafka
#perform average on stack every 5 seconds
import argparse
import logging
import json
import time

#env conf
from pyspark import SparkContext
#???
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from kafka import KafkaProducer


from kafka import Kafkaproducer

import atexit

logger.setLevel(logging.INFO)


kafka_producer = None
target_topic = None
topic_name = None
kafka_broker = None
def shutdown_hook(producer):
	producer.flush(10)
	producer.close(10)
	logger.info('resoure released')
def process_stream(stream):
	# average based on different stock symbol
	#AAPL, AMZN

	#write to kafka topic

	def send_to_kafka(rdd):
		results = rdd.collect()
		for r in results:
			data = json.dumps(
				{
					'symbol':r[0],
					'average': r[1],
					'timestamps': time.time()
				}
			)
			logger.infor(data)
			kafka_producer.send(target_topic, value = data)
	def preprocess(data):
		record = json.loads(data[1].decode('utf-8'))
		#return symbole -- what type of the symbol.
		#pair(price of the symbol, count of the price)
		return record.get('symbol'),(float(record.get('price')), 1)

	stream.map(preprocess).reduceByKey(lambda a, b:  (b[0] + a[0], a[1] + b[1]).map(lambda (k, v): (k, v[0]/v[1])).forachRDD(send_to_kafka)


	stream.map(preprocess)
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('target_topic', help = ' the new topic need to be return')
    parser.add_argument('topic_name', help = 'the name of the topic')
    parser.add_argument('kafka_broker', help = 'the location of the kafka')

    args = parser.parse_args()
    target_topic = args.target_topic
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    # config the spark environment
    # local , 2 threads 
    # if use mesos in distributed system, we don't need it
    sc = SparkContext('local[2]', 'bigdata')
    sc.setLogLevel('WARN')
    # every 5 seconds, send the RDD to spark 
    ssc = StreamingContext(sc, 5)


    #direct stream 
    directKafkaStream = KafkaUnits.createDirectStream(ssr, [topic_name], {'metadata.broker.list': kafka_broker})
    
    process_stream(directKafkaStream)

    # create kafka producer 
    kafka_producer = KfkaProducer (
    	bootstrap_servers = kafka_broker
    )


    #?
    ssc.start()
    ssc.awaitTermination()