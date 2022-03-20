import datetime
import time
import json
import pyspark
import schedule
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer

# Importing Local Modules
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from config import *
from streaming_layer.consumer_handlers import handle_eth_hourly
from batch_layer.batch_processor import batch_eth_hourly

spark_main = SparkSession \
             .builder \
             .appName('Trading Bot Pipeline') \
             .getOrCreate()
            # .master("local") \

consumer = KafkaConsumer(
    KAFKA_TOPICS['eth-hourly'],
    bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
    auto_offset_reset='latest' # 'latest' or 'earliest'
)

print("Start -----------------")

if __name__ == "__main__":

    if spark_main:
        print("Successfully initialized Spark Session")
        spark_main.sparkContext.setLogLevel("FATAL")

    for message in consumer:
        message_dict = json.loads(message.value.decode('UTF-8'))
        print(message_dict)
        if message_dict['format'] == 'hour':
            handle_eth_hourly(message_dict)
        elif message_dict['format'] == 'train':
            pass

    '''
    df = spark_main \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_PORT}") \
        .option("subscribe", KAFKA_TOPICS['eth-hourly']) \
        .option("startingOffsets", "earliest") \
        .load()
    print(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"))
    '''
