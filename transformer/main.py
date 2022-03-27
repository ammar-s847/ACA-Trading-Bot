import datetime
import time
import json
import pyspark
import schedule
from datetime import datetime
from multiprocessing import Process
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
from streaming_layer.consumer_handlers import handle_eth_hourly, handle_eth_hourly_train_data
from batch_layer.batch_processor import batch_eth_hourly, gather_local_cached_data
from model.model_train import train_new_seq_LSTM_model, train_new_bi_LSTM_model

spark_main = SparkSession \
             .builder \
             .appName('Trading Bot Pipeline') \
             .getOrCreate()
            # .master("local") \

consumer = KafkaConsumer(
    KAFKA_TOPICS['eth-hourly'],
    bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
    auto_offset_reset='earliest' # 'latest' or 'earliest'
)

# Scheduling daily batch job
schedule.every(24).hours.do(
    batch_eth_hourly, 
    batch_data = gather_local_cached_data("data/cached_eth_hourly.json")
)

def scheduled_tasks_loop():
    while True:
        schedule.run_pending()

# Using Multiprocessing to run batch scheduler loop outside main consumer loop
batch_process = Process(target=scheduled_tasks_loop)

print("Start -----------------")

if __name__ == "__main__":

    if spark_main:
        print("Successfully initialized Spark Session")
        spark_main.sparkContext.setLogLevel("FATAL")

    batch_process.start()

    for message in consumer:
        message_dict = json.loads(message.value.decode('UTF-8'))
        print(message_dict)
        if message_dict['format'] == 'hour':
            handle_eth_hourly(message_dict)
        elif message_dict['format'] == 'train':
            handle_eth_hourly_train_data(message_dict)

    '''
    ----- Spark Streaming (Hadoop environment required) -----
    df = spark_main \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_PORT}") \
        .option("subscribe", KAFKA_TOPICS['eth-hourly']) \
        .option("startingOffsets", "earliest") \
        .load()
    print(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"))
    '''
