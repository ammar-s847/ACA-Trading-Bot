# from pyspark.sql import SparkSession
# import numpy, pandas, matplotlib, sklearn
# import pyspark
# import requests

# spark = SparkSession \
#         .builder \
#         .appName('batch test') \
#         .getOrCreate()

# url = 'https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol=ETH&market=USD&apikey='
# r = requests.get(url)
# raw_data = r.json()
# raw_data_time_series = raw_data['Time Series (Digital Currency Daily)']

# data_dict = []

# for k, v in raw_data_time_series.items():
#     data_dict.append({
#         'datetime': k,
#         'open': v['1a. open (USD)'],
#         'high': v['2a. high (USD)'],
#         'low': v['3a. low (USD)'],
#         'close': v['4a. close (USD)'],
#         'volume': v['5. volume'],
#         'cap': v['6. market cap (USD)']
#     })

# df = spark.createDataFrame(data_dict)

# print("\n\n")
# print(df.head(5))
# print("\n\n")

import multiprocessing
from multiprocessing import Process
from threading import Thread
import schedule, json
from kafka import KafkaConsumer

# Importing Local Modules
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from config import *

schedule.every(10).seconds.do(
    print,
    "Running Scheduled Job"
)

consumer = KafkaConsumer(
    KAFKA_TOPICS['eth-hourly'],
    bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
    auto_offset_reset='latest'
)

def scheduled_tasks():
    while True:
        schedule.run_pending()

def stream_tasks():
    for message in consumer:
        message_dict = json.loads(message.value.decode('UTF-8'))
        print(message_dict)

batch_thread = Thread(target=scheduled_tasks)
stream_thread = Thread(target=stream_tasks)

batch_process = Process(target=scheduled_tasks)
stream_process = Process(target=stream_tasks)

print("Start -----------------")

if __name__ == "__main__":
    # batch_thread.start()
    # stream_thread.start()
    stream_process.start()
    # batch_process.start()
    while True:
        schedule.run_pending()
