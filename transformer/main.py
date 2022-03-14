import pyspark
from datetime import datetime
from pyspark.sql import SparkSession

# Importing Local Modules
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

spark_main = SparkSession \
        .builder \
        .appName('Trading Bot Pipeline') \
        .getOrCreate()

if spark_main:
    print("Successfully initialized Spark Session")


import requests
import datetime
import time
import json
import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer

print("Start -----------------")

if __name__ == "__main__":
    consumer = KafkaConsumer(
        'messages1',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest' # 'earliest'
    )

    for message in consumer:
        print(message)
        #print(str(json.loads(message)))