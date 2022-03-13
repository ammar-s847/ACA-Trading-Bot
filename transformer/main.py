import pyspark
from datetime import datetime
from pyspark.sql import SparkSession

spark_main = SparkSession \
        .builder \
        .appName('Trading Bot Pipeline') \
        .getOrCreate()

if spark_main:
    print("Successfully initialized Spark Session")
