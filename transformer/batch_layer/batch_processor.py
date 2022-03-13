import numpy, pandas, sklearn
import pyspark
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DateType, DoubleType, BooleanType
from pyspark.sql.functions import col, lit, lag
from pyspark.sql.window import Window

spark = SparkSession \
        .builder \
        .appName('Trading Bot Pipeline') \
        .getOrCreate()

url = 'https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol=ETH&market=USD&apikey=68ZFL562SH8P1EZU'
r = requests.get(url)
raw_data = r.json()
raw_data_time_series = raw_data['Time Series (Digital Currency Daily)']

schema = StructType([ \
    StructField("cap", StringType(), True), \
    StructField("close", StringType(), True), \
    StructField("datetime", StringType(), True), \
    StructField("high", StringType(), True), \
    StructField("low", StringType(), True), \
    StructField("open", StringType(), True), \
    StructField("volume", StringType(), True) \
])

data_dict = []

for k, v in raw_data_time_series.items():
    data_dict.append({
        'datetime': k,
        'open': v['1a. open (USD)'],
        'high': v['2a. high (USD)'],
        'low': v['3a. low (USD)'],
        'close': v['4a. close (USD)'],
        'volume': v['5. volume'],
        'cap': v['6. market cap (USD)']
    })

df = spark.createDataFrame(data_dict, schema=schema)

df = df.withColumn('cap', col('cap').cast('Double')) \
       .withColumn('open', col('open').cast('Double')) \
       .withColumn('close', col('close').cast('Double')) \
       .withColumn('high', col('high').cast('Double')) \
       .withColumn('low', col('low').cast('Double')) \
       .withColumn('volume', col('volume').cast('Double')) \
       .withColumn("datetime", col("datetime").cast(DateType()))

print("\n\n")
print(df)
print("\n\n")
