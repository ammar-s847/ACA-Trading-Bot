from pyspark.sql import SparkSession
import numpy, pandas, matplotlib, sklearn
import pyspark
import requests

spark = SparkSession \
        .builder \
        .appName('batch test') \
        .getOrCreate()

url = 'https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol=ETH&market=USD&apikey='
r = requests.get(url)
raw_data = r.json()
raw_data_time_series = raw_data['Time Series (Digital Currency Daily)']

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

df = spark.createDataFrame(data_dict)

print("\n\n")
print(df.head(5))
print("\n\n")
