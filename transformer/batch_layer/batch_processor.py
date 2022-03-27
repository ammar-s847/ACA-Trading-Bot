import numpy, pandas, sklearn
import pyspark
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField, 
    StructType, 
    StringType, 
    DateType, 
    DoubleType, 
    BooleanType, 
    TimestampType)
from pyspark.sql.functions import col, lit, lag
from pyspark.sql.window import Window
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf

# Importing Local Modules
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from config import *
from data.mongodb import MongoDBHandler
from model.model_train import retrain_model

default_db_handler = MongoDBHandler(MONGO_CONNECT, MONGO_DB_NAME)

def gather_local_cached_data(file_name: str) -> list:
    '''
    gather the locally cached data points (json format) to be used for batch processing
    (obtains a single day of data -> 24 hours)
    '''
    pass

def batch_eth_hourly(
        batch_data: list, 
        db_handler: MongoDBHandler = default_db_handler
    ) -> None:
    '''
    - clean batch data
    - retrain on model
    - validate new training data
    - calculate daily moving average
    - store results on db
    '''
    
    # Convert item list into dataframe
    schema = StructType([
        StructField("close", StringType(), True), \
        StructField("datetime", StringType(), True), \
        StructField("high", StringType(), True), \
        StructField("low", StringType(), True), \
        StructField("open", StringType(), True), \
        StructField("volume", StringType(), True) \
    ])

    # Handling Data types
    df = batch_data.withColumn('open', col('open').cast('Double')) \
    .withColumn('close', col('close').cast('Double')) \
    .withColumn('high', col('high').cast('Double')) \
    .withColumn('low', col('low').cast('Double')) \
    .withColumn('volume', col('volume').cast('Double')) \
    .withColumn("datetime", col("datetime").cast(TimestampType()))

    # Handling empty and duplicate values
    df = df.na.drop()

    # Storing batch into database (calculate daily moving average?)

    # Normalizing data for training
    assembler = VectorAssembler(
        inputCols = ["open", "high", "low", "close", "volume"],
        outputCol = "vector"
    )
    scaler = MinMaxScaler(outputCol="scaled")
    scaler.setInputCol("vector")
    pipeline = Pipeline(stages=[assembler, scaler])
    scaler_model = pipeline.fit(df)
    scaled_df = scaler_model.transform(df)

    scaled_df1 = scaled_df.select([
        "datetime", 
        "open_scaled", 
        "high_scaled", 
        "low_scaled", 
        "close_scaled", 
        "volume_scaled"
    ])

    unlist = udf(lambda x: float(list(x)[0]), DoubleType())

    scaled_columns = [
        "open_scaled", 
        "high_scaled", 
        "low_scaled", 
        "close_scaled", 
        "volume_scaled"
    ]

    scaled_df2 = scaled_df1

    for col_name in scaled_columns:
        scaled_df2 = scaled_df2.withColumn(col_name, unlist(col_name))
    
    retrain_model(scaled_df2)


# data_dict = []

# for k, v in raw_data_time_series.items():
#     data_dict.append({
#         'datetime': k,
#         'open': v['1a. open (USD)'],
#         'high': v['2a. high (USD)'],
#         'low': v['3a. low (USD)'],
#         'close': v['4a. close (USD)'],
#         'volume': v['5. volume']
#     })

''' 
----- Unorganized PySpark Code -----

win = Window.orderBy('datetime')
df = df.withColumn('percent_change_close', (df['close'] - lag(df['close']).over(win))/100)

from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

assembler = VectorAssembler(inputCols=["open", "high", "low", "close", "volume", "cap"], outputCol="vector")
scaler = MinMaxScaler(outputCol="scaled")
scaler.setInputCol("vector")
pipeline = Pipeline(stages=[assembler, scaler])
scaler_model = pipeline.fit(df)
scaled_df = scaler_model.transform(df)
scaled_df.show(5)

columns_to_scale = ["open", "high", "low", "close", "volume", "cap", "percent_change_close"]
assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec").setHandleInvalid("skip") for col in columns_to_scale]
scalers = [MinMaxScaler(inputCol=col + "_vec", outputCol=col + "_scaled") for col in columns_to_scale]
pipeline = Pipeline(stages=assemblers + scalers)
scaler_model = pipeline.fit(df)
scaled_df = scaler_model.transform(df)
scaled_df.show(5)

scaled_df1 = scaled_df.select(["datetime", "open_scaled", "high_scaled", "low_scaled", "close_scaled", "volume_scaled", "cap_scaled", "percent_change_close_scaled"])

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

#Do we need the data scaled in a vector or just scalar for the model?
unlist = udf(lambda x: float(list(x)[0]), DoubleType())

scaled_columns = ["open_scaled", "high_scaled", "low_scaled", "close_scaled", "volume_scaled", "cap_scaled", "percent_change_close_scaled"]

scaled_df2 = scaled_df1

for col_name in scaled_columns:
    scaled_df2 = scaled_df2.withColumn(col_name, unlist(col_name))

'''
