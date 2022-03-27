# from kafka import ConsumerRecord
import numpy as np
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
from model.model_predict import predict_one
from model.model_train import train_new_bi_LSTM_model, train_new_seq_LSTM_model
from data.mongodb import MongoDBHandler

default_db_handler = MongoDBHandler(MONGO_CONNECT, MONGO_DB_NAME)

def handle_eth_hourly(
        message: dict, 
        db_handler: MongoDBHandler = default_db_handler
    ) -> None:
    '''
    - updates cached data
    - generates prediction
    - updates/adds day record in MongoDB
    '''
    pass

def handle_eth_hourly_train_data(message: dict) -> None:
    '''
    - Converts kafka training message data to spark df
    - Cleans and filters data
    - Trains new model
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
    df = df.withColumn('open', col('open').cast('Double')) \
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
    
    train_new_bi_LSTM_model(scaled_df2)
