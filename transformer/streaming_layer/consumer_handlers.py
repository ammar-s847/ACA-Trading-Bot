# from kafka import ConsumerRecord
from pyspark.sql.types import (
    StructField, 
    StructType, 
    StringType, 
    DateType, 
    DoubleType, 
    BooleanType, 
    TimestampType)

# Importing Local Modules
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from config import *
from model.model_predict import predict_one
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
    schema = StructType([
        StructField("close", StringType(), True), \
        StructField("datetime", StringType(), True), \
        StructField("high", StringType(), True), \
        StructField("low", StringType(), True), \
        StructField("open", StringType(), True), \
        StructField("volume", StringType(), True) \
    ])
    
