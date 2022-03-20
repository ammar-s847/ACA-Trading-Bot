# from kafka import ConsumerRecord

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
