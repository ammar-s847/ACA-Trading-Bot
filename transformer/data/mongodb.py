from pymongo import MongoClient

# Importing Local Modules
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from config import MONGO_CONNECT, MONGO_DB_NAME

# DAO (Data Access Object)
class MongoDBHandler():
    def __init__(self, connection_string: str, db_name: str):
        #self.conn = connection_string
        self.mongo = MongoClient(connection_string)
        self.db = self.mongo.get_database(db_name)
    
    def get_single_day_hourly_time_series(self, symbol: str, date: str) -> list:
        try:
            query = self.db.hourly.find({"symbol": symbol, "date" : date})
            return query
        except:
            return None

    def get_all_hourly_data_for_symbol(self, symbol: str) -> list:
        try:
            query = self.db.hourly.find({"symbol": symbol})
            return query
        except:
            return None

    def add_new_hourly_data_point(self, data: dict) -> None:
        '''
        data format:
        {
            symbol: str,
            date: str,
            time: str,
            OHLCV: {
                open: float
                high: float
                low: float
                close: float
                volume: float
            }
        }
        '''
        check = self.db.hourly.find_one(
            {
                "symbol": data['symbol'],
                "date": data['date'],
                "time": data['time']
            }
        )
        if check == None or len(check) == 0: # check if already exists
            self.db.hourly.insert_one(data)

    def add_new_hourly_prediction(self, data: dict) -> None:
        '''
        data format:
        {
            symbol: str,
            date: str,
            time: str,
            predicted_close: float/double
        }
        '''
        check = self.db.hourly.find_one(data)
        if check == None or len(check) == 0: # check if already exists
            self.db.predicted_hourly.insert_one(data)

    # def update_single_day_time_series(self, symbol: str, new_data: dict) -> dict:
    #     pass
