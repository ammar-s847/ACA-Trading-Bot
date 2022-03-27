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
    
    def get_single_day_time_series(self, symbol: str, date: str) -> dict:
        table = self.db.hourly
        check = table.find_one({"symbol": symbol, "date" : date})
        if check == None or len(check) == 0:
            return None
        else:
            pass

    def get_all_data_for_symbol(self, symbol: str) -> dict:
        pass

    def update_single_day_time_series(self, symbol: str, new_data: dict) -> dict:
        pass
