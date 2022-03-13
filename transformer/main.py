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
