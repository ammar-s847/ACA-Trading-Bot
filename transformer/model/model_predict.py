import pandas as pd
import tensorflow as tf
from keras.models import Sequential
from keras.layers import LSTM, Bidirectional, Embedding, Dense
from keras import Model, Input
from keras.optimizers import Adam
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

# Importing Local Modules
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
import model_train

def predict_one(
        model, 
        dataset: SparkDataFrame, 
        format = ["open", "high", "low", "close", "volume"],
        time_step = 75
    ):
    '''
    Predict one data point ahead

    Arguments:
    - model
    - dataset
    - format (OHLCV or Univariate)
    - interval (optional)
    '''
    pandasDF = dataset.toPandas()

    X_pred, y_pred = model_train.create_dataset(pandasDF[format], time_step)

    prediction = model.predict(X_pred)

    return prediction
