import pandas as pd
import numpy as np
import tensorflow as tf
from keras.models import Sequential
from keras.layers import LSTM, Bidirectional, Embedding, Dense
from keras import Model, Input
#from tensorflow.keras.optimizers import adam_v2
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

# Importing Local Modules
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
import model.model_train as model_train

def predict_one(
        model, 
        dataset: np.ndarray
    ):
    '''
    Predict one data point ahead (using Numpyy array as input)

    Arguments:
    - model
    - dataset
    '''
    return model.predict(dataset)

def predict_one_with_spark_df(
        model, 
        dataset: SparkDataFrame, 
        format = ["open", "high", "low", "close", "volume"],
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
    X_pred = pandasDF.to_numpy()

    prediction = model.predict(X_pred)

    return prediction