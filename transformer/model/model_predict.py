import model_train
import pandas as pd
import tensorflow as tf
from keras.models import Sequential
from keras.layers import LSTM, Bidirectional, Embedding, Dense
from keras import Model, Input
from keras.optimizers import Adam

def predict_one(model, dataset, format=["open", "high", "low", "close", "volume"]):
    '''
    Predict one data point ahead

    Arguments:
    - model
    - data points
    - format (OHLCV or Univariate)
    - interval
    '''
    pandasDF = dataset.toPandas()

    time_step = 75
    X_pred, y_pred = model_train.create_dataset(pandasDF[format], time_step)

    prediction = model.predict(X_pred)

    return prediction
