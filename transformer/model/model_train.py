import numpy as np
import pyspark
import pandas as pd
import tensorflow as tf
from keras.models import Sequential
from keras.layers import LSTM, Bidirectional, Embedding, Dense
from keras import Model, Input
from keras.optimizers import Adam
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

def create_dataset(dataset, time_step, offset = 0):
    x_train, y_train = [], []
    for i in range(time_step, len(dataset)):
        x_train.append(dataset[i-time_step:i])
        y_train.append(dataset['close'][i + offset])
    return np.array(x_train), np.array(y_train)
    
def train_new_seq_LSTM_model(
        dataset, 
        format = ["open", "high", "low", "close", "volume"], 
        model_name = "eth_hourly_seq_LSTM"
    ):
    '''
    Predict one data point ahead

    Arguments:
    - data points
    - interval
    - format (OHLCV or Univariate)
    '''
    pandasDF = dataset.toPandas()
    times = sorted(pandasDF.index.values)
    #fifteen_pct = sorted(pandasDF.index.values)[-int(0.15*len(times))]
    #thirty_pct = sorted(pandasDF.index.values)[-int(0.30*len(times))]
    twenty_pct = sorted(pandasDF.index.values)[-int(0.2*len(times))]

    df_train = pandasDF[(pandasDF.index < twenty_pct)]
    df_val = pandasDF[(pandasDF.index >= twenty_pct)]
    #df_val = pandasDF[(pandasDF.index >= thirty_pct) & (pandasDF.index < fifteen_pct)]
    #df_test = pandasDF[(pandasDF.index >= fifteen_pct)]

    time_step = 75
    X_train, y_train = create_dataset(df_train[format], time_step)
    X_val, y_val = create_dataset(df_val[format], time_step, offset=df_train.shape[0]+1)
    #X_test, y_test = create_dataset(df_test[format], time_step, offset=df_train.shape[0] + df_val.shape[0] + 1)

    model = Sequential([
        LSTM(50, return_sequences=True, input_shape=(time_step,5)),
        LSTM(50, return_sequences=True),
        LSTM(50),
        Dense(1)
    ])

    model.compile(
        optimizer = Adam(learning_rate=0.001),
        loss = 'mse',
        metrics = ['mae', 'mape']
    )

    callback = tf.keras.callbacks.ModelCheckpoint('cached_models\\' + model_name + '.hdf5', monitor='mape', save_best_only=True, verbose=1)

    model.fit(
        X_train,
        y_train,
        epochs = 30,
        batch_size = 30,
        verbose = 2,
        callbacks = [callback],
        validation_data = (
            X_val, 
            y_val
        )
    )

    model = tf.keras.models.load_model('cached_models\\' + model_name + '.hdf5')

    return model

def train_new_bi_LSTM_model():
    '''
    Predict one data point ahead

    Arguments:
    - data points
    - interval
    - format (OHLCV or Univariate)
    '''
    pass


def retrain_model(model, new_dataset):
    '''
    Predict one data point ahead

    Arguments:
    - model
    - new dataset
    '''
    
    pandasDF = new_dataset.toPandas()

    time_step=75
    X_new, y_new = create_dataset(pandasDF, time_step)

    model.compile(
        optimizer = Adam(learning_rate=0.001),
        loss = 'mse',
        metrics = ['mae', 'mape']
    )

    model.fit(
        X_new,
        y_new,
        epochs = 30,
        batch_size = 30,
        verbose = 2,
        # callbacks = [callback],
        # validation_data=(
        #     X_val, 
        #     y_val
        # )
    )

    return model
