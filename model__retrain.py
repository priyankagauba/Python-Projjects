import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
import Config as cg
from influxdb import *
from influxdb import DataFrameClient
from datetime import datetime, timedelta
from numpy import array
import pytz
import time
import keras
from keras.models import Sequential, load_model
from keras.layers import Dense,Dropout,GRU
from numpy import array
from sklearn.preprocessing import MinMaxScaler
from numpy.random import seed
seed(1)
from tensorflow import set_random_seed
set_random_seed(2)

class Model_Retrain():

    def __init__(self):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)

    def __call__(self):
        self.output()

    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select "time", "EM4" from ' + cg.ENERGY24 + ' where time > now() - 150d '
        df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        df['time'] = df['time'].astype('datetime64[ns]')
        df = df.set_index('time')
        #print(df.head())

        Time_range = pd.DataFrame(pd.date_range(start=(df.index.min()), end=(df.index.max()), freq='H'))
        Time_range = Time_range.rename(columns={0: "time"})
        Time_range.set_index("time", inplace=True)
        df = df.merge(Time_range, how="outer", right_index=True, left_index=True)
        df = df.interpolate(method='time')
        df[df['EM4'] < 0] = 0
        df.index.freq = 'H'
        df = df.fillna(0)
        return df

    def split_sequence(self, df, n_in, n_out):
        X, y = list(), list()
        for i in range(len(df)):
            end_ix = i + n_in
            out_end_ix = end_ix + n_out
            if out_end_ix > len(df):
                break
            seq_x, seq_y = df[i:end_ix], df[end_ix:out_end_ix]
            X.append(seq_x)
            y.append(seq_y)
        return array(X), array(y)

    def to_supervised_learning(self, df):
        df = list(df['EM4'].values)
        n_in, n_out = 24, 4
        X, y = self.split_sequence(df, n_in, n_out)
        Xl = []
        yl = []
        for i in range(len(X)):
            Xl.append(X[i])
            yl.append(y[i])
        inp = pd.DataFrame(Xl)
        out = pd.DataFrame(yl)
        out = out.rename(columns={0: 'out1', 1: 'out2', 2: 'out3', 3: 'out4'})
        df = inp.merge(out, left_index=True, right_index=True)
        return df
    
        # normalize the dataset
    def scaled_data(self,data):
        scaler = MinMaxScaler(feature_range=(0, 1))
        data = scaler.fit_transform(data)
        data = pd.DataFrame(data)
        return data
    

    def model_mlp(self, df):
        n_in, n_out = 24, 4
        X_train = df.drop(['out1', 'out2', 'out3', 'out4'], axis=1).values
        y_train = df[['out1', 'out2', 'out3', 'out4']].values
        model = Sequential()
        model.add(Dense(100, activation='relu', input_dim=n_in))
        model.add(Dense(64, activation='relu'))
        model.add(Dropout(0.3))
        model.add(Dense(32, activation='relu'))
        model.add(Dropout(0.3))
        model.add(Dense(n_out))
        model.compile(optimizer='adam', loss='mse')
        model.fit(X_train, y_train, epochs=500, verbose=0)
        print('MLP')
        #return model.save("MLP_model.h5")
        

    def GRU_model(self,df):
        #df=self.read_data()
        # define input sequence
        raw_seq = self.scaled_data(df)[0].values
        
        # choose a number of time steps
        n_steps_in, n_steps_out,n_features,epochs= 24, 4,1,200
        
        # split into samples
        X, y = self.split_sequence(raw_seq, n_steps_in, n_steps_out)

        # reshape from [samples, timesteps] into [samples, timesteps, features]
        X = X.reshape((X.shape[0], X.shape[1], 1))
        
        model = Sequential()
        model.add(GRU(64, activation='tanh', return_sequences=True, input_shape=(n_steps_in, n_features)))
        model.add(GRU(64, activation='tanh'))
        model.add(Dense(32))
        model.add(Dropout(0.3))
        model.add(Dense(n_steps_out))
        adam = keras.optimizers.Adam(learning_rate=3e-04)
        model.compile(optimizer=adam, loss='mse')
        model.fit(X, y, epochs= epochs , verbose=0)
        #return model.save("GRU_model.h5")
        print('GRU')
    

    def output(self):
        df = self.read_Data()
        df = self.to_supervised_learning(df)
        self.model_mlp(df)
        self.GRU_model(df)


if __name__ == '__main__':
    cat = Model_Retrain()
    cat.output()