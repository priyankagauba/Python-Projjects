import warnings
warnings.filterwarnings('ignore')
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import pandas as pd
import numpy as np
import datetime
from sklearn.metrics import mean_squared_error
from numpy import array
from keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
pd.set_option('display.max_columns()', None)


class Model2():

    def __init__(self):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)

    def __call__(self):
        self.output()

    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select * from ' + cg.ENERGY24 + ' where time > now() - 24h '
        df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        df['time'] = df['time'].astype('datetime64[ns]')
        return df


    def output(self):
        try:

            df = self.read_Data()
            df = df.set_index('time')
            EM4 = df[["EM4"]]
            EM4[EM4['EM4'] < 0] = 0
            
            model_mlp = load_model('MLP_model.h5')
            X = np.array(EM4.values).reshape(1, 24)
            start = (EM4.index.max() + pd.offsets.Hour(1))
            end = (EM4.index.max() + pd.offsets.Hour(4))
            predicted = pd.DataFrame(pd.date_range(start=start, end=end, freq='H'))
            prediction = model_mlp.predict(X, verbose=0)
            z = 1.64
            predicted['EM4_mlp'] = (prediction[0].reshape(4, 1))
            sse = (EM4[-1:].values - predicted['EM4_mlp'][:-3].values) ** 2
            lower_bound = prediction - z * np.sqrt(sse / len(EM4))
            upper_bound = prediction + z * np.sqrt(sse / len(EM4))
            lower_bound[lower_bound < 0] = 0.0
            predicted['lower_bound_mlp'] = pd.Series(lower_bound[0])
            predicted['upper_bound_mlp'] = pd.Series(upper_bound[0])
    
    
            model_gru = load_model('GRU_model.h5')
            scaler = MinMaxScaler(feature_range=(0, 1))
            transform_EM4 = scaler.fit_transform(EM4)
            n_input = 24
            X = array(transform_EM4[-n_input:]).reshape((1, n_input, 1))
            pred = model_gru.predict(X, verbose=0)
            pred = scaler.inverse_transform(pred) 
            predicted['EM4_gru']=pd.DataFrame(pred[0]) 
            z = 1.64
            sse = (EM4[-1:].values - predicted['EM4_gru'][:-3].values) ** 2
            predicted['lower_bound_gru'] = predicted[['EM4_gru']] - z * np.sqrt(sse / len(EM4))
            predicted['upper_bound_gru'] = predicted[['EM4_gru']] + z * np.sqrt(sse / len(EM4))
            predicted = predicted.set_index(0)
            predicted[predicted < 0] = 0
            print(predicted)
            #print(self.DFDBClient.write_points(predicted, cg.NN))
        except Exception as e:
            print(e)


if __name__ == '__main__':
    cat = Model2()
    t = cat.output()