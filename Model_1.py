import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
from statistics import stdev
from datetime import datetime, timedelta
from sklearn.metrics import mean_squared_error
from math import sqrt
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.arima_model import ARIMA
import pytz


class Model1():

        def __init__(self):
            self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)

        def __call__(self):
            self.output()

        def read_Data_hw(self):
            con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
            query = 'select * from ' + cg.ENERGY24  + ' where time > now() - 3d '
            df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
            df['time'] = df['time'].astype('datetime64[ns]')
            return df

        def read_Data_sr(self):
            print('inside read method model_1')
            con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
            query = 'select * from ' + cg.ENERGY24  + ' where time > now() - 4d '
            df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
            df['time'] = df['time'].astype('datetime64[ns]')
            return df

        def read_Data_ar(self):
            print('inside read method model_1')
            con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
            query = 'select * from ' + cg.ENERGY24 + ' where time > now() - 2d '
            df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
            df['time'] = df['time'].astype('datetime64[ns]')
            return df

        def seasonal_period(self, EM4):
            import warnings
            warnings.filterwarnings('ignore')
            n = 10
            train = EM4[0:-n]
            test = EM4[-n:]
            x = int(len(train) / 2)
            l = []
            for j in range(2, x):
                t, d, s, p, b, r = ['add', True, 'add', j, False, True]
                fit1 = ExponentialSmoothing(np.asarray(train['EM4']), trend=t, damped=d, seasonal=s,
                                            seasonal_periods=p).fit(optimized=True, use_boxcox=b, remove_bias=r)
                x = fit1.forecast(len(test))
                rms = sqrt(mean_squared_error(test.EM4, x))
                l.append(rms)
            y = pd.DataFrame(l)
            y = (y[y[0] <= y[0].min()].index.values)[0]
            y = y + 2
            return y

        def holt_winters(self, EM4):
            a = 1
            t, d, s, p, b, r = ['add', True, 'add', self.seasonal_period(EM4), False, True]
            fit1 = ExponentialSmoothing(np.asarray(EM4['EM4']), trend=t, damped=d, seasonal=s, seasonal_periods=p).fit(
                optimized=True, use_boxcox=b, remove_bias=r)
            x = fit1.forecast(a)
            z = 1.64
            sse = fit1.sse
            lower_bound = x - z * np.sqrt(sse / len(EM4))
            upper_bound = x + z * np.sqrt(sse / len(EM4))
            lower_bound[lower_bound < 0] = 0.0
            date = (EM4.index.max() + pd.offsets.Hour(1))
            predicted = pd.DataFrame(
                [date, x[0], lower_bound[0].astype('float'), upper_bound[0].astype('float'), p.astype('int')]).T
            predicted = predicted.rename(
                columns={0: 'time', 1: 'EM4', 2: 'lower_bound', 3: 'upper_bound', 4: 'HP'})
            predicted[predicted['EM4'] < 0] = 0.0
            predicted['lower_bound'] = predicted['lower_bound'].astype('float')
            predicted['upper_bound'] = predicted['upper_bound'].astype('float')
            predicted['EM4'] = predicted['EM4'].astype('float')
            predicted = predicted.set_index('time')
            predicted = predicted.rename_axis(None, axis=1)
            predicted = predicted.fillna(0)
            return predicted

        def sarima(self, EM4):
            x = 12
            order = [1, 1, 1]
            seasonal_order = [0, 1, 1, x]
            a = 1
            model = SARIMAX(EM4, order=order, seasonal_order=seasonal_order, typ='levels').fit(disp=False, transparams=False, trend='c', solver='nm')
            x = model.get_forecast(1)
            predicted = x.summary_frame(alpha=0.05)
            predicted = predicted[['mean', 'mean_ci_lower', 'mean_ci_upper']]
            predicted = predicted.rename(columns={'mean': 'EM4', 'mean_ci_lower': 'lower_bound', 'mean_ci_upper': 'upper_bound'})
            predicted = predicted.rename_axis('time', axis=1)
            predicted[predicted < 0] = 0
            predicted['lower_bound'] = predicted['lower_bound'].astype('float')
            predicted['upper_bound'] = predicted['upper_bound'].astype('float')
            predicted['EM4'] = predicted['EM4'].astype('float')
            predicted = predicted.fillna(0)
            return predicted

        def arima(self, EM4):
            order = [1, 0, 0]
            a = 1
            model = ARIMA(EM4, order=order).fit(disp=False, transparams=False, trend='c', solver='nm')
            predictions = model.forecast(a, alpha=0.05)
            date = (EM4.index.max() + pd.offsets.Hour(1))
            predicted = pd.DataFrame([{'time': date, 'EM4_ar': predictions[0][0],
                                       'lower_bound_ar': predictions[2][0][0], 'upper_bound_ar': predictions[2][0][1]}])
            predicted = predicted.set_index('time')
            predicted[predicted < 0] = 0
            predicted['lower_bound_ar'] = predicted['lower_bound_ar'].astype('float')
            predicted['upper_bound_ar'] = predicted['upper_bound_ar'].astype('float')
            predicted['EM4_ar'] = predicted['EM4_ar'].astype('float')
            predicted = predicted.fillna(0)
            return predicted

        def output(self):
            try:
                df = self.read_Data_hw()
                df = df.set_index('time')
                EM4 = df[["EM4"]]
                EM4[EM4['EM4'] < 0] = 0
                hw = self.holt_winters(EM4)
                df = self.read_Data_sr()
                df = df.set_index('time')
                EM4 = df[["EM4"]]
                EM4[EM4['EM4'] < 0] = 0
                sr = self.sarima(EM4)
                predicted = hw.merge(sr, left_index=True, right_index=True, suffixes=['_hw', '_sr'])
                df = self.read_Data_ar()
                df = df.set_index('time')
                EM4 = df[["EM4"]]
                EM4[EM4['EM4'] < 0] = 0
                ar = self.arima(EM4)
                predicted = predicted.merge(ar, left_index=True, right_index=True)
                # print(predicted)
                print(self.DFDBClient.write_points(predicted, cg.MODEL))
            except Exception as e:
                print(e)


# if __name__ == '__main__':
#     cat = Model1()
#     t = cat.output()

