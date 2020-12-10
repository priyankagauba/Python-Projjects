import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime


class Loss_Agg():

    def __init__(self):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)

    def __call__(self):
        self.output()

    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select "kvah_loss_thd", "kvah_loss_total", "kvah_loss_pf", "time", "DeviceID" from ' + cg.WRITE_MEASUREMENT + ' where time > now() - 1d '
        df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        df['time'] = df['time'].astype('datetime64[ns]')
        df['time'] = df['time'] + datetime.timedelta(hours=5, minutes=30)
        return df

    def time_thd(self, df):
        df['Time_max'] = df['time'].dt.time
        a = df.loc[list(df.groupby('DeviceID')['kvah_loss_thd'].idxmax())][['Time_max', 'DeviceID']]
        b = df.loc[list(df.groupby('DeviceID')['kvah_loss_thd'].idxmin())][['Time_max', 'DeviceID']]
        a.index = a['DeviceID']
        a = a.drop('DeviceID', axis=1)
        a['Time_min'] = list(b['Time_max'])
        return a


    def time_pf(self, df):
        df['Time_max'] = df['time'].dt.time
        a = df.loc[list(df.groupby('DeviceID')['kvah_loss_pf'].idxmax())][['Time_max', 'DeviceID']]
        b = df.loc[list(df.groupby('DeviceID')['kvah_loss_pf'].idxmin())][['Time_max', 'DeviceID']]
        a.index = a['DeviceID']
        a = a.drop('DeviceID', axis=1)
        a['Time_min'] = list(b['Time_max'])
        return a

    def time_total(self, df):
        df['Time_max'] = df['time'].dt.time
        a = df.loc[list(df.groupby('DeviceID')['kvah_loss_total'].idxmax())][['Time_max', 'DeviceID']]
        b = df.loc[list(df.groupby('DeviceID')['kvah_loss_total'].idxmin())][['Time_max', 'DeviceID']]
        a.index = a['DeviceID']
        a = a.drop('DeviceID', axis=1)
        a['Time_min'] = list(b['Time_max'])
        return a

    def time_as_index(self, df):
        t = pd.DataFrame(df.groupby(['DeviceID'])['time'].max())
        t.reset_index(inplace=True)
        return t

    def output(self):
        try:
            df = self.read_Data()
            df['time'] = df['time'].astype('datetime64[s]')
            x1 = df.groupby('DeviceID')['kvah_loss_thd'].agg(['mean', 'min', 'max', 'sum', 'count'])
            a = self.time_thd(df)
            x1 = x1.merge(a, on='DeviceID', how="outer")
            x1.columns = ['Average_thd', 'Minimum_thd', 'Maximum_thd', 'Sum_thd', 'Total_Count_thd', 'Time_max_thd', 'Time_min_thd']

            x2 = df.groupby('DeviceID')['kvah_loss_pf'].agg(['mean', 'min', 'max', 'sum', 'count'])
            a = self.time_pf(df)
            x2 = x2.merge(a, on='DeviceID', how="outer")
            x2.columns = ['Average_pf', 'Minimum_pf', 'Maximum_pf', 'Sum_pf', 'Total_Count_pf', 'Time_max_pf', 'Time_min_pf']
            x = x2.merge(x1, on='DeviceID', how="outer")

            x3 = df.groupby('DeviceID')['kvah_loss_total'].agg(['mean', 'min', 'max', 'sum', 'count'])
            a = self.time_total(df)
            x3 = x3.merge(a, on='DeviceID', how="outer")
            x3.columns = ['Average_total', 'Minimum_total', 'Maximum_total', 'Sum_total', 'Total_Count_total', 'Time_max_total', 'Time_min_total']
            x = x.merge(x3, on='DeviceID', how="outer")

            t = self.time_as_index(df)
            x = x.merge(t, on='DeviceID', how="outer")
            x = x.fillna(0)
            x.set_index('time', inplace=True)
            print(self.DFDBClient.write_points(x, cg.LOSS_AGG))
        except Exception as e:
            print('Exception in LossTHD:' + str(e))

