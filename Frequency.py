import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime
import pytz


class Frequency():

    def __init__(self):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)

    def __call__(self):
        self.output()

    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select "EM_Frequency (Hz)", "DeviceID", "time" from ' + cg.TARGET_MEASUREMENT + ' where time > now() - 1d '
        df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        df['time'] = df['time'].astype('datetime64[ns]')
        df['time'] = df['time'] + datetime.timedelta(hours=5, minutes=30)
        return df

    def categorization_time(self, df):
        bins = [0, 49, 51, np.inf]
        names = ['Time_Warning', 'Time_Good', 'Time_Critical']
        d = dict(enumerate(names, 1))
        df['Status'] = np.vectorize(d.get)(np.digitize(df['EM_Frequency (Hz)'], bins))
        return df

    def calculate_time(self, df):
        df = self.categorization_time(df)
        x = pd.DataFrame(df.groupby([(df.Status != df.Status.shift()).cumsum()])['time'].apply(
            lambda x: (x.iloc[-1] - x.iloc[0]).total_seconds() / 60))
        x['Status'] = df.loc[df.Status.shift(-1) != df.Status]['Status'].values
        x.reset_index(drop=True, inplace=True)
        return x

    def categorization_count(self, df):
        bins = [0, 49, 51, np.inf]
        names = ['Count_Warning', 'Count_Good', 'Count_Critical']
        d = dict(enumerate(names, 1))
        df['Status'] = np.vectorize(d.get)(np.digitize(df['EM_Frequency (Hz)'], bins))
        return df

    def calculate_count(self, df):
        df = self.categorization_count(df)
        c = pd.DataFrame(df.groupby(['DeviceID', 'Status']).size()).reset_index()
        c = pd.pivot_table(index='DeviceID', columns='Status', values=0, data=c, aggfunc=np.sum).astype(np.float64)
        c.reset_index(inplace=True)
        return c

    def time_as_index(self, df):
        t = pd.DataFrame(df.groupby(['DeviceID'])['time'].max())
        t.reset_index(inplace=True)
        return t

    def time(self, df):
        df['Time_max'] = df['time'].dt.time
        a = df.loc[list(df.groupby('DeviceID')['EM_Frequency (Hz)'].idxmax())][['Time_max', 'DeviceID']]
        b = df.loc[list(df.groupby('DeviceID')['EM_Frequency (Hz)'].idxmin())][['Time_max', 'DeviceID']]
        a.index = a['DeviceID']
        a = a.drop('DeviceID', axis=1)
        a['Time_min'] = list(b['Time_max'])
        return a

    def zero_duration(self, df):
        p = df.index.to_series().diff() != 1
        p = df.groupby(p.cumsum())['time'].agg(lambda x: abs((x.iloc[-1] - x.iloc[0]).total_seconds() / 60)).sum()
        return p

    def output(self):
        try:
            df = self.read_Data()
            t = self.time_as_index(df)
            df0 = df[df['EM_Frequency (Hz)'] == 0]
            df = df[df['EM_Frequency (Hz)'] > 0]
            x = df.groupby('DeviceID')['EM_Frequency (Hz)'].describe()
            x.reset_index(inplace=True)
            x.columns = ['DeviceID', 'Total_Count', 'Average', 'SD', 'Minimum', '25th_percentile', 'Median',
                         '75th_percentile', 'Maximum']
            x = x.merge(t, on='DeviceID', how="outer")
            y = df.groupby('DeviceID').apply(self.calculate_time)
            y = y.reset_index()
            y = pd.pivot_table(index='DeviceID', columns='Status', values='time', data=y, aggfunc=np.sum).astype(
                np.float64)
            y.reset_index(inplace=True)
            x = x.merge(y, on='DeviceID', how="outer")
            a = self.time(df)
            x = x.merge(a, on='DeviceID', how="outer")
            c = self.calculate_count(df)
            x = x.merge(c, on='DeviceID', how="outer")
            if (len(df0) != 0):
                o = pd.DataFrame(df0.groupby(['DeviceID']).size(), columns=['count_0']).reset_index()
                x = x.merge(o, on='DeviceID', how="outer")
                p = pd.DataFrame(df0.groupby(['DeviceID']).apply(self.zero_duration),
                                 columns=['Duration_0']).reset_index()
                x = x.merge(p, on='DeviceID', how="outer")
            x = x.fillna(0)
            x.set_index('time', inplace=True)
            eastern = pytz.timezone('Asia/Kolkata')
            x.index = x.index.tz_localize(eastern).tz_convert(pytz.utc)
            print(self.DFDBClient.write_points(x, cg.FREQUENCY, tag_columns=['DeviceID']))
        except Exception as e:
            print('Exception in Frequency script:' + str(e))