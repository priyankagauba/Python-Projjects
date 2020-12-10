import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime



class C3():

    def __init__(self, a):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        self.a = a

    def __call__(self):
        self.output()
    
    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select * from ' + cg.TARGET_MEASUREMENT  + ' where time > now() - 1d '
        df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        df['time'] = df['time'].astype('datetime64[ns]')
        df['time'] = df['time'] + datetime.timedelta(hours=5, minutes=30)
        return df

    def avg_stability(self, df):
        df['diff'] = abs(df.groupby('DeviceID')['EM_Current Ph3 (A)'].diff())
        df = df.fillna(0)
        df['big_spike'] = np.where(df["diff"] > (df["diff"].mean() * 1.1), 1, 0)
        e = pd.DataFrame(
            df[df['big_spike'] == 1].groupby('DeviceID')['big_spike'].count().astype(np.float64)).reset_index()
        d = pd.DataFrame(df.groupby('DeviceID')['diff'].agg(['mean', 'min', 'max'])).reset_index().merge(e,
                                                                                                         on='DeviceID')
        d = d.rename(columns={"mean": "average_spike", "min": "minimum_spike", "max": "maximum_spike",
                              "big_spike": "big_spike_count"})
        return d

    def time_as_index(self, df):
        t = pd.DataFrame(df.groupby(['DeviceID'])['time'].max())
        t.reset_index(inplace=True)
        return t

    def time(self, df):
        df['Time_max'] = df['time'].dt.time
        a = df.loc[list(df.groupby('DeviceID')['EM_Current Ph3 (A)'].idxmax())][['Time_max', 'DeviceID']]
        b = df.loc[list(df.groupby('DeviceID')['EM_Current Ph3 (A)'].idxmin())][['Time_max', 'DeviceID']]
        a.index = a['DeviceID']
        a = a.drop('DeviceID', axis=1)
        a['Time_min'] = list(b['Time_max'])
        return a

    def categorization_time(self, df):
        df['Status'] = 'normal'
        for i in self.a.keys():
            x = df[df['DeviceID'] == i]
            x['Status'] = np.where(x['EM_Current Ph3 (A)'] > self.a[i], 'high', x['Status'])
            df[df['DeviceID'] == i] = x
        return df

    def calculate_count(self, df):
        df = self.categorization_time(df)
        c = pd.DataFrame(df.groupby(['DeviceID', 'Status']).size()).reset_index()
        c = pd.pivot_table(index='DeviceID', columns='Status', values=0, data=c, aggfunc=np.sum).astype(np.float64)
        c = c.rename(columns={"normal": "normal_count", "high": "high_count"})
        c.reset_index(inplace=True)
        return c

    def duration(self, df):
        x = pd.DataFrame(df.groupby([(df.Status != df.Status.shift()).cumsum()])['time'].apply(
            lambda x: (x.iloc[-1] - x.iloc[0]).total_seconds() / 60))
        x['Status'] = df.loc[df.Status.shift(-1) != df.Status]['Status'].values
        x.reset_index(drop=True, inplace=True)
        return x

    def calculate_time(self, df):
        df = self.categorization_time(df)
        y = df.groupby(['DeviceID']).apply(self.duration).reset_index()
        y = pd.pivot_table(index='DeviceID', columns='Status', values='time', data=y, aggfunc=np.sum).astype(np.float64).reset_index()
        y = y.rename(columns={"normal": "normal_duration", "high": "high_duration"})
        return y

    def summary_data(self, df):
        x = df.groupby('DeviceID')['EM_Current Ph3 (A)'].describe()
        x.columns = ['Total_Count', 'Average', 'SD', 'Minimum', '25th_percentile', 'Median', '75th_percentile',
                     'Maximum']
        x['IQR'] = x['75th_percentile'] - x['25th_percentile']
        return x

    def zero_duration(self, df):
        t = df.index.to_series().diff() != 1
        t = df.groupby(t.cumsum())['time'].agg(lambda x: abs((x.iloc[-1] - x.iloc[0]).total_seconds() / 60)).sum()
        return t

    def scs_count(self, df):
        s = ((df.SCS != df.SCS.shift(axis=0)).sum(axis=0) - 1) / 2
        return s

    def output(self):
        try:
            df = self.read_Data()
            q = self.time_as_index(df)
            s = pd.DataFrame(df.groupby('DeviceID').apply(self.scs_count))
            s = s.rename(columns={0: 'scs_count'})
            q = q.merge(s, on="DeviceID", how="outer")
            df0 = df[df['EM_Current Ph3 (A)'] == 0]
            df = df[df['EM_Current Ph3 (A)'] > 0]
            x = self.summary_data(df)
            x = q.merge(x, on="DeviceID", how="outer")
            y = self.calculate_time(df)
            x = x.merge(y, on="DeviceID", how="outer")
            z = self.calculate_count(df)
            x = x.merge(z, on="DeviceID", how="outer")
            w = self.avg_stability(df)
            x = x.merge(w, on="DeviceID", how="outer")
            v = self.time(df)
            x = x.merge(v, on="DeviceID", how="outer")

            if (len(df0) != 0):
                o = pd.DataFrame(df0.groupby(['DeviceID']).size(), columns=['count_0']).reset_index()
                x = x.merge(o, on='DeviceID', how='outer')
                t = pd.DataFrame(df0.groupby(['DeviceID']).apply(self.zero_duration), columns=['Duration_0']).reset_index()
                x = x.merge(t, on='DeviceID', how='outer')
            x = x.fillna(0)
            x.set_index('time', inplace=True)
            print(self.DFDBClient.write_points(x, cg.CurrentL3))
        except Exception as e:
            print('Exception in CurrentL3:'+str(e))
