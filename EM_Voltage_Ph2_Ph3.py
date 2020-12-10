import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime



class V23():

    def __init__(self):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
    
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
        df['diff'] = abs(df.groupby('DeviceID')['mean_volt'].diff())
        df = df.fillna(0)
        df['big_spike'] = np.where(df["diff"] > (df["diff"].mean() * 1.1), 1, 0)
        e = pd.DataFrame(
            df[df['big_spike'] == 1].groupby('DeviceID')['big_spike'].count().astype(np.float64)).reset_index()
        d = pd.DataFrame(df.groupby('DeviceID')['diff'].agg(['mean', 'min', 'max'])).reset_index().merge(e,on='DeviceID')
        d = d.rename(columns={"mean": "average_spike", "min": "minimum_spike", "max": "maximum_spike", "big_spike": "big_spike_count"})
        return d

    def time_as_index(self, df):
        t = pd.DataFrame(df.groupby(['DeviceID'])['time'].max())
        t.reset_index(inplace=True)
        return t

    def time(self, df):
        df['Time_max'] = df['time'].dt.time
        a = df.loc[list(df.groupby('DeviceID')['mean_volt'].idxmax())][['Time_max', 'DeviceID']]
        b = df.loc[list(df.groupby('DeviceID')['mean_volt'].idxmin())][['Time_max', 'DeviceID']]
        a.index = a['DeviceID']
        a = a.drop('DeviceID', axis=1)
        a['Time_min'] = list(b['Time_max'])
        return a
    
    def categorization_count(self,df):
        bins = [0, 375, 385, 445, 456, np.inf]
        names = ['count_critical', 'count_warning', 'count_normal', 'count_warning', 'count_critical']
        d = dict(enumerate(names, 1))
        df['Status'] = np.vectorize(d.get)(np.digitize(df['mean_volt'], bins))
        return df


    def categorization_time(self,df):
        bins = [0, 375, 385, 445, 456, np.inf]
        names = ['time_critical', 'time_warning', 'time_normal', 'time_warning', 'time_critical']
        d = dict(enumerate(names, 1))
        df['Status'] = np.vectorize(d.get)(np.digitize(df['mean_volt'], bins))
        return df


    def calculate_count(self, df):
        df = self.categorization_count(df)
        c = pd.DataFrame(df.groupby(['DeviceID', 'Status']).size()).reset_index()
        c = pd.pivot_table(index='DeviceID', columns='Status', values=0, data=c, aggfunc=np.sum).astype(np.float64)
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
        return y

    def summary_data(self, df):
        x = df.groupby('DeviceID')['mean_volt'].describe()
        x.columns = ['Total_Count', 'Average', 'SD', 'Minimum', '25th_percentile', 'Median', '75th_percentile', 'Maximum']
        x['IQR'] = x['75th_percentile'] - x['25th_percentile']
        return x
    
    def zero_duration(self,df):
        p = df.index.to_series().diff()!=1
        p = df.groupby(p.cumsum())['time'].agg(lambda x: abs((x.iloc[-1] - x.iloc[0]).total_seconds() / 60)).sum()
        return p 

    def output(self):
        try:
            df = self.read_Data()
            q = self.time_as_index(df)
            df0 = df[(df['EM_Voltage Ph2-Ph3 (V)'] == 0)]
            df = df[(df['EM_Voltage Ph2-Ph3 (V)'] > 0)]
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
            print(self.DFDBClient.write_points(x, cg.VoltageP23))
            return x
        except Exception as e:
            print('Exception in EM_Voltage_Ph2_Ph3:'+str(e))

