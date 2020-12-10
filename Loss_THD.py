import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime



class THDLoss():
    
    def __init__(self):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)

    def __call__(self):
        self.output()
    
    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select * from ' + cg.WRITE_MEASUREMENT  + ' where time > now() - 1d '
        df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        df['time'] = df['time'].astype('datetime64[ns]')
        df['time'] = df['time'] + datetime.timedelta(hours=5, minutes=30)
        return df
    
    def total_sum(self,df):
        y = pd.DataFrame(df.groupby('DeviceID')['kvah_loss_thd'].sum()).astype(np.float64)
        y.columns = ['Sum']
        y = y.reset_index()
        return y 
    
    def time(self,df):
        df['Time_max'] = df['time'].dt.time
        a=df.loc[list(df.groupby('DeviceID')['kvah_loss_thd'].idxmax())][['Time_max','DeviceID']]
        b=df.loc[list(df.groupby('DeviceID')['kvah_loss_thd'].idxmin())][['Time_max','DeviceID']]
        a.index=a['DeviceID']
        a=a.drop('DeviceID',axis=1)
        a['Time_min']=list(b['Time_max'])
        return a
    
    def time_as_index(self,df):
        t = pd.DataFrame(df.groupby(['DeviceID'])['time'].max())
        t.reset_index(inplace = True)
        return t

    def output(self):
        try:
            df = self.read_Data()
            x = df.groupby('DeviceID')['kvah_loss_thd'].describe()
            x.columns = ['Total_Count', 'Average', 'SD', 'Minimum', '25th_percentile', 'Median', '75th_percentile', 'Maximum']
            y = self.total_sum(df)
            x = x.merge(y, on = 'DeviceID', how = "outer")
            t = self.time_as_index(df)
            x=x.merge(t,on='DeviceID', how = "outer")
            a=self.time(df)
            x=x.merge(a,on='DeviceID', how = "outer")
            x = x.fillna(0)
            x.set_index('time', inplace = True)
            print(self.DFDBClient.write_points(x, cg.THD_LOSS))
        except Exception as e:
            print('Exception in LossTHD:'+str(e))
