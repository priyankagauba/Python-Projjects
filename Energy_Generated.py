import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime




class EnergyGenerated():
    
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
    
    def time_as_index(self,df):
        t = pd.DataFrame(df.groupby(['DeviceID'])['time'].max())
        t.reset_index(inplace = True)
        return t

    def energy_per_day(self, df):
        y = pd.DataFrame(df.groupby(['DeviceID'])['EM_TOTAL_Export_Energy(kWh)'].apply(lambda x:x.iloc[-1]-x.iloc[0]))
        y.rename(columns={"DeviceID": "DeviceID", "EM_TOTAL_Export_Energy(kWh)": "EPD"}, inplace = True)
        y.reset_index(inplace=True)
        return y
    
    def output(self):
        try:
            df = self.read_Data()
            x = df.groupby('DeviceID')['EM_TOTAL_Export_Energy(kWh)'].describe()
            x.columns = ['Total_Count', 'Average', 'SD', 'Minimum', '25th_percentile', 'Median', '75th_percentile', 'Maximum']
            x.reset_index()
            t = self.time_as_index(df)
            x = x.merge(t,on='DeviceID', how = "outer")
            y = self.energy_per_day(df)
            x = x.merge(y,on='DeviceID', how = "outer")
            x = x.fillna(0)
            x.set_index('time', inplace = True)
            print(self.DFDBClient.write_points(x, cg.ENERGY_GENERATED))
        except Exception as e:
            print('Exception in Energy Generated:'+str(e))
