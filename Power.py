import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime



class Power():
    
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
    
    def categorization_time(self,df):
        df['Time'] = df['time'].dt.time
        df['Status'] = 'time_0_4'
        df['Status'] = np.where(df['Time'] < datetime.time(4,0,0),'time_0_4',df['Status'])
        df['Status'] = np.where( (df['Time'] >= datetime.time(4,0,0) ) & ( df['Time'] < datetime.time(8,0,0) ),'time_4_8',df['Status'])
        df['Status'] = np.where( (df['Time'] >= datetime.time(8,0,0) ) & ( df['Time'] < datetime.time(12,0,0) ),'time_8_12',df['Status'])
        df['Status'] = np.where( (df['Time'] >= datetime.time(12,0,0) ) & ( df['Time'] < datetime.time(16,0,0) ),'time_12_16',df['Status'])
        df['Status'] = np.where( (df['Time'] >= datetime.time(16,0,0) ) & ( df['Time'] < datetime.time(20,0,0) ),'time_16_20',df['Status'])
        df['Status'] = np.where( (df['Time'] >= datetime.time(20,0,0) ) & ( df['Time'] <= datetime.time(23,59,59) ),'time_20_24',df['Status'])
        return df
    
    def calculate_time(self,df): 
        df = self.categorization_time(df)
        x = pd.DataFrame(df.groupby([(df.Status != df.Status.shift()).cumsum()])['time'].apply(lambda x:(x.iloc[-1]-x.iloc[0]).total_seconds()/60))
        x['Status']=df.loc[df.Status.shift(-1) != df.Status]['Status'].values
        x.reset_index(drop=True,inplace=True)
        return x
    
    def categorization_count(self,df):
        df['Time'] = df['time'].dt.time
        df['Status'] = 'Count_0_4'
        df['Status'] = np.where(df['Time'] < datetime.time(4,0,0),'Count_0_4',df['Status'])
        df['Status'] = np.where( (df['Time'] >= datetime.time(4,0,0) ) & ( df['Time'] < datetime.time(8,0,0) ),'Count_4_8',df['Status'])
        df['Status'] = np.where( (df['Time'] >= datetime.time(8,0,0) ) & ( df['Time'] < datetime.time(12,0,0) ),'Count_8_12',df['Status'])
        df['Status'] = np.where( (df['Time'] >= datetime.time(12,0,0) ) & ( df['Time'] < datetime.time(16,0,0) ),'Count_12_16',df['Status'])
        df['Status'] = np.where( (df['Time'] >= datetime.time(16,0,0) ) & ( df['Time'] < datetime.time(20,0,0) ),'Count_16_20',df['Status'])
        df['Status'] = np.where( (df['Time'] >= datetime.time(20,0,0) ) & ( df['Time'] <= datetime.time(23,59,59) ),'Count_20_24',df['Status'])
        return df
    
    def calculate_count(self,df): 
        df = self.categorization_count(df)
        c = pd.DataFrame(df.groupby(['DeviceID', 'Status']).size()).reset_index()
        c = pd.pivot_table(index ='DeviceID', columns ='Status', values = 0, data=c,aggfunc=np.sum).astype(np.float64)
        c.reset_index(inplace=True)
        return c
    
    def time(self,df):
        df['time'] = pd.to_datetime(df['time'])
        df['time_max'] = df['time'].dt.time
        a = df.loc[list(df.groupby('DeviceID')['EM_Active Power (kW)'].idxmax())][['time_max','DeviceID']]
        b = df.loc[list(df.groupby('DeviceID')['EM_Active Power (kW)'].idxmin())][['time_max','DeviceID']]
        a.index=a['DeviceID']
        a = a.drop('DeviceID',axis=1)
        a['time_min']=list(b['time_max'])
        return a
    
    def time_as_index(self,df):
        t = pd.DataFrame(df.groupby(['DeviceID'])['time'].max())
        t.reset_index(inplace = True)
        return t
    
    def zero_duration(self,df):
        p = df.index.to_series().diff()!=1
        p = df.groupby(p.cumsum())['time'].agg(lambda x: abs((x.iloc[-1] - x.iloc[0]).total_seconds() / 60)).sum()
        return p

    def machine_running_load(self, df):
        avg_power = df[df['SCS'] == 1]["EM_Active Power (kW)"].mean()
        return avg_power
    
    def output(self):
        try:
            df = self.read_Data()
            t = self.time_as_index(df)
            df0 = df[df['EM_Active Power (kW)'] == 0]
            df = df[df['EM_Active Power (kW)'] > 0]
            x = df.groupby('DeviceID')['EM_Active Power (kW)'].describe()
            x.reset_index(inplace=True)
            x.columns = ['DeviceID', 'Total_Count', 'Average', 'SD', 'Minimum', '25th_percentile', 'Median', '75th_percentile', 'Maximum']
            x = x.merge(t, on='DeviceID', how = "outer")
            y = df.groupby('DeviceID').apply(self.calculate_time)
            y = y.reset_index()
            y = pd.pivot_table(index ='DeviceID', columns ='Status', values = 'time',data=y,aggfunc=np.sum).astype(np.float64)
            y.reset_index(inplace=True)
            x = x.merge(y,on='DeviceID', how = "outer")
            a = self.time(df)
            x = x.merge(a,on='DeviceID', how = "outer")
            c = self.calculate_count(df)
            x = x.merge(c,on='DeviceID', how = "outer")
            x['Duty_Cycle'] = (x['Average']/x['Maximum'])
            b = pd.DataFrame(df.groupby("DeviceID").apply(self.machine_running_load)).astype(np.float64).reset_index()
            b = b.rename(columns={"DeviceID": "DeviceID", 0: "avg_running_load"})
            x = x.merge(b, on='DeviceID', how="outer")
            if(len(df0) != 0):
                o = pd.DataFrame(df0.groupby(['DeviceID']).size(), columns = ['count_0']).reset_index()
                x = x.merge(o, on='DeviceID', how = "outer")
                p = pd.DataFrame(df0.groupby(['DeviceID']).apply(self.zero_duration),columns=['Duration_0']).reset_index()
                x = x.merge(p,on='DeviceID', how = "outer")
            x = x.fillna(0)
            x.set_index('time', inplace = True)
            print(self.DFDBClient.write_points(x, cg.POWER))
        except Exception as e:
            print('Exception in Power:'+str(e))