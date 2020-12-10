import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime


class Power_And_Energy():

    def __init__(self):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)

    def __call__(self):
        self.output()

    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select "EM_TOTAL_Import_Energy(kWh)", "time", "DeviceID", "EM_Active Power (kW)" from ' + cg.TARGET_MEASUREMENT + ' where time > now() - 30m '
        df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        df['time'] = df['time'].astype('datetime64[ns]')
        df['time'] = df['time'] + datetime.timedelta(hours=5, minutes=30)
        return df

    def energy_difference(self, df):
        y = pd.DataFrame(
            df.groupby(['DeviceID'])['EM_TOTAL_Import_Energy(kWh)'].apply(lambda x: x.iloc[-1] - x.iloc[0]))
        y = y.pivot_table(columns='DeviceID', values="EM_TOTAL_Import_Energy(kWh)").reset_index()
        y.columns.name = None
        y = y.drop("index", axis=1)
        return y

    def power_difference(self, df):
        p = pd.DataFrame(df.groupby(['DeviceID'])['EM_Active Power (kW)'].mean())
        p = p.pivot_table(columns='DeviceID', values="EM_Active Power (kW)").reset_index()
        p.columns.name = None
        p = p.drop("index", axis=1)
        return p

    def output(self):
        try:
            df = self.read_Data()
            y = self.energy_difference(df)
            y["time"] = df["time"].max()
            y = y.set_index("time")
            y = y.fillna(0)
            p = self.power_difference(df)
            p["time"] = df["time"].max()
            p = p.set_index("time")
            p = p.fillna(0)
            print(self.DFDBClient.write_points(p, cg.POWER30))
            print(self.DFDBClient.write_points(y, cg.ENERGY30))
        except Exception as e:
            print(e)