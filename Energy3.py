import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
from datetime import datetime, timedelta
import pytz
import time


class Energy3():

    def __init__(self):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)

    def __call__(self):
        self.output()

    def difference(self, df1):
        diff = []
        for g, i in df1.groupby(pd.Grouper(freq='30T', key='time'))['EM_TOTAL_Import_Energy(kWh)']:
            try:
                diff.append([g, i.iloc[-1] - i.iloc[0]])
            except:
                diff.append([g, np.nan])
        x = pd.DataFrame(diff)
        return x

    def half_hr_agg(self, df1):
        o = df1.groupby('DeviceID').apply(self.difference)
        o = o.pivot_table(index=0, columns='DeviceID', values=1).reset_index()
        o.columns.name = None
        o = o.rename(columns={0: ""})
        o = o.set_index("")
        o = o.fillna(0)
        return o

    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select "EM_TOTAL_Import_Energy(kWh)", "DeviceID", "time" from ' + cg.TARGET_MEASUREMENT + ' where time > now() - 6h'
        df1 = pd.DataFrame(con_obj.query(query).get_points())
        df1['time'] = df1['time'].astype('datetime64[ns]')

        date = pd.DataFrame([pd.Timestamp(df1.time.min())], columns=['date_min'])
        date['new_date'] = date['date_min'].dt.floor('30min')
        start = (date['new_date'] + timedelta(minutes=0))[0]
        print(start + timedelta(hours=5, minutes=30))

        date['end_now'] = [pd.Timestamp(pd.datetime.utcnow())]
        date['end_now'] = date['end_now'].dt.floor('30min')
        end = (date['end_now'] + timedelta(hours=0))[0]
        print(end + timedelta(hours=5, minutes=30))

        df1 = df1[(df1['time'] >= start) & (df1['time'] < end)]

        # print(df1.time + timedelta(hours=5, minutes=30))
        df4 = self.half_hr_agg(df1)
        Time_range = pd.DataFrame(pd.date_range(start=start, end=end, freq='30T'))
        Time_range = Time_range.rename(columns={0: "time"})
        Time_range.set_index("time", inplace=True)
        # print(Time_range)
        df = df4.merge(Time_range, how="outer", right_index=True, left_index=True)
        # df.index = df.index.dt.floor('30min')
        # df.index = df.index + timedelta(hours=5, minutes=30)
        # print(df)
        df = df.fillna(0)
        df.index.freq = '30T'
        return df

    def output(self):
        try:
            df = self.read_Data()
            df = df.shift(1, axis=0)
            df = df[1:]
            # df.index = df.index + timedelta(hours=5, minutes=30)
            # print(df)
            # print(df.tail(2))
            print(self.DFDBClient.write_points(df, cg.ENERGY3))
        except Exception as e:
            print("Inside Energy3:" + str(e))


# if __name__ == '__main__':
#     cat = Energy3()
#     cat.output()