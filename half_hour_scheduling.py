import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
from datetime import datetime, timedelta
import pytz
from math import sqrt
import time

class PowerFactorLoss_KPI():

    def __init__(self, base_pf):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        self.base_pf = base_pf

    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select "DeviceID", "EM_Power Factor", "EM_Active Power (kW)", "mean_volt", "Mean_THD", "EM_THD Voltage", "EM_Voltage Ph1-Ph2 (V)", "EM_Voltage Ph2-Ph3 (V)", "EM_Voltage Ph1-Ph3 (V)", "EM_Current Ph1 (A)", "EM_Current Ph2 (A)", "EM_Current Ph3 (A)", "mean_current", "time" from ' + cg.TARGET_MEASUREMENT + ' where time > now() - 2h '
        df = pd.DataFrame(con_obj.query(query).get_points())
        df['time'] = df['time'].astype('datetime64[ns]')

        date = pd.DataFrame([pd.Timestamp(df.time.min())], columns=['date_min'])
        date['new_date'] = date['date_min'].dt.floor('30min')
        start = (date['new_date'] - timedelta(minutes=0))[0]
        print(start + timedelta(hours=5, minutes=30))

        date['end_now'] = [pd.Timestamp(pd.datetime.utcnow())]
        date['end_now'] = date['end_now'].dt.floor('30min')
        end = (date['end_now'] + timedelta(hours=0))[0]
        print(end + timedelta(hours=5, minutes=30))

        df = df[(df['time'] >= start) & (df['time'] < end)]
        return df

    def read_Data_energy(self, df):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select * from ' + cg.ENERGY3 + ' where time > now() - 2h '
        df5 = pd.DataFrame(con_obj.query(query).get_points())
        df5['time'] = df5['time'].astype('datetime64[ns]')
        # df5['time'] = df5['time'] + timedelta(hours=5, minutes=30)

        date = pd.DataFrame([pd.Timestamp(df.index.min())], columns=['date_min'])
        date['new_date'] = date['date_min'].dt.floor('30min')
        start = (date['new_date'] - timedelta(minutes=0))[0]
        print(start + timedelta(hours=5, minutes=30))

        date['end_now'] = [pd.Timestamp(pd.datetime.utcnow())]
        date['end_now'] = date['end_now'].dt.floor('30min')
        end = (date['end_now'] + timedelta(hours=0))[0]
        print(end + timedelta(hours=5, minutes=30))

        df5 = df5[(df5['time'] >= start) & (df5['time'] <= end)]
        # print(df5)
        x = list(df5.columns)[:-1]
        energy = pd.melt(df5, id_vars=['time'], value_vars=x)
        energy = energy.rename(columns={'time': 'Time', 'variable': 'DeviceID', 'value': 'energy'})
        # print(energy)
        return energy

    def __call__(self):
        self.output()

    def calculated_columns_df(self, df):
        df['S'] = df['EM_Active Power (kW)'] / df['EM_Power Factor']
        df['S1_pf'] = df['EM_Active Power (kW)'] / 0.99
        df['running_current'] = (df['S'] * 1000) / (sqrt(3) * df['mean_volt'])
        df['current_rms'] = df.apply(lambda x: x['running_current'] / sqrt(1 + (x['Mean_THD'] * x['Mean_THD'])), axis=1)
        df['voltage_rms'] = df.apply(lambda x: x['mean_volt'] / sqrt(1 + (x['EM_THD Voltage'] * x['EM_THD Voltage'])),
                                     axis=1)
        df['PCWH'] = (sqrt(3) * df['current_rms'] * df['voltage_rms']) / 1000
        return df

    def calculated_columns_df_out(self, df):
        df_out = pd.DataFrame()
        df_out['S'] = df[['S']].mean()
        df_out['S1_pf'] = df[['S1_pf']].mean()[0]
        df_out['running_current'] = df[['running_current']].mean()[0]
        df_out['current_rms'] = df[['current_rms']].mean()[0]
        df_out['voltage_rms'] = df[['voltage_rms']].mean()[0]
        df_out['PCWH'] = df[['PCWH']].mean()[0]
        return df_out

    def vol_imb_calculated_columns(self, df):
        df['Vmax'] = df[['EM_Voltage Ph1-Ph2 (V)', 'EM_Voltage Ph2-Ph3 (V)', 'EM_Voltage Ph1-Ph3 (V)']].max(axis=1)
        df['Vmin'] = df[['EM_Voltage Ph1-Ph2 (V)', 'EM_Voltage Ph2-Ph3 (V)', 'EM_Voltage Ph1-Ph3 (V)']].min(axis=1)
        df['Vimb'] = np.where((df['Vmax'] - df['mean_volt']) > (df['mean_volt'] - df['Vmin']),
                              (df['Vmax'] - df['mean_volt']), (df['mean_volt'] - df['Vmin']))
        df['Vimb%'] = (df['Vimb'] / df['mean_volt']) * 100
        return df

    def cur_imb_calculated_columns(self, df):
        df['Imax'] = df[['EM_Current Ph1 (A)', 'EM_Current Ph2 (A)', 'EM_Current Ph3 (A)']].max(axis=1)
        df['Imin'] = df[['EM_Current Ph1 (A)', 'EM_Current Ph2 (A)', 'EM_Current Ph3 (A)']].min(axis=1)
        df['Iimb'] = np.where((df['Imax'] - df['mean_current']) > (df['mean_current'] - df['Imin']),
                              (df['Imax'] - df['mean_current']), (df['mean_current'] - df['Imin']))
        df['Iimb%'] = (df['Iimb'] / df['mean_current']) * 100
        return df

    def heat_loss(self, df):
        df['heat_loss'] = 2 * (df['Vimb%'] ** 2) * 100
        return df

    def time_as_index(self, df):
        t = pd.DataFrame(df.groupby(['DeviceID'])['time'].max())
        t.reset_index(inplace=True)
        return t

    def time_duration(self, df):
        x = pd.DataFrame(
            df.groupby('DeviceID').apply(lambda x: (x['time'].max() - x['time'].min()).total_seconds() / 3600)).rename(
            columns={0: 'duration_hrs'})
        return x

    def fun_new(self, df):
        df = df.reset_index()
        df = self.calculated_columns_df(df)
        df_out = df.groupby('DeviceID').apply(self.calculated_columns_df_out)

        t = self.time_as_index(df)
        df_out = df_out.merge(t, on="DeviceID", how="outer")

        x = self.time_duration(df)
        df_out = df_out.merge(x, on="DeviceID", how="outer")
        df_out['kvah_loss_thd'] = (df_out['S'] - df_out['PCWH']) * df_out['duration_hrs']
        df_out['kvah_loss_pf'] = (df_out['S'] - df_out['S1_pf']) * df_out['duration_hrs']
        average_pf = df.groupby('DeviceID')['EM_Power Factor'].mean()
        df_out = df_out.merge(average_pf, on='DeviceID', how="outer")
        df_out.rename(columns={'EM_Power Factor': 'average_pf'}, inplace=True)
        average_thd = df.groupby('DeviceID')['Mean_THD'].mean()
        df_out = df_out.merge(average_thd, on='DeviceID', how="outer")
        df_out.rename(columns={'Mean_THD': 'average_thd'}, inplace=True)
        df_out['average_thd'] = df_out['average_thd']*100.0

        df = self.vol_imb_calculated_columns(df)
        df = self.cur_imb_calculated_columns(df)
        df = self.heat_loss(df)
        imbalance = pd.DataFrame()
        imbalance['Vimb%'] = df.groupby('DeviceID')['Vimb%'].mean()
        imbalance['Iimb%'] = df.groupby('DeviceID')['Iimb%'].mean()
        imbalance['heat_loss'] = df.groupby('DeviceID')['heat_loss'].mean()
        df_out = df_out.merge(imbalance, on='DeviceID', how="outer")
        return df_out

    def output(self):
        try:
            df = self.read_Data()
            df['EM_Active Power (kW)'] = np.where((df['EM_Power Factor'] == 0), 0, df['EM_Active Power (kW)'])
            df['Mean_THD'] = df['Mean_THD'] / 100
            df['EM_THD Voltage'] = df['EM_THD Voltage'] / 100

            df.set_index('time', inplace=True)
            df_out = df.groupby(pd.Grouper(freq='30T')).apply(self.fun_new)
            res = list(zip(*df_out.index))
            df_out['Time'] = res[0]
            df_out['Time'] = df_out['Time'] + timedelta(minutes=30)

            energy = self.read_Data_energy(df)
            df_out = df_out.merge(energy, on=['Time', 'DeviceID'], how="outer")
            df_out['energy_kvah'] = df_out['energy'] / df_out['average_pf']
            df_out = df_out.drop('time', axis = 1)
            df_out = df_out.set_index('Time')
            df_out = df_out.fillna(0)
            # print(df_out)
            # df_out.index = df_out.index + timedelta(hours=5, minutes=30)
            print(self.DFDBClient.write_points(df_out, cg.WRITE_MEASUREMENT, tag_columns=['DeviceID']))
        except Exception as e:
            print(e)


# if __name__ == '__main__':
#     cat = PowerFactorLoss_KPI(0.99)
#     cat.output()