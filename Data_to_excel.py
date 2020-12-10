import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime



class Data_To_Excel():

    def __init__(self):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
    
    def __call__(self):
        self.output()
    
    def read_EM_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select * from ' + cg.TARGET_MEASUREMENT  + ' where time > now() - 1d '
        df1 = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        return df1
    
    def read_CT_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select * from ' + cg.CT_MEASUREMENT  + ' where time > now() - 1d '
        df2 = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        return df2
    
    def output(self):
        try:
            df1 = self.read_EM_Data()
            df2 = self.read_CT_Data()
            d = pd.datetime.now().date()
            d = str(d)
            d = d.replace ("-", "_")
            path = '//atgrzsw3571.avl01.avlcorp.lan/ITCTestbedData/data/EM_'+ str(d)+'.xlsx'
            with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
                df1.to_excel(writer, sheet_name='EM', index=False)
                df2.to_excel(writer, sheet_name='CT', index=False)
                writer.save()
        except Exception as e:
            print('Exception in Data to excel:'+str(e))