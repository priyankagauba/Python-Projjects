import datetime
import pandas as pd
from influxdb import *
from pytz import timezone

import Config as cs


class Data30Min(object):

    # ------------------------- initializing the member variables of the class
    def __init__(self, host, port, database, measurement):
        self.host = host
        self.port = port
        self.database = database
        self.measurement = measurement
        self.influxDBClient = self.connectInfluxdbClient()

    def __call__(self):
        self.move_data(self.influxDBClient, cs.TARGET_MEASUREMENT, 1440)
        print('Inside the call method')

    # -------------------------- getting the instant of influxdbClient
    def connectInfluxdbClient(self):
        return InfluxDBClient(host=self.host, port=self.port, database=self.database)

    # -------------------------- delete data after certain intervals
    def move_data(self, con_obj, target_measurement, duration):
        try:
            query_delete = 'delete from ' + target_measurement + ' where time < now() - ' + str(duration) + 'm'
            result_delete = con_obj.query(query_delete)
            print(result_delete)
        except Exception as e:
            print('Exception in InfluxDB_30_min:'+str(e))