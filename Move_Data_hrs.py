import datetime
import pandas as pd
from influxdb import *
from pytz import timezone

import Config as cs


class Move1hrs(object):

    # ------------------------- initializing the member variables of the class
    def __init__(self, host, port, database, measurement):
        self.host = host
        self.port = port
        self.database = database
        self.measurement = measurement
        self.influxDBClient = self.connectInfluxdbClient()

    def __call__(self):
        self.move_data(self.influxDBClient, cs.SOURCE_MEASUREMENT, cs.TARGET_MEASUREMENT, 240)
        print('Inside the call method')

    # -------------------------- getting the instant of influxdbClient
    def connectInfluxdbClient(self):
        return InfluxDBClient(host=self.host, port=self.port, database=self.database)

    def move_data(self, con_obj, source_measurement, target_measurement, duration):
        try:
            # query_move = "select * into "+source_measurement+" from "+target_measurement+ " where time < now() and time > now() - "+str(duration)+"m group by *"
            # query_move = "select * into " + source_measurement + " from " + target_measurement + " where time < now() and time > now() - " + str(
            #     duration) + "m group by *"
            query_move = "select * into "+source_measurement+" from "+target_measurement+ " group by *"
            result_move = con_obj.query(query_move)
            print(result_move)
        except Exception as e:
            print('Exception in Move_Data_hrs'+str(e))


