import schedule as sd
import Config as conf
from Energy_24 import ENERGY24
from Model_1 import Model1
from model__retrain import Model_Retrain
from NeuralN import Model2
import time

if __name__ == '__main__':
    try:
        host = conf.INFLUX_DB_IP
        port = conf.INFLUX_DB_PORT
        database = conf.INFLUX_DB
        measurement = conf.TARGET_MEASUREMENT

        # Energy24
        energy_24 = ENERGY24()
        sd.every(20).minutes.do(energy_24)

        # model_retrain
        model_retrain_obj = Model_Retrain()
        sd.every(2880).minutes.do(model_retrain_obj)

        # model1
        model1_obj = Model1()
        sd.every(30).minutes.do(model1_obj)

        # model2
        model2_obj = Model2()
        sd.every(30).minutes.do(model1_obj)


        while True:
            sd.run_pending()

    except Exception as e:
        print(e)

