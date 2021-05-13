import sys
import json 
import time
import numpy as np
import paho.mqtt.client
import paho.mqtt.publish
import datetime as dt

def on_connect(client, userdata, flags, rc):
	print("Conectado Publicador de la Tanque de Agua") 

def main():
    client = paho.mqtt.client.Client("tanquePub",False)
    client.qos = 0
    client.on_connect = on_connect

    client.connect("localhost",1883)


    meanPercentageLoss = 10
    meanPercentageGain = 20 
    commonStandardDeviation = 5 
    maxWaterVolume = 100
    waterVolume = maxWaterVolume
    increaseCount = 0 
    
    currentTime = dt.datetime.now().replace(second=0, microsecond=0)
    while True:
        lossPercentage = float(np.random.normal(meanPercentageLoss,commonStandardDeviation))
        
        waterVolume = waterVolume - (maxWaterVolume * lossPercentage / 100 )
        increaseCount += 1

        if (waterVolume < 0): waterVolume = 0
        if(increaseCount == 3):
            gainPercentage = float(np.random.normal(meanPercentageGain,commonStandardDeviation))
            increaseCount = 0
            waterVolume = waterVolume + (maxWaterVolume * gainPercentage / 100 )
            print("Increasing water level at ", str(currentTime))
        if (waterVolume > 100): waterVolume = 100

        payload1 = {
            "waterVolume_current_banio" : str(waterVolume),
            "time_current" : str(currentTime)
        }
        print(payload1)

        
        client.publish("casa/banio/nivel_tanque", json.dumps(payload1), qos = 0)
        if(waterVolume <= 50):
            payload2 = {
                "tanque_message" : "Warning, the water tank has half or less than half of its total capacity.",
                "time_current" : str(currentTime)
            }
            client.publish("casa/banio/nivel_tanque", json.dumps(payload2), qos = 0)
            print(payload2)
        if(waterVolume ==  0):
            payload3= {
                "tanque_message" : "DANGER, The water tank is completely empty.",
                "time_current" : str(currentTime)
            }
            client.publish("casa/banio/nivel_tanque", json.dumps(payload3), qos = 0)
            print(payload3)

        currentTime += dt.timedelta(minutes = 10)
        time.sleep(1)


if (__name__ == "__main__"):
    main()
    sys.exit(0)