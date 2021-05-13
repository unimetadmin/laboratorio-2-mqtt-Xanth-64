import sys
import json 
import time
import numpy as np
import paho.mqtt.client
import paho.mqtt.publish
import datetime as dt
def on_connect(client, userdata, flags, rc):
	print("Conectado Publicador de la Nevera")

def main():
    client = paho.mqtt.client.Client("neveraPub",False)
    client.qos = 0
    client.on_connect = on_connect
    client.connect(host='localhost', port=1883)

    meanTemp = 10
    sdTemp = 2
    interruptor = 1
    currentTime = dt.datetime.now().replace(microsecond = 0, second = 0) 
    while True:
        currentTemp = float(np.random.normal(meanTemp,sdTemp))
        payload1 = {
            "temperatura_current_freezer" : str(currentTemp),
            "time_current" : str(currentTime)
        }
        
        if(interruptor):
            currentIce = int(np.random.uniform(0,10)) 
            payload2 = {
                "ice_current_freezer": str(currentIce),
                "time_current" : str(currentTime)
            }
            interruptor = 0 
            client.publish("casa/cocina/temperatura_nevera", json.dumps(payload2), qos = 0)
        else: interruptor = 1

        client.publish("casa/cocina/temperatura_nevera", json.dumps(payload1), qos = 0)
        
        currentTime += dt.timedelta(minutes = 5)
        time.sleep(1)
if (__name__ == "__main__"):
    main()
    sys.exit(0)