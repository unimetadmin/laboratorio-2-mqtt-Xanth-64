import sys
import json 
import time
import numpy as np
import paho.mqtt.client
import paho.mqtt.publish
import datetime as dt

def on_connect(client, userdata, flags, rc):
	print("Conectado Publicador de la Olla")

def main():
    client = paho.mqtt.client.Client("ollaPub",False)
    client.qos = 0
    client.on_connect = on_connect

    client.connect(host='localhost', port=1883)
    currentTime = dt.datetime.now().replace(microsecond = 0)
    while True:
        temp = float(np.random.uniform(0,150))
        
        payload1 = {
                "temperature_current_pot" : str(temp),
                "time_current" : str(currentTime)
            }
        if (temp >= 100):
            payload2 = {
                "olla_message" : "Water is Boiling",
                "time_current" : str(currentTime)
            }
        else:
            payload2 = None
        client.publish("casa/cocina/temperatura_olla", json.dumps(payload1), qos = 0)
        
        if(payload2):
            client.publish("casa/cocina/temperatura_olla", json.dumps(payload2), qos = 0)
        currentTime += dt.timedelta(seconds = 1)
        time.sleep(1)
        
if (__name__ == "__main__"):
    main()
    sys.exit(0)