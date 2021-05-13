import sys
import json 
import time
import numpy as np
import paho.mqtt.client
import paho.mqtt.publish
import datetime as dt

def on_connect(client, userdata, flags, rc):
	print("Conectado Publicador de la cantidad de personas en la Sala") 

def main():
    client = paho.mqtt.client.Client("personasSalaPub",False)
    client.qos = 0
    client.on_connect = on_connect

    client.connect("localhost",1883)

    currentTime = dt.datetime.now().replace(second = 0, microsecond = 0)
    while True:
        cantidad_Personas = int(np.random.uniform(0,10))
        

        payload1 = {
            "cantPersonas_current_sala" : str(cantidad_Personas),
            "time_current" : str(currentTime)
        }
        client.publish("casa/sala/contador_personas", json.dumps(payload1), qos = 0)
      
        if(cantidad_Personas > 5):
            payload2 = {
                "sala_message" : "People Limit Exceeded",
                "time_current" : str(currentTime)
            }
            
            client.publish("casa/sala/contador_personas", json.dumps(payload2), qos = 0)
        
        currentTime += dt.timedelta(minutes = 1)
        time.sleep(1)

if (__name__ == "__main__"):
    main()
    sys.exit(0)