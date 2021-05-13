import sys
import json 
import time
import numpy as np
import paho.mqtt.client
import paho.mqtt.publish
import datetime as dt
import requests
def on_connect(client, userdata, flags, rc):
	print("Conectado Publicador de la Temperatura en la Sala por Alexa") 

def main():
    client = paho.mqtt.client.Client("alexaPub",False)
    client.qos = 0
    client.on_connect = on_connect

    client.connect("localhost",1883)


    api_key = "ddd2791e033a02396446568972901a0f" #Esto es super inseguro pero creo que valido

    currentTime = dt.datetime.now().replace(second=0, microsecond=0)
    req = requests.get(f"http://api.openweathermap.org/data/2.5/weather?q=Caracas&appid={api_key}")
    while (req.status_code != 200):
        print("Retrying Conecction to API")
        req = requests.get(f"http://api.openweathermap.org/data/2.5/weather?q=Caracas&appid={api_key}")
    informationPayload = req.json()
    description = informationPayload["weather"][0]["description"]
    tempBase = float(informationPayload["main"]["temp"])
    city = informationPayload["name"]
    while True:       
        payload = {
            "City": city,
            "Temperature" : (tempBase + float(np.random.normal(0,1))),
            "Description" :  description,
            "current_time" : str(currentTime)
            }
        client.publish("casa/sala/alexa_echo", json.dumps(payload), qos = 0)
        currentTime += dt.timedelta(minutes = 1)
        #print(payload)
        time.sleep(1) 

if (__name__ == "__main__"):
    main()
    sys.exit(0)