import sys
import paho.mqtt.client
import psycopg2
import json 
def on_connect(client, userdata, flags, rc):
    print("Conectado Subscritor de la sala")
    client.subscribe(topic="casa/sala/#")

def on_message(client, userdata, message):
    connection = psycopg2.connect(user = "tqpwzlrl", password = "pydt0BcWXopyIahMpfcB7qbMwrx9qbdw", host = "queenie.db.elephantsql.com", database = "tqpwzlrl")
    cursor = connection.cursor()
    res = json.loads(message.payload)
        
    print(res)
    if (res.get("cantPersonas_current_sala")):
        cursor.execute(""" CREATE TABLE IF NOT EXISTS personasSala
(ID SERIAL PRIMARY KEY NOT NULL,
personas INT NOT NULL,
update_time TIMESTAMP NOT NULL);""")
        connection.commit()

        cursor.execute("""INSERT INTO personasSala (personas,update_time) 
VALUES ( %(pers)s,%(time)s );""",{"pers": res["cantPersonas_current_sala"], "time": res["time_current"]})

        connection.commit()

    if (res.get("Temperature")):
        cursor.execute(""" CREATE TABLE IF NOT EXISTS temperaturasAlexa
(ID SERIAL PRIMARY KEY NOT NULL,
temperature REAL NOT NULL,
description VARCHAR(255) NOT NULL,
update_time TIMESTAMP NOT NULL);""")

        connection.commit()

        cursor.execute("""INSERT INTO temperaturasAlexa (temperature,update_time, description) 
        VALUES ( %(temp)s,%(time)s , %(desc)s);""",{"temp": res["Temperature"], "time": res["current_time"], "desc": res["Description"]})

        connection.commit()

    if (res.get("sala_message")):
        cursor.execute(""" CREATE TABLE IF NOT EXISTS mensajes_sala
(ID SERIAL PRIMARY KEY NOT NULL,
message TEXT NOT NULL,
update_time TIMESTAMP NOT NULL);""")

        connection.commit()

        cursor.execute("""INSERT INTO mensajes_sala (message,update_time) 
        VALUES ( %(msg)s,%(time)s );""",{"msg": res["sala_message"], "time": res["time_current"]})

        connection.commit()


def main():
    client = paho.mqtt.client.Client(client_id='salaSub', clean_session=False)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(host='localhost', port=1883)
    client.loop_forever()


if __name__ == "__main__":
    main()
    sys.exit(0)