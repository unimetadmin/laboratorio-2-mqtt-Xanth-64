import sys
import paho.mqtt.client
import psycopg2
import json 
def on_connect(client, userdata, flags, rc):
    print("Conectado Subscritor del baño")
    client.subscribe(topic="casa/banio/#")

def on_message(client, userdata, message):
    try:
        connection = psycopg2.connect(user = "tqpwzlrl", password = "pydt0BcWXopyIahMpfcB7qbMwrx9qbdw", host = "queenie.db.elephantsql.com", database = "tqpwzlrl")
        cursor = connection.cursor()
        res = json.loads(message.payload)
        
        print(res)
        if (res.get("waterVolume_current_banio")):
            cursor.execute(""" CREATE TABLE IF NOT EXISTS volumen_agua_tanque
(ID SERIAL PRIMARY KEY NOT NULL,
water_volume REAL NOT NULL,
update_time TIMESTAMP NOT NULL);""")
            connection.commit()

            cursor.execute("""INSERT INTO volumen_agua_tanque (water_volume,update_time) 
VALUES ( %(vol)s,%(time)s );""",{"vol": res["waterVolume_current_banio"], "time": res["time_current"]})

            connection.commit()

        if (res.get("tanque_message")):        
            cursor.execute(""" CREATE TABLE IF NOT EXISTS mensajes_tanque
(ID SERIAL PRIMARY KEY NOT NULL,
message TEXT NOT NULL,
update_time TIMESTAMP NOT NULL);""")
            connection.commit()

            cursor.execute("""INSERT INTO mensajes_tanque (message,update_time) 
VALUES ( %(msg)s,%(time)s );""",{"msg": res["tanque_message"], "time": res["time_current"]})

            connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error Coneccting to the BD: ", error)
    finally:
        if connection:
            cursor.close()
            connection.close()

def main():
    client = paho.mqtt.client.Client(client_id='banioSub', clean_session=False)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(host='localhost', port=1883)
    client.loop_forever()


if __name__ == "__main__":
    main()
    sys.exit(0)