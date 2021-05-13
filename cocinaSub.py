import sys
import paho.mqtt.client
import psycopg2
import json 
def on_connect(client, userdata, flags, rc):
    print("Conectado Subscritor de la Cocina")
    client.subscribe(topic="casa/cocina/#")

def on_message(client, userdata, message):
    try:
        connection = psycopg2.connect(user = "tqpwzlrl", password = "pydt0BcWXopyIahMpfcB7qbMwrx9qbdw", host = "queenie.db.elephantsql.com", database = "tqpwzlrl")
        cursor = connection.cursor()
        res = json.loads(message.payload)
        
        print(res)
        if (res.get("temperature_current_pot")):
            cursor.execute(""" CREATE TABLE IF NOT EXISTS temperaturasOlla
(ID SERIAL PRIMARY KEY NOT NULL,
temperature REAL NOT NULL,
update_time TIMESTAMP NOT NULL);""")
            connection.commit()

            cursor.execute("""INSERT INTO temperaturasOlla (temperature,update_time) 
VALUES ( %(temp)s,%(time)s );""",{"temp": res["temperature_current_pot"], "time": res["time_current"]})

            connection.commit()

        if (res.get("temperatura_current_freezer")):
            cursor.execute(""" CREATE TABLE IF NOT EXISTS temperaturasNevera
(ID SERIAL PRIMARY KEY NOT NULL,
temperature REAL NOT NULL,
update_time TIMESTAMP NOT NULL);""")

            connection.commit()

            cursor.execute("""INSERT INTO temperaturasNevera (temperature,update_time) 
            VALUES ( %(temp)s,%(time)s );""",{"temp": res["temperatura_current_freezer"], "time": res["time_current"]})
    
            connection.commit()
        if (res.get("ice_current_freezer")):
            cursor.execute(""" CREATE TABLE IF NOT EXISTS hielosNevera
(ID SERIAL PRIMARY KEY NOT NULL,
hielo REAL NOT NULL,
update_time TIMESTAMP NOT NULL);""")

            connection.commit()

            cursor.execute("""INSERT INTO hielosNevera (hielo,update_time) 
            VALUES ( %(ice)s,%(time)s );""",{"ice": res["ice_current_freezer"], "time": res["time_current"]})

            connection.commit()
        if (res.get("olla_message")):
            cursor.execute(""" CREATE TABLE IF NOT EXISTS mensajesOlla
(ID SERIAL PRIMARY KEY NOT NULL,
message TEXT NOT NULL,
update_time TIMESTAMP NOT NULL);""")
            
            connection.commit()

            cursor.execute("""INSERT INTO mensajesOlla (message,update_time) 
            VALUES ( %(msg)s,%(time)s );""",{"msg": res["olla_message"], "time": res["time_current"]})

            connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error Coneccting to the BD: ", error)
    finally:
        if connection:
            cursor.close()
            connection.close()

def main():
        client = paho.mqtt.client.Client(client_id='cocinaSub', clean_session=False)
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(host='localhost', port=1883)
        client.loop_forever()


if __name__ == "__main__":
    main()
    sys.exit(0)