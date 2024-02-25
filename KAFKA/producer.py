# Code inspired from Confluent Cloud official examples library
# https://github.com/confluentinc/examples/blob/7.1.1-post/clients/cloud/python/producer.py

from confluent_kafka import Producer
import json
import ccloud_lib # Library not installed with pip but imported from ccloud_lib.py
import numpy as np
import time
import requests
from datetime import datetime

# Initialize configurations from "python.config" file
CONF = ccloud_lib.read_ccloud_config("python.config")
TOPIC = "LIME_V2"

# Create Producer instance
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
producer = Producer(producer_conf)

# Create topic if it doesn't already exist
ccloud_lib.create_topic(CONF, TOPIC)


delivered_records = 0

# Fonction de rappel pour la confirmation de la livraison
def acked(err, msg):
    global delivered_records
    # Delivery report handler called on successful or failed delivery of message
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

# Définir les paramètres de l'API Rapid
url = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"


try:
    while True:
        response = requests.get(url)
        response_json=response.content
        json_string =response_json.decode('utf-8')
        data_json = json.loads(json_string)

        record_key = "vélos disponibles"

        record_value=[]
        for i in range(0,len(data_json["data"]["stations"])):
            station_id = data_json["data"]["stations"][i]["station_id"]
            bikes_available = data_json["data"]["stations"][i]["num_bikes_available"]
            docks_available = data_json["data"]["stations"][i]["num_docks_available"]
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            record ={
                "time": current_time,
                "station_id": station_id,
                "bikes_available": bikes_available,
                "docks_available": docks_available
            }    
            # Convertir le dictionnaire en chaîne JSON pour l'envoi
            record_value = json.dumps(record)

            print(f"i={i},Il ya {bikes_available} velo_disponible et {docks_available} place_disponible dans la station {station_id} à {current_time}")

            #  Envoyer les données au topic Kafka
            producer.produce(
                TOPIC,
                key=str(record["station_id"]),
                value=record_value,
                on_delivery=acked
            )
            # p.poll() serves delivery reports (on_delivery)
            # from previous produce() calls thanks to acked callback
            producer.poll(0)
        time.sleep(60)
except KeyboardInterrupt:
    pass
finally:
     # S'assurer que tous les messages sont envoyés avant de terminer le script
    producer.flush() 