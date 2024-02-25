# Example written based on the official 
# Confluent Kakfa Get started guide https://github.com/confluentinc/examples/blob/7.1.1-post/clients/cloud/python/consumer.py

from confluent_kafka import Consumer
import json
import ccloud_lib
import time

# Initialize configurations from "python.config" file
CONF = ccloud_lib.read_ccloud_config("python.config")
TOPIC = "LIME_V2" 

# Create Consumer instance
# 'auto.offset.reset=earliest' to start reading from the beginning of the
# topic if no committed offsets exist
consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
consumer_conf['group.id'] = 'Lime_group'
consumer_conf['auto.offset.reset'] = 'earliest' # This means that you will consume latest messages that your script haven't consumed yet!
consumer = Consumer(consumer_conf)

# Subscribe to topic
consumer.subscribe([TOPIC])

# Process messages
try:
    while True:
        msg = consumer.poll(10) # Search for all non-consumed events. It times out after X seconds
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:
            # Check for Kafka message
            record_key = msg.key()
            record_value = msg.value()
            data = json.loads(record_value)
            id=data["station_id"]
            date=data["time"]
            velo_disponible=data["bikes_available"]
            place_disponible=data["docks_available"]
            print(f"Il ya {velo_disponible} velo_disponible et {place_disponible} place_disponible dans la station {id} à {date}")
            time.sleep(0.1) # Wait half a second
except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    consumer.close()
