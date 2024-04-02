import random
import time
import uuid
import json
import datetime
import kafka
from confluent_kafka import KafkaError


# Constants
KAFKA_URI = "localhost:9092"
TOPIC = "streaming-event"

def setup_kafka_producer():
    """
    Set up the Kafka producer
    """
    return kafka.KafkaProducer(
        bootstrap_servers = KAFKA_URI, 
        api_version = '0.9', 
        value_serializer = lambda v: json.dumps(v).encode('utf-8'))

def send_to_kafka(topic, event_data):
    """
    Sends event data to the Kafka producer

    Params:
        topic (str): The Kafka topic to which the event should be sent
        event_data (dict): The event data to be sent
    """
    try:
        future = kafka_producer.send(topic, event_data)
        kafka_producer.flush()  # Don't save data in a buffer and send immediately
        return future
    
    except KafkaError as e:
        print(f"Error sending to Kafka: {e}")
        return None

event_sended = 1

# Generate data and send to kafka topic
try:
    while True:

        kafka_producer = setup_kafka_producer()

        ts_event = int(time.time() * 1000) # timestamp in milliseconds
        dt_event = datetime.datetime.fromtimestamp(ts_event/1000)

        event = {
            "_id": str(uuid.uuid4()),
            "timestamp": ts_event, 
            "dataitem": str(dt_event),
            "user_id": "user" + str(random.randint(1, 50))
        }

        send_to_kafka(TOPIC, event)

        print(f"Event #{event_sended} sended to the Topic {TOPIC}")
        
        event_sended += 1
        time.sleep(3)

except KeyboardInterrupt:
    kafka_producer.close()