import random
import time
import json
import kafka
from confluent_kafka import KafkaError
from uuid import uuid4


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

songs = {
    "Drake": ["One Dance", "Rich Baby Daddy", "IDGAF"],
    "Taylor Swift": ['Cruel Summer', 'Lover', 'Anti-Hero'],
    "Ariana Grande": ["yes, and?", "we can't be friends", "the boy is mine"]
}

# Generate data and send to kafka topic
try:
    event_sent = 1

    kafka_producer = setup_kafka_producer()

    while True:
        artist = random.choice(list(songs.keys()))
        song = random.choice(songs[artist])
        
        event = {
            'user': "user-" + str(uuid4()),
            "timestamp": int(time.time() * 1000),
            "artist": artist,
            "song": song
        }

        send_to_kafka(TOPIC, event)

        print(f"Event #{event_sent} sent to the Topic {TOPIC}")
        
        event_sent += 1
        time.sleep(3)

except KeyboardInterrupt:
    print("Interrupt detected. Stopping event generation.")

finally:
    kafka_producer.close()