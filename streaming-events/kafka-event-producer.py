import random
import time
import uuid
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Constants
KAFKA_URI = "localhost:9092"
TOPIC = "topic-test"

def setup_kafka_producer():
    """
    Set up the Kafka producer
    """
    return KafkaProducer(
        bootstrap_servers = KAFKA_URI, 
        api_version = '0.9', 
        value_serializer = lambda v: json.dumps(v).encode('utf-8'))

def send_to_kafka(kafka_producer, topic, event_data):
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

def generate_event():
    """
    Generate a random event
    """
    return {
        "event_id": str(uuid.uuid4()),
        "value": round(random.uniform(0, 100), 2),
        "timestamp": int(time.time() * 1000),
    }

def main():
    try:
        event_sent = 1
        kafka_producer = setup_kafka_producer()

        while True:
            event = generate_event()

            # Send event to Kafka
            send_to_kafka(kafka_producer, TOPIC, event)
            print(f"Event #{event_sent} sent to the Topic {TOPIC}")
            
            event_sent += 1
            time.sleep(3)

    except KeyboardInterrupt:
        print("Interrupt detected. Stopping event generation")

    finally:
        print("Closing Kafka producer")
        kafka_producer.close()

if __name__ == "__main__":
    main()
