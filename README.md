# Kafka Event Generator

## Description

This repository contains a Python application that generates and sends events to a configured Kafka topic. It's designed for simplicity and quick setup, making it a useful tool for testing Kafka-based systems.

## Prerequisites

### Habilitar un consumidor de Kafka (opcional)

To monitor the generated events in real-time, set up a Kafka consumer in a terminal following the steps in the guide: [Kafka Configuration Guide](https://github.com/aleixcastellvi/tech-doc/blob/main/kafka-setup.md)

After the first configuration steps, access the kafka container with:

```bash
docker exec -it kafka-broker-1 bash
```

Verify the existence of `topic-test`. If it doesn’t exist, create it as per the guide.

Now start the kafka consumer with:

```bash
kafka-console-consumer.sh --topic topic-test --bootstrap-server kafka-broker-1:9092 --from-beginning
```

Your terminal is now ready to display events transmitted to the `topic-test` topic.

## Setting Up the Application

1. Create a Virtual Environment

Conda (Mac OSX):

```bash
conda create -n kafka-env python=3.12

conda activate kafka-env
```

2. Install dependencies

```bash
pip install -r requirements.txt
```

3. Run the app

```bash
python streaming-events/kafka-event-producer.py
```

## What to Expect

- Events are generated and sent to the kafka topic `topic-test` every 3 seconds.
- If you have configured a consumer on your terminal, while the kafka consumer is running, you will see these events appear in real-time on your configured terminal.