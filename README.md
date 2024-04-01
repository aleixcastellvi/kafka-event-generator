# Kafka Event Generator

## Description

This repository contains multiple event formats to be sent to the configured topic.

Below are the different available applications for sending events:

* [Streaming Banking Event App](/streaming-banking-event/README.md)

## Running the app

Clone this repository to your local machine and navigate to the project directory.

#### Setting up a Virtual Environment

* **Conda (Mac OSX)**

```bash
conda create -n kafka-event-generator python=3.10
```

```bash
conda activate kafka-event-generator
```

Install dependencies with pip

```bash
pip install -r requirements.txt
```

Run the app

```bash
python streaming-banking-event/banking-app.py
```