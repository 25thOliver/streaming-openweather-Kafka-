# OpenWeather → Kafka → Consumer pipeline

## Overview
- Source: OpenWeather API (polling)
- Broker: Apache Kafka (local dev)
- Topic(s):
  - `openweather.raw` — raw JSON feed from OpenWeather
- Consumers: (to be implemented) — e.g., process & store to MongoDB / Elasticsearch / S3
- Purpose: Real-time weather ingestion, downstream processing & analytics

## Local dev setup (Step 1)
1. Install Docker & Docker Compose
2. `docker-compose up -d`
3. `export OPENWEATHER_API_KEY=...`
4. Create topic: see repo README or run the create-topic command

## Next steps
- Step 2: implement poller/producer (Python or Java) that calls OpenWeather, produces messages to Kafka
- Step 3: build consumer that parses and stores to a document DB (MongoDB/Elasticsearch) and creates meaningful schema
- Step 4: add schema/validation + monitoring + CI

## Step 2 — Producer

- Language: Python
- Libraries: kafka-python, requests, python-dotenv
- Config in `.env`:
  - `OPENWEATHER_API_KEY`
  - `OPENWEATHER_CITY`
  - `KAFKA_BOOTSTRAP_SERVERS`
  - `KAFKA_TOPIC`
- Script: `openweather_producer.py`
  - Polls OpenWeather API every 30s
  - Produces JSON messages into `openweather.raw`

## Step 3 — Consumer

- Language: Python
- Libraries: kafka-python, pymongo
- Config in `.env`:
  - `MONGO_URI`
  - `MONGO_DB`
  - `MONGO_COLLECTION`
- Script: `openweather_consumer.py`
  - Subscribes to topic `openweather.raw`
  - Writes JSON messages into MongoDB collection `weather_data`

## Step 4 — Validation

We enforce data consistency at two levels:

1. **Python (Pydantic in consumer)**  
   - Validates fields: `city (str)`, `timestamp (int)`, `payload (dict)`.  
   - Skips invalid records and logs errors.

2. **MongoDB JSON Schema**  
   - Ensures every document has `city`, `timestamp`, and `payload.main/weather`.  
   - Rejects invalid documents even if consumer fails to validate.

This guarantees strong data integrity from Kafka → MongoDB.
