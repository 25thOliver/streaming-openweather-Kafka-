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
