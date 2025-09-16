import os
import json
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "openweather.raw")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGODB", "openweather")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "weather_data")

def create_consumer():
	return KafkaConsumer(
		TOPIC,
		bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
		value_deserializer=lambda v: json.loads(v.decode("utf-8")),
		auto_offset_reset="earliest",
		enable_auto_commit=True,
		group_id="openweather-consumer-group",
	)

def main():
	consumer = create_consumer()
	client = MongoClient(MONGO_URI)
	collection = client[MONGO_DB][MONGO_COLLECTION]

	logging.info("Consumer subscribed to topic={TOPIC}, writing to {MONGO_DB}.{MONGO_COLLECTION}")
	
	for message in consumer:
		data = message.value
		collection.insert_one(data)
		logging.info(f"Inserted weather record for {data.get('city')} into MongoDB")

if __name__ == "__main__":
	main() 
