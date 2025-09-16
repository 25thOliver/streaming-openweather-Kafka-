import os
import time
import json
import logging
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s %(message)s]")
load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = os.getenv("OPENWEATHER_CITY", "Nairobi")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "openweather.raw")

URL =f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

def create_producer():
	return KafkaProducer(
		bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
		value_serializer=lambda v: json.dumps(v).encode("utf-8"),
		linger_ms=10,
		retries=5,
	)

def fetch_weather():
	try:
	   resp = requests.get(URL, timeout=5)
	   resp.raise_for_status()
	   return resp.json()
	except Exception as e:
	   logging.error(f"Error fetching weather data: {e}")
	   return None

def main():
	producer = create_producer()
	logging.info(f"Producer connected to {KAFKA_BOOTSTRAP_SERVERS}, topic={TOPIC}")

	while True:
		data = fetch_weather()
		if data:
			message = {
				"city": CITY,
				"timestamp": int(time.time()),
				"payload": data,

			}
			producer.send(TOPIC, message)
			logging.info(f"Produced weather update for {CITY}")
		time.sleep(30)

if __name__ == "__main__":
	main()
