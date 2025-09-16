import os
import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "openweather.raw")

AFRICAN_CAPITALS = [
    {"city": "Algiers", "lat": 36.7538, "lon": 3.0588},
    {"city": "Luanda", "lat": -8.8390, "lon": 13.2894},
    {"city": "Porto-Novo", "lat": 6.4969, "lon": 2.6289},
    {"city": "Gaborone", "lat": -24.6282, "lon": 25.9231},
    {"city": "Ouagadougou", "lat": 12.3714, "lon": -1.5197},
    {"city": "Bujumbura", "lat": -3.3614, "lon": 29.3599},
    {"city": "Yaoundé", "lat": 3.8480, "lon": 11.5021},
    {"city": "Bangui", "lat": 4.3947, "lon": 18.5582},
    {"city": "N'Djamena", "lat": 12.1348, "lon": 15.0557},
    {"city": "Moroni", "lat": -11.7172, "lon": 43.2473},
    {"city": "Kinshasa", "lat": -4.4419, "lon": 15.2663},
    {"city": "Brazzaville", "lat": -4.2634, "lon": 15.2429},
    {"city": "Djibouti", "lat": 11.5721, "lon": 43.1456},
    {"city": "Cairo", "lat": 30.0444, "lon": 31.2357},
    {"city": "Malabo", "lat": 3.7500, "lon": 8.7833},
    {"city": "Asmara", "lat": 15.3229, "lon": 38.9251},
    {"city": "Addis Ababa", "lat": 9.0330, "lon": 38.7469},
    {"city": "Libreville", "lat": 0.4162, "lon": 9.4673},
    {"city": "Banjul", "lat": 13.4549, "lon": -16.5790},
    {"city": "Accra", "lat": 5.5600, "lon": -0.2057},
    {"city": "Conakry", "lat": 9.6412, "lon": -13.5784},
    {"city": "Bissau", "lat": 11.8636, "lon": -15.5977},
    {"city": "Yamoussoukro", "lat": 6.8276, "lon": -5.2893},
    {"city": "Nairobi", "lat": -1.2921, "lon": 36.8219},
    {"city": "Maseru", "lat": -29.3151, "lon": 27.4869},
    {"city": "Monrovia", "lat": 6.3156, "lon": -10.8014},
    {"city": "Tripoli", "lat": 32.8872, "lon": 13.1913},
    {"city": "Antananarivo", "lat": -18.8792, "lon": 47.5079},
    {"city": "Lilongwe", "lat": -13.9626, "lon": 33.7741},
    {"city": "Bamako", "lat": 12.6392, "lon": -8.0029},
    {"city": "Nouakchott", "lat": 18.0790, "lon": -15.9650},
    {"city": "Port Louis", "lat": -20.1609, "lon": 57.5012},
    {"city": "Rabat", "lat": 34.0209, "lon": -6.8416},
    {"city": "Maputo", "lat": -25.9692, "lon": 32.5732},
    {"city": "Windhoek", "lat": -22.5597, "lon": 17.0832},
    {"city": "Niamey", "lat": 13.5128, "lon": 2.1120},
    {"city": "Abuja", "lat": 9.0579, "lon": 7.4951},
    {"city": "Kigali", "lat": -1.9536, "lon": 30.0606},
    {"city": "São Tomé", "lat": 0.3365, "lon": 6.7273},
    {"city": "Dakar", "lat": 14.6928, "lon": -17.4467},
    {"city": "Victoria", "lat": -4.6167, "lon": 55.4500},
    {"city": "Freetown", "lat": 8.4657, "lon": -13.2317},
    {"city": "Mogadishu", "lat": 2.0469, "lon": 45.3182},
    {"city": "Pretoria", "lat": -25.7461, "lon": 28.1881},  # SA’s administrative capital
    {"city": "Khartoum", "lat": 15.5007, "lon": 32.5599},
    {"city": "Mbabane", "lat": -26.3054, "lon": 31.1367},
    {"city": "Dodoma", "lat": -6.1630, "lon": 35.7516},
    {"city": "Lomé", "lat": 6.1725, "lon": 1.2314},
    {"city": "Tunis", "lat": 36.8065, "lon": 10.1815},
    {"city": "Kampala", "lat": 0.3476, "lon": 32.5825},
    {"city": "Lusaka", "lat": -15.3875, "lon": 28.3228},
    {"city": "Harare", "lat": -17.8292, "lon": 31.0522},
]


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def fetch_weather(city_info):
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={city_info['lat']}&lon={city_info['lon']}&appid={OPENWEATHER_API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            "city": city_info["city"],
            "timestamp": int(time.time()),
            "payload": data,
        }
    else:
        print(f"Failed to fetch {city_info['city']}: {response.status_code}")
        return None

def main():
    while True:
        for city_info in AFRICAN_CAPITALS:
            record = fetch_weather(city_info)
            if record:
                producer.send(TOPIC, value=record)
                print(f"Published weather data for {city_info['city']}")
        time.sleep(300)  # wait 5 minutes before the next batch

if __name__ == "__main__":
    main()
