from kafka import KafkaProducer
import json
import time

import requests

# Configuration
API_KEY = '68a331bbd87eefddf393ce4d32db7509'  # Clé API
CITIES = ['Paris', 'London', 'Tokyo']  # Villes cibles


# Fonction pour récupérer les données météo
def get_weather_data(city):
    weather = []
    for i in city:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={i}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        weather.append(response.json() if response.status_code == 200 else None)
    return weather

get_weather_data(CITIES)

# Configuration
KAFKA_SERVER = 'localhost:9092'

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Envoi des données en continu toutes les minutes
while True:
    data = get_weather_data(CITIES)  # Récupère les données météo pour les villes spécifiées
    producer.send("tp-meteo", value=data)
    print("Données envoyées :", data)
    
    # Attendre 60 secondes avant le prochain envoi
    time.sleep(60)
