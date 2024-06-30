# production to kafka  

import os
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random

LONDON_COORDINATES = { 'latitude': 51.5074, 'longitude': -0.1278 }
BIRMINGHAM_COORDINATES = { 'latitude': 52.4862, 'longitude': -1.8904 }

# Calculate movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# Environment Variables for Configs
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", 'vehicle_data')
GPS_TOPIC = os.getenv("GPS_TOPIC", 'gps_data')
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", 'traffic_data')
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", 'weather_data')
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", 'emergency_data')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60)) # update frequency
    return start_time

def simulate_vehicle_movement():
    global start_location

    # move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # simulate actual road travel by incrementing random numbers
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(deviceId):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': deviceId,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'Porsche',
        'model': '911',
        'year': '2024',
        'fuelType': 'Gas'
    }

def simulate_journey(producer, deviceId):
    while True:
        vehicle_data = generate_vehicle_data(deviceId)
        print(vehicle_data)
        break

if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-ID-1')
    except KeyboardInterrupt:
        print("Simulation ended by the user")
    except Exception as e:
        print(f'Unexpected Error occurred')
        

