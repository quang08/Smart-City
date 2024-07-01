# production to kafka  

import os
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import time
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

random.seed(42) # ensures that the vehicle movement simulation, the data generated for Kafka, and all other random-based operations produce the same results each time the script is executed. Good for testing
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

def get_next_time(): #  Updates and returns the next timestamp for vehicle data generation, with a random update frequency between 30 to 60 seconds.
    # generate a sequence of timestamps that simulate periodic updates
    global start_time
    start_time += timedelta(seconds=random.randint(30,60)) # update frequency
    return start_time

def generate_gps_data(deviceId, timestamp, vehicle_type='private'):
    return {
        "id": uuid.uuid4(),
        "deviceId": deviceId,
        "timestamp": timestamp,
        "speed": random.uniform(0, 40), #km/h
        "direction": "North-East",
        "vehicle_type": vehicle_type
    }

def generate_traffic_camera_data(deviceId, timestamp,location, camera_id):
    return {
        "id": uuid.uuid4(),
        "deviceId": deviceId,
        "cameraId": camera_id,
        "location": location,
        "timestamp": timestamp,
        "snapshot": "Base64EncodedString" # maybe s3 encoded image url 
    }

def generate_weather_data(deviceId, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "deviceId": deviceId,
        "location": location,
        "timestamp": timestamp,
        "temperature": random.uniform(-5, 26),
        "weather_condition": random.choice(["Sunny", "Cloudy", "Rain", "Snow"]),
        "precipitation": random.uniform(0, 25),
        "windSpeed": random.uniform(0, 100),
        "humidity": random.uniform(0, 100), # %
        "airQualityIndex": random.uniform(0, 500),
    }

def generate_emergency_incident_data(deviceId, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "deviceId": deviceId,
        "incidentId": uuid.uuid4(),
        "type": random.choice(["Accident", "Fire", "Medical", "Police", "None"]),
        "timestamp": timestamp,
        "location": location,
        "status": random.choice(["Active", "Resolved"]),
        "description": "Description of the incident"
    }


def simulate_vehicle_movement():
    global start_location

    # move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # simulate actual road travel by incrementing random float numbers so that it won't increment on the same number
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

def json_serializer(obj): # convert specific objects into JSON serializable formats.
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of Type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg): # callback used to handle delivery reports from Kafka,
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'), # data obj convert to json string, then ensures UUIDs convert to strings, then encode to turn JSON string to byte string by Kafka standard 
        on_delivery=delivery_report
    ) 

def simulate_journey(producer, deviceId): # Continuously generates vehicle data using generate_vehicle_data() and sends it to Kafka
    while True:
        vehicle_data = generate_vehicle_data(deviceId)
        gps_data = generate_gps_data(deviceId, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(deviceId, vehicle_data['timestamp'], vehicle_data['location'], 'Sony-Cam1')
        weather_data = generate_weather_data(deviceId, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(deviceId, vehicle_data['timestamp'], vehicle_data['location'])

        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude'] and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print('Vehicle has reached Birmingham. Simulation ending...')
            break

        # write into Kafka
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        # Flush the producer periodically: Ensuring that all messages produced to Kafka are actually sent and acknowledged by the Kafka brokers
        producer.flush()

        # Simulate a delay
        time.sleep(5)

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
        print(f'Unexpected error occurred: {e}')
        

