import json
import threading
from kafka import KafkaConsumer
import redis

def start_kafka_consumer():
    print("[Kafka Consumer] Initializing background listener thread...", flush=True)
    
    # 1. Connect to Redis cache instance
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # 2. Connect to Kafka broker
    consumer = KafkaConsumer(
        'driver-telemetry',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='matching-service-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("[Kafka Consumer] Successfully connected to Kafka & Redis. Listening for driver coordinates...", flush=True)
    
    # 3. Continuous processing loop
    for message in consumer:
        try:
            data = message.value
            driver_id = data.get("driverId")
            lat = data.get("latitude")
            lon = data.get("longitude")
            
            if driver_id and lat and lon:
                # Store coordinates into a Redis Geospatial Index named "drivers:locations"
                # Redis expects arguments in the exact order: (index_name, longitude, latitude, member_key)
                r.geoadd("drivers:locations", (lon, lat, driver_id))
                print(f"[Redis Indexer] Updated location for {driver_id} -> ({lat}, {lon})", flush=True)
                
        except Exception as e:
            print(f"[Kafka Consumer Error] Failed to process message stream record: {e}", flush=True)

def run_consumer_in_background():
    # Execute the messaging loop inside a separate background thread to keep FastAPI unblocked
    thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    thread.start()
