cat << 'EOF' > setup_python_service.sh
#!/bin/bash

# Navigate to the target python service folder
TARGET_DIR="matching-service-python"
mkdir -p $TARGET_DIR/app

# 1. Create requirements.txt
cat << 'INNER_EOF' > $TARGET_DIR/requirements.txt
fastapi==0.110.0
uvicorn==0.28.0
kafka-python-ng==2.2.2
redis==5.0.3
INNER_EOF

# 2. Create consumer.py (Kafka Engine -> Redis Geospatial Indexer)
cat << 'INNER_EOF' > $TARGET_DIR/app/consumer.py
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
INNER_EOF

# 3. Create main.py (FastAPI Routing Server)
cat << 'INNER_EOF' > $TARGET_DIR/app/main.py
from fastapi import FastAPI, HTTPException
import redis
from app.consumer import run_consumer_in_background

app = FastAPI(title="Real-Time Ride Matching Service")

# Initialize connection pool for Redis queries
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

@app.on_event("startup")
def startup_event():
    # Bootstrap our background messaging listener when the API starts up
    run_consumer_in_background()

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "matching-service-python"}

@app.get("/drivers/nearby")
def get_nearby_drivers(lat: float, lon: float, radius_km: float = 5.0):
    """
    Queries Redis using Geospatial indexing to locate all active drivers 
    within a specific radius of a user's pickup coordinate location.
    """
    try:
        # Search the Geospatial index "drivers:locations"
        # Returns list of tuples: (driver_id, distance_from_center)
        nearby = r.georadius(
            name="drivers:locations",
            longitude=lon,
            latitude=lat,
            radius=radius_km,
            unit="km",
            withdist=True,
            sort="ASC"
        )
        
        # Format list array response structure
        drivers_list = [{"driverId": item[0], "distanceKm": round(item[1], 2)} for item in nearby]
        return {"searchRadiusKm": radius_km, "driversFoundCount": len(drivers_list), "drivers": drivers_list}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache lookup failed: {str(e)}")
INNER_EOF

echo "✓ Python service directory tree and files successfully written!"
EOF

# Execute the creation script
bash setup_python_service.sh
rm setup_python_service.sh