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
