package com.tracking.driverservice.model;

public class DriverLocation {
    private String driverId;
    private double latitude;
    private double longitude;
    private long timestamp;

    // No-argument constructor (Required by Jackson for JSON parsing)
    public DriverLocation() {
    }

    // All-argument constructor
    public DriverLocation(String driverId, double latitude, double longitude, long timestamp) {
        this.driverId = driverId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
    }

    // Getters and Setters
    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}

