package com.ibm.wxd.datalabs.demo.cass_spark_iceberg.dto;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class SensorReading {
    // Identifiers
    private UUID assetId;
    private String timeBucket;  // YYYY-MM-DD-HH
    private Instant readingTimestamp;
    private UUID readingId;
    
    // Sensor Measurements
    private Double powerOutput;
    private Double voltage;
    private Double current;
    private Double temperature;
    private Double vibrationLevel;
    private Double frequency;
    private Double powerFactor;
    
    // Environmental Data
    private Double ambientTemperature;
    private Double windSpeed;
    private Double solarIrradiance;
    
    // Asset Metadata
    private String assetName;
    private String assetType;
    private UUID facilityId;
    private String facilityName;
    private String region;
    private Double latitude;
    private Double longitude;
    
    // Status
    private String operationalStatus;
    private String alertLevel;
    
    // Calculated Metrics
    private Double efficiency;
    private Double capacityFactor;
    
    // Constructor
    public SensorReading() {
        this.readingId = UUID.randomUUID();
    }
    
    // Helper method to set time bucket from timestamp
    public void setTimeBucketFromTimestamp() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
            .withZone(ZoneOffset.UTC);
        this.timeBucket = formatter.format(this.readingTimestamp);
    }
    
    // Getters and Setters
    public UUID getAssetId() { return assetId; }
    public void setAssetId(UUID assetId) { this.assetId = assetId; }
    
    public String getTimeBucket() { return timeBucket; }
    public void setTimeBucket(String timeBucket) { this.timeBucket = timeBucket; }
    
    public Instant getReadingTimestamp() { return readingTimestamp; }
    public void setReadingTimestamp(Instant readingTimestamp) { 
        this.readingTimestamp = readingTimestamp;
        setTimeBucketFromTimestamp();
    }
    
    public UUID getReadingId() { return readingId; }
    public void setReadingId(UUID readingId) { this.readingId = readingId; }
    
    public Double getPowerOutput() { return powerOutput; }
    public void setPowerOutput(Double powerOutput) { this.powerOutput = powerOutput; }
    
    public Double getVoltage() { return voltage; }
    public void setVoltage(Double voltage) { this.voltage = voltage; }
    
    public Double getCurrent() { return current; }
    public void setCurrent(Double current) { this.current = current; }
    
    public Double getTemperature() { return temperature; }
    public void setTemperature(Double temperature) { this.temperature = temperature; }
    
    public Double getVibrationLevel() { return vibrationLevel; }
    public void setVibrationLevel(Double vibrationLevel) { this.vibrationLevel = vibrationLevel; }
    
    public Double getFrequency() { return frequency; }
    public void setFrequency(Double frequency) { this.frequency = frequency; }
    
    public Double getPowerFactor() { return powerFactor; }
    public void setPowerFactor(Double powerFactor) { this.powerFactor = powerFactor; }
    
    public Double getAmbientTemperature() { return ambientTemperature; }
    public void setAmbientTemperature(Double ambientTemperature) { 
        this.ambientTemperature = ambientTemperature; 
    }
    
    public Double getWindSpeed() { return windSpeed; }
    public void setWindSpeed(Double windSpeed) { this.windSpeed = windSpeed; }
    
    public Double getSolarIrradiance() { return solarIrradiance; }
    public void setSolarIrradiance(Double solarIrradiance) { 
        this.solarIrradiance = solarIrradiance; 
    }
    
    public String getAssetName() { return assetName; }
    public void setAssetName(String assetName) { this.assetName = assetName; }
    
    public String getAssetType() { return assetType; }
    public void setAssetType(String assetType) { this.assetType = assetType; }
    
    public UUID getFacilityId() { return facilityId; }
    public void setFacilityId(UUID facilityId) { this.facilityId = facilityId; }
    
    public String getFacilityName() { return facilityName; }
    public void setFacilityName(String facilityName) { this.facilityName = facilityName; }
    
    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }
    
    public Double getLatitude() { return latitude; }
    public void setLatitude(Double latitude) { this.latitude = latitude; }
    
    public Double getLongitude() { return longitude; }
    public void setLongitude(Double longitude) { this.longitude = longitude; }
    
    public String getOperationalStatus() { return operationalStatus; }
    public void setOperationalStatus(String operationalStatus) { 
        this.operationalStatus = operationalStatus; 
    }
    
    public String getAlertLevel() { return alertLevel; }
    public void setAlertLevel(String alertLevel) { this.alertLevel = alertLevel; }
    
    public Double getEfficiency() { return efficiency; }
    public void setEfficiency(Double efficiency) { this.efficiency = efficiency; }
    
    public Double getCapacityFactor() { return capacityFactor; }
    public void setCapacityFactor(Double capacityFactor) { 
        this.capacityFactor = capacityFactor; 
    }
    
    @Override
    public String toString() {
        return String.format("SensorReading{assetName='%s', timestamp=%s, power=%.2f kW, status='%s'}",
            assetName, readingTimestamp, powerOutput, operationalStatus);
    }
}

