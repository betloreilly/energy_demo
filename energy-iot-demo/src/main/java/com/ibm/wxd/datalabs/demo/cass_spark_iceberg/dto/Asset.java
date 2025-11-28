package com.ibm.wxd.datalabs.demo.cass_spark_iceberg.dto;

import java.util.UUID;

public class Asset {
    private UUID assetId;
    private String assetName;
    private String assetType;  // wind_turbine, solar_panel, substation, transmission_line
    private UUID facilityId;
    private String facilityName;
    private String region;
    private Double latitude;
    private Double longitude;
    private Double ratedCapacity;  // kW
    private Double ratedVoltage;   // V
    
    public Asset() {
        this.assetId = UUID.randomUUID();
        this.facilityId = UUID.randomUUID();
    }
    
    public Asset(String assetType, String assetName, String facilityName, String region) {
        this();
        this.assetType = assetType;
        this.assetName = assetName;
        this.facilityName = facilityName;
        this.region = region;
    }
    
    // Getters and Setters
    public UUID getAssetId() { return assetId; }
    public void setAssetId(UUID assetId) { this.assetId = assetId; }
    
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
    
    public Double getRatedCapacity() { return ratedCapacity; }
    public void setRatedCapacity(Double ratedCapacity) { this.ratedCapacity = ratedCapacity; }
    
    public Double getRatedVoltage() { return ratedVoltage; }
    public void setRatedVoltage(Double ratedVoltage) { this.ratedVoltage = ratedVoltage; }
    
    @Override
    public String toString() {
        return String.format("Asset{name='%s', type='%s', facility='%s', region='%s'}",
            assetName, assetType, facilityName, region);
    }
}

