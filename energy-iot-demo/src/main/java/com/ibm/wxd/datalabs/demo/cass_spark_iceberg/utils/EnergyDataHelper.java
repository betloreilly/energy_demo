package com.ibm.wxd.datalabs.demo.cass_spark_iceberg.utils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.ibm.wxd.datalabs.demo.cass_spark_iceberg.dto.Asset;
import com.ibm.wxd.datalabs.demo.cass_spark_iceberg.dto.SensorReading;

public class EnergyDataHelper {
    private Random random = new Random();
    
    // Asset distribution configuration
    private static final int WIND_TURBINES = 500;
    private static final int SOLAR_PANELS = 200;
    private static final int SUBSTATIONS = 50;
    private static final int TRANSMISSION_LINES = 100;
    
    // Regions
    private static final String[] REGIONS = {"North", "South", "East", "West", "Central"};
    
    /**
     * Generate specified number of assets with realistic distribution
     */
    public List<Asset> generateAssets(int totalAssets) {
        List<Asset> assets = new ArrayList<>();
        
        // Calculate proportions
        int numWindTurbines = (int)(totalAssets * 0.59);  // 59%
        int numSolarPanels = (int)(totalAssets * 0.24);   // 24%
        int numSubstations = (int)(totalAssets * 0.06);   // 6%
        int numTransmissionLines = totalAssets - numWindTurbines - numSolarPanels - numSubstations;  // Rest
        
        // Generate wind turbines
        assets.addAll(generateWindTurbines(numWindTurbines));
        
        // Generate solar panels
        assets.addAll(generateSolarPanels(numSolarPanels));
        
        // Generate substations
        assets.addAll(generateSubstations(numSubstations));
        
        // Generate transmission lines
        assets.addAll(generateTransmissionLines(numTransmissionLines));
        
        return assets;
    }
    
    private List<Asset> generateWindTurbines(int count) {
        List<Asset> turbines = new ArrayList<>();
        int turbinesPerFarm = count / 5;  // 5 farms
        
        String[] farmNames = {"North Wind Farm", "South Wind Farm", "East Wind Farm", 
                              "West Wind Farm", "Central Wind Farm"};
        
        for (int i = 0; i < farmNames.length && turbines.size() < count; i++) {
            String region = farmNames[i].split(" ")[0];
            int farmTurbines = (i == farmNames.length - 1) ? 
                              (count - turbines.size()) : turbinesPerFarm;
            
            for (int j = 1; j <= farmTurbines; j++) {
                Asset asset = new Asset("wind_turbine", 
                    String.format("%s-WT-%03d", region, j),
                    farmNames[i],
                    region);
                asset.setRatedCapacity(2500.0);  // 2.5 MW
                asset.setRatedVoltage(690.0);
                asset.setLatitude(40.0 + random.nextDouble() * 10);
                asset.setLongitude(-100.0 + random.nextDouble() * 20);
                turbines.add(asset);
            }
        }
        return turbines;
    }
    
    private List<Asset> generateSolarPanels(int count) {
        List<Asset> panels = new ArrayList<>();
        int panelsPerFarm = count / 5;
        
        String[] farmNames = {"North Solar Farm", "South Solar Farm", "East Solar Farm",
                              "Central Solar Farm", "West Solar Farm"};
        
        for (int i = 0; i < farmNames.length && panels.size() < count; i++) {
            String region = farmNames[i].split(" ")[0];
            int farmPanels = (i == farmNames.length - 1) ? 
                            (count - panels.size()) : panelsPerFarm;
            
            for (int j = 1; j <= farmPanels; j++) {
                Asset asset = new Asset("solar_panel",
                    String.format("%s-SP-%03d", region, j),
                    farmNames[i],
                    region);
                asset.setRatedCapacity(500.0);  // 500 kW
                asset.setRatedVoltage(1000.0);
                asset.setLatitude(35.0 + random.nextDouble() * 10);
                asset.setLongitude(-110.0 + random.nextDouble() * 20);
                panels.add(asset);
            }
        }
        return panels;
    }
    
    private List<Asset> generateSubstations(int count) {
        List<Asset> substations = new ArrayList<>();
        int perRegion = count / 5;
        
        for (int i = 0; i < REGIONS.length && substations.size() < count; i++) {
            int regionSubstations = (i == REGIONS.length - 1) ? 
                                   (count - substations.size()) : perRegion;
            
            for (int j = 1; j <= regionSubstations; j++) {
                Asset asset = new Asset("substation",
                    String.format("%s-SUB-%02d", REGIONS[i], j),
                    REGIONS[i] + " Substation Network",
                    REGIONS[i]);
                asset.setRatedCapacity(50000.0);  // 50 MW
                asset.setRatedVoltage(132000.0 + random.nextInt(3) * 88000.0);  // 132kV, 220kV, or 400kV
                asset.setLatitude(38.0 + random.nextDouble() * 12);
                asset.setLongitude(-105.0 + random.nextDouble() * 20);
                substations.add(asset);
            }
        }
        return substations;
    }
    
    private List<Asset> generateTransmissionLines(int count) {
        List<Asset> lines = new ArrayList<>();
        int perRegion = count / 5;
        
        for (int i = 0; i < REGIONS.length && lines.size() < count; i++) {
            int regionLines = (i == REGIONS.length - 1) ? 
                             (count - lines.size()) : perRegion;
            
            for (int j = 1; j <= regionLines; j++) {
                Asset asset = new Asset("transmission_line",
                    String.format("%s-TL-%03d", REGIONS[i], j),
                    REGIONS[i] + " Transmission Network",
                    REGIONS[i]);
                asset.setRatedCapacity(20000.0);  // 20 MW
                asset.setRatedVoltage(random.nextBoolean() ? 132000.0 : 220000.0);
                asset.setLatitude(38.0 + random.nextDouble() * 12);
                asset.setLongitude(-105.0 + random.nextDouble() * 20);
                lines.add(asset);
            }
        }
        return lines;
    }
    
    /**
     * Generate a sensor reading for an asset at a specific time
     */
    public SensorReading generateReading(Asset asset, Instant timestamp) {
        SensorReading reading = new SensorReading();
        
        // Set asset info
        reading.setAssetId(asset.getAssetId());
        reading.setAssetName(asset.getAssetName());
        reading.setAssetType(asset.getAssetType());
        reading.setFacilityId(asset.getFacilityId());
        reading.setFacilityName(asset.getFacilityName());
        reading.setRegion(asset.getRegion());
        reading.setLatitude(asset.getLatitude());
        reading.setLongitude(asset.getLongitude());
        reading.setReadingTimestamp(timestamp);
        
        // Generate readings based on asset type
        switch (asset.getAssetType()) {
            case "wind_turbine":
                generateWindTurbineReading(reading, asset);
                break;
            case "solar_panel":
                generateSolarPanelReading(reading, asset, timestamp);
                break;
            case "substation":
                generateSubstationReading(reading, asset);
                break;
            case "transmission_line":
                generateTransmissionLineReading(reading, asset);
                break;
        }
        
        return reading;
    }
    
    private void generateWindTurbineReading(SensorReading reading, Asset asset) {
        // Environmental conditions
        double windSpeed = 3.0 + random.nextDouble() * 22.0;  // 3-25 m/s
        double ambientTemp = 10.0 + random.nextDouble() * 25.0;  // 10-35°C
        
        reading.setWindSpeed(windSpeed);
        reading.setAmbientTemperature(ambientTemp);
        reading.setSolarIrradiance(null);  // N/A for wind turbines
        
        // Power output depends on wind speed (simplified power curve)
        double powerOutput;
        if (windSpeed < 4.0) {
            powerOutput = 0.0;  // Cut-in speed not reached
        } else if (windSpeed < 12.0) {
            // Cubic relationship in normal range
            powerOutput = asset.getRatedCapacity() * Math.pow((windSpeed - 4.0) / 8.0, 3);
        } else if (windSpeed < 25.0) {
            powerOutput = asset.getRatedCapacity() * (0.9 + random.nextDouble() * 0.1);
        } else {
            powerOutput = 0.0;  // Cut-out speed, turbine shutdown
        }
        
        // Add realistic noise
        powerOutput *= (0.95 + random.nextDouble() * 0.10);
        reading.setPowerOutput(powerOutput);
        
        // Voltage and current
        reading.setVoltage(asset.getRatedVoltage() * (0.95 + random.nextDouble() * 0.10));
        reading.setCurrent(powerOutput * 1000 / reading.getVoltage());  // P = V * I
        
        // Temperature (bearing/gearbox temperature)
        reading.setTemperature(40.0 + (powerOutput / asset.getRatedCapacity()) * 35.0 + 
                              random.nextGaussian() * 5.0);
        
        // Vibration (increases with power output and degradation)
        reading.setVibrationLevel(1.0 + (powerOutput / asset.getRatedCapacity()) * 5.0 + 
                                 random.nextGaussian() * 2.0);
        
        // Grid parameters
        reading.setFrequency(50.0 + random.nextGaussian() * 0.1);
        reading.setPowerFactor(0.95 + random.nextDouble() * 0.04);
        
        // Calculate efficiency and capacity factor
        double theoreticalPower = asset.getRatedCapacity() * Math.pow(windSpeed / 12.0, 3);
        reading.setEfficiency(Math.min(100.0, (powerOutput / Math.max(1.0, theoreticalPower)) * 100.0));
        reading.setCapacityFactor((powerOutput / asset.getRatedCapacity()) * 100.0);
        
        // Determine status and alerts
        determineStatus(reading);
    }
    
    private void generateSolarPanelReading(SensorReading reading, Asset asset, Instant timestamp) {
        // Time-based solar irradiance (simplified: depends on hour of day)
        int hour = timestamp.atZone(java.time.ZoneOffset.UTC).getHour();
        double irradiance;
        
        if (hour < 6 || hour > 20) {
            irradiance = 0.0;  // Night
        } else {
            // Bell curve during day, peak at noon
            double hourFromNoon = Math.abs(hour - 13);
            irradiance = 1000.0 * Math.exp(-Math.pow(hourFromNoon / 5.0, 2));
            irradiance *= (0.8 + random.nextDouble() * 0.4);  // Weather variation
        }
        
        reading.setSolarIrradiance(irradiance);
        reading.setWindSpeed(null);  // N/A for solar
        reading.setAmbientTemperature(15.0 + random.nextDouble() * 20.0);
        
        // Power output proportional to irradiance
        double powerOutput = asset.getRatedCapacity() * (irradiance / 1000.0) * 
                            (0.90 + random.nextDouble() * 0.10);
        reading.setPowerOutput(powerOutput);
        
        // Panel temperature (higher in sunlight)
        reading.setTemperature(reading.getAmbientTemperature() + 
                              (irradiance / 1000.0) * 25.0 + 
                              random.nextGaussian() * 3.0);
        
        // Electrical parameters
        reading.setVoltage(asset.getRatedVoltage() * (0.95 + random.nextDouble() * 0.10));
        reading.setCurrent(powerOutput * 1000 / reading.getVoltage());
        reading.setFrequency(50.0 + random.nextGaussian() * 0.05);
        reading.setPowerFactor(0.98 + random.nextDouble() * 0.02);
        reading.setVibrationLevel(null);  // N/A for solar
        
        // Efficiency (decreases with temperature)
        double tempDerate = 1.0 - ((reading.getTemperature() - 25.0) * 0.004);
        reading.setEfficiency(Math.min(100.0, 85.0 * tempDerate * (0.95 + random.nextDouble() * 0.05)));
        reading.setCapacityFactor((powerOutput / asset.getRatedCapacity()) * 100.0);
        
        determineStatus(reading);
    }
    
    private void generateSubstationReading(SensorReading reading, Asset asset) {
        // Substations handle high power throughput
        double loadFactor = 0.3 + random.nextDouble() * 0.65;  // 30-95% load
        double powerOutput = asset.getRatedCapacity() * loadFactor;
        
        reading.setPowerOutput(powerOutput);
        reading.setVoltage(asset.getRatedVoltage() * (0.98 + random.nextDouble() * 0.04));
        reading.setCurrent(powerOutput * 1000 / reading.getVoltage());
        
        // Transformer temperature
        reading.setTemperature(25.0 + loadFactor * 25.0 + random.nextGaussian() * 3.0);
        reading.setAmbientTemperature(15.0 + random.nextDouble() * 20.0);
        
        reading.setFrequency(50.0 + random.nextGaussian() * 0.08);
        reading.setPowerFactor(0.92 + random.nextDouble() * 0.06);
        reading.setVibrationLevel(0.5 + random.nextGaussian() * 0.3);
        
        reading.setWindSpeed(null);
        reading.setSolarIrradiance(null);
        
        reading.setEfficiency(96.0 + random.nextGaussian() * 1.5);
        reading.setCapacityFactor(loadFactor * 100.0);
        
        determineStatus(reading);
    }
    
    private void generateTransmissionLineReading(SensorReading reading, Asset asset) {
        // Transmission lines can have bidirectional power flow
        double powerOutput = (random.nextDouble() - 0.5) * 2.0 * asset.getRatedCapacity() * 0.8;
        
        reading.setPowerOutput(powerOutput);
        reading.setVoltage(asset.getRatedVoltage() * (0.97 + random.nextDouble() * 0.06));
        reading.setCurrent(Math.abs(powerOutput) * 1000 / reading.getVoltage());
        
        // Line temperature affected by current and ambient
        double ambientTemp = 15.0 + random.nextDouble() * 20.0;
        reading.setAmbientTemperature(ambientTemp);
        reading.setTemperature(ambientTemp + Math.abs(powerOutput / asset.getRatedCapacity()) * 15.0 + 
                              random.nextGaussian() * 2.0);
        
        reading.setFrequency(50.0 + random.nextGaussian() * 0.1);
        reading.setPowerFactor(0.90 + random.nextDouble() * 0.08);
        reading.setVibrationLevel(null);
        reading.setWindSpeed(null);
        reading.setSolarIrradiance(null);
        
        reading.setEfficiency(98.0 + random.nextGaussian() * 1.0);
        reading.setCapacityFactor(Math.abs(powerOutput / asset.getRatedCapacity()) * 100.0);
        
        determineStatus(reading);
    }
    
    private void determineStatus(SensorReading reading) {
        // Default to normal
        reading.setOperationalStatus("online");
        reading.setAlertLevel("normal");
        
        // 5% chance of maintenance/offline
        double statusRoll = random.nextDouble();
        if (statusRoll < 0.03) {
            reading.setOperationalStatus("maintenance");
            reading.setAlertLevel("normal");
            return;
        } else if (statusRoll < 0.05) {
            reading.setOperationalStatus("offline");
            reading.setAlertLevel("critical");
            return;
        } else if (statusRoll < 0.08) {
            reading.setOperationalStatus("degraded");
            reading.setAlertLevel("warning");
        }
        
        // Check for anomalies (10% get warnings, 2% critical)
        double anomalyRoll = random.nextDouble();
        
        if (anomalyRoll < 0.02) {
            // Critical alert conditions
            if (reading.getTemperature() != null && random.nextBoolean()) {
                reading.setTemperature(reading.getTemperature() + 20.0);  // Overheat
            }
            if (reading.getVibrationLevel() != null && random.nextBoolean()) {
                reading.setVibrationLevel(reading.getVibrationLevel() + 8.0);  // Excessive vibration
            }
            if (reading.getFrequency() != null) {
                reading.setFrequency(reading.getFrequency() + (random.nextBoolean() ? 0.6 : -0.6));
            }
            reading.setAlertLevel("critical");
            
        } else if (anomalyRoll < 0.10) {
            // Warning conditions
            if (reading.getTemperature() != null && random.nextBoolean()) {
                reading.setTemperature(reading.getTemperature() + 10.0);
            }
            if (reading.getVibrationLevel() != null && random.nextBoolean()) {
                reading.setVibrationLevel(reading.getVibrationLevel() + 3.0);
            }
            if (reading.getEfficiency() != null) {
                reading.setEfficiency(reading.getEfficiency() * 0.85);  // Reduced efficiency
            }
            if (reading.getAlertLevel().equals("normal")) {
                reading.setAlertLevel("warning");
            }
        }
    }
}

