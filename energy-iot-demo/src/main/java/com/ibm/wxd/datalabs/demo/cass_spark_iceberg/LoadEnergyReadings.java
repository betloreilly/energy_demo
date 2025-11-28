package com.ibm.wxd.datalabs.demo.cass_spark_iceberg;

import java.util.ArrayList;
import java.util.List;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.ibm.wxd.datalabs.demo.cass_spark_iceberg.dto.Asset;
import com.ibm.wxd.datalabs.demo.cass_spark_iceberg.dto.SensorReading;
import com.ibm.wxd.datalabs.demo.cass_spark_iceberg.utils.CassUtil;
import com.ibm.wxd.datalabs.demo.cass_spark_iceberg.utils.EnergyDataHelper;

/**
 * Loads randomly generated energy sector sensor data into Cassandra
 * 
 * Usage: java LoadEnergyReadings [num_assets] [readings_per_asset]
 * Example: java LoadEnergyReadings 850 360
 *          Generates 850 assets with 360 readings each (1 hour at 10-sec intervals)
 */
public class LoadEnergyReadings {
    private static Logger LOGGER = LoggerFactory.getLogger(LoadEnergyReadings.class);
    private CqlSession session;
    
    // Configurable parameters
    private static final int DEFAULT_NUM_ASSETS = 850;
    private static final int DEFAULT_READINGS_PER_ASSET = 360;  // 1 hour of data
    private static final int BATCH_SIZE = 1000;
    
    private CassUtil util;
    private EnergyDataHelper helper;

    public LoadEnergyReadings() {
        util = new CassUtil();
        helper = new EnergyDataHelper();
        session = util.getLocalCQLSession();
        
        // Create keyspace and table FIRST
        util.createEnergySchema(session);
    }

    /**
     * Generate sensor readings for all assets over time
     */
    public List<SensorReading> generateSensorReadings(int numAssets, int readingsPerAsset) {
        List<SensorReading> readings = new ArrayList<>();
        
        LOGGER.info("Generating {} assets...", numAssets);
        List<Asset> assets = helper.generateAssets(numAssets);
        LOGGER.info("Assets generated: {} wind turbines, {} solar panels, {} substations, {} transmission lines",
            assets.stream().filter(a -> a.getAssetType().equals("wind_turbine")).count(),
            assets.stream().filter(a -> a.getAssetType().equals("solar_panel")).count(),
            assets.stream().filter(a -> a.getAssetType().equals("substation")).count(),
            assets.stream().filter(a -> a.getAssetType().equals("transmission_line")).count());
        
        // Time range: last N readings at 10-second intervals
        Instant endTime = Instant.now();
        Instant startTime = endTime.minusSeconds(readingsPerAsset * 10L);
        
        LOGGER.info("Generating readings from {} to {} (10-second intervals)...", startTime, endTime);
        
        // Generate readings for each asset
        int assetCount = 0;
        for (Asset asset : assets) {
            Instant currentTime = startTime;
            for (int i = 0; i < readingsPerAsset; i++) {
                SensorReading reading = helper.generateReading(asset, currentTime);
                readings.add(reading);
                currentTime = currentTime.plusSeconds(10);
            }
            
            assetCount++;
            if (assetCount % 100 == 0) {
                LOGGER.info("Generated readings for {} / {} assets ({} total readings so far)...", 
                    assetCount, numAssets, readings.size());
            }
        }
        
        LOGGER.info("Generated {} total sensor readings for {} assets", readings.size(), numAssets);
        return readings;
    }

    /**
     * Load sensor readings into Cassandra (only insert non-null columns to avoid tombstones)
     */
    public void loadData(int numAssets, int readingsPerAsset) {
        List<SensorReading> readings = generateSensorReadings(numAssets, readingsPerAsset);
        
        LOGGER.info("Starting to insert {} readings into Cassandra (skipping null columns)...", readings.size());
        
        int batchCount = 0;
        int totalInserted = 0;
        long startTime = System.currentTimeMillis();
        
        for (SensorReading reading : readings) {
            // Build dynamic INSERT with only non-null columns
            String insertCql = buildInsertStatement(reading);
            session.execute(SimpleStatement.newInstance(insertCql));
            
            batchCount++;
            totalInserted++;
            
            if (batchCount >= BATCH_SIZE) {
                long elapsed = (System.currentTimeMillis() - startTime) / 1000;
                double rate = totalInserted / Math.max(1.0, elapsed);
                LOGGER.info("Inserted {} / {} readings ({} readings/sec)...", 
                    totalInserted, readings.size(), (int)rate);
                batchCount = 0;
            }
        }
        
        long totalTime = (System.currentTimeMillis() - startTime) / 1000;
        double avgRate = readings.size() / Math.max(1.0, totalTime);
        
        LOGGER.info("Successfully inserted {} sensor readings in {} seconds ({} readings/sec)", 
            readings.size(), totalTime, (int)avgRate);
        
        // Show sample queries
        LOGGER.info("\n=== Sample Queries ===");
        LOGGER.info("-- View recent readings:");
        LOGGER.info("SELECT * FROM {}.sensor_readings_by_asset WHERE asset_id = <uuid> AND time_bucket = '<bucket>' LIMIT 10;", 
            CassUtil.KEYSPACE_NAME);
        LOGGER.info("\n-- Count by asset type:");
        LOGGER.info("SELECT asset_type, COUNT(*) FROM {}.sensor_readings_by_asset GROUP BY asset_type ALLOW FILTERING;",
            CassUtil.KEYSPACE_NAME);
        
        util.closeSession(session);
    }
    
    /**
     * Build dynamic INSERT statement with only non-null columns (avoids tombstones)
     */
    private String buildInsertStatement(SensorReading reading) {
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        
        // Required columns (always present)
        columns.append("asset_id, time_bucket, reading_timestamp, reading_id, ");
        values.append(reading.getAssetId()).append(", '")
              .append(reading.getTimeBucket()).append("', '")
              .append(reading.getReadingTimestamp()).append("', ")
              .append(reading.getReadingId()).append(", ");
        
        // Sensor measurements (include only if not null)
        addColumn(columns, values, "power_output", reading.getPowerOutput());
        addColumn(columns, values, "voltage", reading.getVoltage());
        addColumn(columns, values, "current", reading.getCurrent());
        addColumn(columns, values, "temperature", reading.getTemperature());
        addColumn(columns, values, "vibration_level", reading.getVibrationLevel());
        addColumn(columns, values, "frequency", reading.getFrequency());
        addColumn(columns, values, "power_factor", reading.getPowerFactor());
        
        // Environmental data (include only if not null)
        addColumn(columns, values, "ambient_temperature", reading.getAmbientTemperature());
        addColumn(columns, values, "wind_speed", reading.getWindSpeed());
        addColumn(columns, values, "solar_irradiance", reading.getSolarIrradiance());
        
        // Asset metadata (always present)
        columns.append("asset_name, asset_type, facility_id, facility_name, region, latitude, longitude, ");
        values.append("'").append(reading.getAssetName()).append("', '")
              .append(reading.getAssetType()).append("', ")
              .append(reading.getFacilityId()).append(", '")
              .append(reading.getFacilityName()).append("', '")
              .append(reading.getRegion()).append("', ")
              .append(reading.getLatitude()).append(", ")
              .append(reading.getLongitude()).append(", ");
        
        // Status and metrics (always present)
        columns.append("operational_status, alert_level, efficiency, capacity_factor");
        values.append("'").append(reading.getOperationalStatus()).append("', '")
              .append(reading.getAlertLevel()).append("', ")
              .append(reading.getEfficiency()).append(", ")
              .append(reading.getCapacityFactor());
        
        return "INSERT INTO " + CassUtil.KEYSPACE_NAME + ".sensor_readings_by_asset (" + 
               columns.toString() + ") VALUES (" + values.toString() + ");";
    }
    
    /**
     * Add column and value to INSERT if value is not null
     */
    private void addColumn(StringBuilder columns, StringBuilder values, String columnName, Double value) {
        if (value != null) {
            columns.append(columnName).append(", ");
            values.append(value).append(", ");
        }
    }

    public static void main(String[] args) {
        int numAssets = DEFAULT_NUM_ASSETS;
        int readingsPerAsset = DEFAULT_READINGS_PER_ASSET;
        
        if (args.length > 0) {
            try {
                numAssets = Integer.parseInt(args[0]);
                LOGGER.info("Number of assets to generate: {}", numAssets);
                
                if (args.length > 1) {
                    readingsPerAsset = Integer.parseInt(args[1]);
                    LOGGER.info("Readings per asset: {} (covers {} minutes)", 
                        readingsPerAsset, readingsPerAsset / 6);
                }
            } catch (NumberFormatException e) {
                LOGGER.error("Invalid arguments. Usage: LoadEnergyReadings [num_assets] [readings_per_asset]");
                System.exit(1);
            }
        } else {
            LOGGER.info("Using defaults: {} assets, {} readings/asset", 
                DEFAULT_NUM_ASSETS, DEFAULT_READINGS_PER_ASSET);
            LOGGER.info("Usage: java LoadEnergyReadings [num_assets] [readings_per_asset]");
        }
        
        LOGGER.info("\n=== Energy Sector Data Generation ===");
        LOGGER.info("Total readings to generate: {}", numAssets * readingsPerAsset);
        LOGGER.info("Estimated time: ~{} minutes", (numAssets * readingsPerAsset) / 5000);
        LOGGER.info("=====================================\n");
        
        (new LoadEnergyReadings()).loadData(numAssets, readingsPerAsset);
    }
}

