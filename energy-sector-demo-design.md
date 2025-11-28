# Energy Sector Demo - Cassandra to Iceberg with watsonx.data

**Use Case**: Real-time operational intelligence for distributed energy infrastructure with time-series sensor data, predictive maintenance, and grid optimization analytics.

---

## Table of Contents

1. [Demo Overview](#demo-overview)
2. [Data Model Design](#data-model-design)
3. [Cassandra Schema (Operational)](#cassandra-schema-operational)
4. [Iceberg Schema (Analytics)](#iceberg-schema-analytics)
5. [Data Generation Specifications](#data-generation-specifications)
6. [Java Code Modifications](#java-code-modifications)
7. [Sample Analytical Queries](#sample-analytical-queries)
8. [Demo Narrative](#demo-narrative)

---

## Demo Overview

### Business Scenario

**PowerGrid Energy** operates a distributed energy infrastructure across multiple regions with:
- 500 wind turbines across 10 wind farms
- 200 solar panel arrays across 5 solar facilities
- 50 substations managing grid distribution
- 100 transmission line monitoring points

Each asset generates **time-series telemetry data** every 10 seconds:
- Power generation/consumption
- Equipment temperature, voltage, current
- Vibration and acoustic sensors
- Weather conditions
- Grid stability metrics

### Volume Projection

- **750 IoT sensors** × 6 readings/minute = **4,500 readings/minute**
- **270,000 readings/hour**
- **6.5M readings/day**
- **195M readings/month**

### Key Use Cases

1. **Real-Time Monitoring**: Track equipment health and power output
2. **Predictive Maintenance**: Identify anomalies before failures
3. **Grid Optimization**: Balance supply/demand across regions
4. **Regulatory Reporting**: Aggregate production and emissions data
5. **Digital Twin Analytics**: Model and simulate asset performance

---

## Data Model Design

### Architecture Philosophy

We use a **denormalized data model in both Cassandra and Iceberg** - this is the industry-standard approach for IoT time-series data.

### Why Two Systems?

**Cassandra (Operational):**
- ✅ **High write throughput**: Handle 4,500+ inserts/minute for real-time sensor data
- ✅ **Time-series partitioning**: Partition by asset + time bucket for efficient recent data queries
- ✅ **Low-latency reads**: Sub-millisecond queries for latest readings
- ✅ **Hot data**: Optimized for recent data (last hours/days)

**Iceberg (Analytical):**
- ✅ **Historical analysis**: Long-term storage for months/years of data
- ✅ **Time-based partitioning**: Year/month/day partitions for efficient date-range queries
- ✅ **Complex analytics**: Aggregations, trends, and pattern analysis
- ✅ **AI/ML ready**: Open format for machine learning pipelines
- ✅ **Cost-effective**: Object storage (S3/MinIO) cheaper than operational database

### Data Model: Denormalized Time-Series

Both systems use the **same denormalized structure** - all sensor measurements and asset metadata in one table:
- No complex joins needed
- Simple queries: `SELECT ... FROM sensor_readings WHERE ...`
- Fast aggregations: All data in one place
- Easy to understand and maintain

---

## Cassandra Schema (Operational)

### Main Table: `sensor_readings_by_asset`

**Purpose**: Store all sensor telemetry with asset metadata (denormalized for operational performance)

```sql
CREATE KEYSPACE IF NOT EXISTS energy_ks 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE energy_ks;

CREATE TABLE IF NOT EXISTS sensor_readings_by_asset (
    -- Partition Key: Asset ID + Time Bucket (for time-series optimization)
    asset_id UUID,
    time_bucket TEXT,  -- Format: YYYY-MM-DD-HH (hourly buckets)
    
    -- Clustering Key: Reading timestamp (descending for recent-first queries)
    reading_timestamp TIMESTAMP,
    reading_id UUID,
    
    -- Sensor Measurements
    power_output DOUBLE,           -- kW (can be negative for consumption)
    voltage DOUBLE,                 -- Volts
    current DOUBLE,                 -- Amperes
    temperature DOUBLE,             -- Celsius
    vibration_level DOUBLE,         -- mm/s
    frequency DOUBLE,               -- Hz (grid frequency)
    power_factor DOUBLE,            -- 0-1
    
    -- Environmental Data
    ambient_temperature DOUBLE,     -- Celsius
    wind_speed DOUBLE,              -- m/s (for wind turbines)
    solar_irradiance DOUBLE,        -- W/m² (for solar panels)
    
    -- Asset Metadata (denormalized)
    asset_name TEXT,
    asset_type TEXT,                -- 'wind_turbine', 'solar_panel', 'substation', 'transmission_line'
    facility_id UUID,
    facility_name TEXT,
    region TEXT,                    -- 'north', 'south', 'east', 'west', 'central'
    latitude DOUBLE,
    longitude DOUBLE,
    
    -- Operational Status
    operational_status TEXT,        -- 'online', 'offline', 'maintenance', 'degraded'
    alert_level TEXT,               -- 'normal', 'warning', 'critical'
    
    -- Calculated Metrics
    efficiency DOUBLE,              -- Percentage
    capacity_factor DOUBLE,         -- Percentage
    
    PRIMARY KEY ((asset_id, time_bucket), reading_timestamp, reading_id)
) WITH CLUSTERING ORDER BY (reading_timestamp DESC, reading_id DESC)
  AND comment = 'Time-series sensor data with asset metadata for operational queries';
```

**Partition Strategy**: 
- Partition by `asset_id` + `time_bucket` to avoid hot partitions
- Allows efficient queries for "recent readings for asset X"
- Time bucket prevents unbounded partition growth

### Query Patterns Supported

```sql
-- Get last hour of readings for a specific turbine
SELECT * FROM sensor_readings_by_asset 
WHERE asset_id = ? AND time_bucket = '2025-11-28-11'
ORDER BY reading_timestamp DESC
LIMIT 360;  -- 6 readings/min × 60 min

-- Get latest reading for asset
SELECT * FROM sensor_readings_by_asset 
WHERE asset_id = ? AND time_bucket = '2025-11-28-11'
ORDER BY reading_timestamp DESC
LIMIT 1;

-- Get all critical alerts in current hour across all assets
SELECT * FROM sensor_readings_by_asset 
WHERE time_bucket = '2025-11-28-11' AND alert_level = 'critical'
ALLOW FILTERING;
```

---

## Iceberg Schema (Analytics) - Denormalized Time-Series

**Philosophy**: For IoT/time-series data, keep it simple and denormalized. Iceberg handles large datasets efficiently without complex star schemas.

### Main Table: `sensor_readings` (Denormalized)

```sql
CREATE TABLE iceberg_data.energy_data.sensor_readings (
    reading_id VARCHAR,
    reading_timestamp TIMESTAMP,
    
    -- Asset Information (denormalized)
    asset_id VARCHAR,
    asset_name VARCHAR,
    asset_type VARCHAR,              -- 'wind_turbine', 'solar_panel', 'substation', 'transmission_line'
    
    -- Facility Information (denormalized)
    facility_id VARCHAR,
    facility_name VARCHAR,
    region VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    
    -- Sensor Measurements
    power_output DOUBLE,             -- kW
    voltage DOUBLE,                  -- Volts
    current DOUBLE,                  -- Amperes
    temperature DOUBLE,              -- Celsius
    vibration_level DOUBLE,          -- mm/s
    frequency DOUBLE,                -- Hz
    power_factor DOUBLE,             -- 0-1
    
    -- Environmental Data
    ambient_temperature DOUBLE,      -- Celsius
    wind_speed DOUBLE,               -- m/s
    solar_irradiance DOUBLE,         -- W/m²
    
    -- Status
    operational_status VARCHAR,      -- 'online', 'offline', 'maintenance', 'degraded'
    alert_level VARCHAR,             -- 'normal', 'warning', 'critical'
    
    -- Calculated Metrics
    efficiency DOUBLE,               -- Percentage
    capacity_factor DOUBLE,          -- Percentage
    
    -- Time Components (for efficient filtering)
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    
    PRIMARY KEY (reading_id)
) PARTITIONED BY (year, month, day);
```

**Key Design Decisions**:
- ✅ **Denormalized**: Asset and facility info in same table - no joins needed
- ✅ **Time-partitioned**: Efficient pruning for time-based queries
- ✅ **All metrics together**: Real-world IoT platforms store everything in one table
- ✅ **Simple**: Easy to query, easy to understand

### Optional: Aggregated Table for Fast Dashboards

```sql
CREATE TABLE iceberg_data.energy_data.hourly_metrics (
    asset_id VARCHAR,
    hour_timestamp TIMESTAMP,
    asset_type VARCHAR,
    facility_name VARCHAR,
    region VARCHAR,
    
    -- Aggregated Metrics
    avg_power_output DOUBLE,
    max_power_output DOUBLE,
    min_power_output DOUBLE,
    total_energy_kwh DOUBLE,         -- Power × time
    
    avg_temperature DOUBLE,
    max_temperature DOUBLE,
    avg_vibration DOUBLE,
    max_vibration DOUBLE,
    
    avg_efficiency DOUBLE,
    
    -- Counts
    total_readings INTEGER,
    critical_alerts INTEGER,
    warning_alerts INTEGER,
    
    -- Time components
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    
    PRIMARY KEY (asset_id, hour_timestamp)
) PARTITIONED BY (year, month);
```

---

## Data Generation Specifications

### Asset Distribution

```
Total Assets: 850

Wind Turbines: 500 (59%)
├─ North Wind Farm: 100 turbines
├─ South Wind Farm: 120 turbines
├─ East Wind Farm: 80 turbines
├─ West Wind Farm: 110 turbines
└─ Central Wind Farm: 90 turbines

Solar Panels: 200 (24%)
├─ North Solar Farm: 50 panels
├─ South Solar Farm: 60 panels
├─ East Solar Farm: 40 panels
├─ Central Solar Farm: 30 panels
└─ West Solar Farm: 20 panels

Substations: 50 (6%)
├─ 10 per region × 5 regions

Transmission Lines: 100 (11%)
├─ 20 per region × 5 regions
```

### Reading Generation Rules

**Wind Turbines:**
- Power output: 0-2500 kW (rated capacity: 2.5 MW)
- Dependent on wind speed: 3-25 m/s
- Temperature: 30-80°C (bearing temperature)
- Vibration: 0.5-15 mm/s (>10 = warning)
- Voltage: 690V ±5%
- Frequency: 50 Hz ±0.5 Hz

**Solar Panels:**
- Power output: 0-500 kW (rated capacity: 500 kW)
- Dependent on solar irradiance: 0-1000 W/m²
- Temperature: 25-60°C (panel temperature)
- Voltage: 1000V ±5%
- Current: 0-500A

**Substations:**
- Power throughput: 5000-50000 kW
- Voltage: 132kV, 220kV, or 400kV
- Temperature: 20-50°C (transformer temp)
- Load factor: 30-95%

**Transmission Lines:**
- Power flow: -10000 to +10000 kW (bidirectional)
- Voltage: 132kV or 220kV
- Current: calculated from power/voltage
- Temperature: ambient ±5°C

### Anomaly Injection

To make data realistic and demonstrate predictive maintenance:

**10% of readings include anomalies:**
- High temperature (>75°C for turbines) → alert_level = 'warning'
- Excessive vibration (>10 mm/s) → alert_level = 'warning'
- Low power output despite good conditions → alert_level = 'warning'
- Frequency deviation (>0.3 Hz) → alert_level = 'critical'
- Voltage out of range (>±8%) → alert_level = 'critical'

**5% of assets in degraded/maintenance status**

---

## Java Implementation

All Java source files are available in the `java-files/` directory and ready to deploy.

### File Structure

```
java-files/
├── README.md                    # Build and deployment instructions
├── SensorReading.java          # DTO for sensor readings
├── Asset.java                  # DTO for energy assets
├── EnergyDataHelper.java       # Realistic data generation with physics
├── LoadEnergyReadings.java     # Main data loader for Cassandra
└── CassandraToIceberg.java     # ETL job for Spark
```

See `java-files/README.md` for complete build, deployment, and usage instructions.

## Java Code Highlights

### 1. `LoadEnergyReadings.java`

```java
package com.ibm.wxd.datalabs.demo.cass_spark_iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.ibm.wxd.datalabs.demo.cass_spark_iceberg.dto.SensorReading;
import com.ibm.wxd.datalabs.demo.cass_spark_iceberg.utils.CassUtil;
import com.ibm.wxd.datalabs.demo.cass_spark_iceberg.utils.EnergyDataHelper;

public class LoadEnergyReadings {
    private static Logger LOGGER = LoggerFactory.getLogger(LoadEnergyReadings.class);
    private CqlSession session;
    private PreparedStatement preparedStatement;
    
    // Configurable parameters
    private static final int NUM_ASSETS = 850;
    private static final int READINGS_PER_ASSET = 360;  // 1 hour of data at 10-second intervals
    private static final int BATCH_SIZE = 1000;
    
    CassUtil util;
    EnergyDataHelper helper;

    public LoadEnergyReadings() {
        util = new CassUtil();
        helper = new EnergyDataHelper();
        session = util.getLocalCQLSession();
        
        String insertQuery = "INSERT INTO " + CassUtil.KEYSPACE_NAME +
                ".sensor_readings_by_asset (asset_id, time_bucket, reading_timestamp, reading_id, " +
                "power_output, voltage, current, temperature, vibration_level, frequency, power_factor, " +
                "ambient_temperature, wind_speed, solar_irradiance, " +
                "asset_name, asset_type, facility_id, facility_name, region, latitude, longitude, " +
                "operational_status, alert_level, efficiency, capacity_factor) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        
        preparedStatement = session.prepare(insertQuery);
        util.createEnergySchema(session);
    }

    public List<SensorReading> generateSensorReadings(int numAssets, int readingsPerAsset) {
        List<SensorReading> readings = new ArrayList<>();
        
        // Generate assets
        List<Asset> assets = helper.generateAssets(numAssets);
        
        // Generate readings for each asset
        Instant endTime = Instant.now();
        Instant startTime = endTime.minusSeconds(readingsPerAsset * 10); // 10 seconds between readings
        
        for (Asset asset : assets) {
            Instant currentTime = startTime;
            for (int i = 0; i < readingsPerAsset; i++) {
                SensorReading reading = helper.generateReading(asset, currentTime);
                readings.add(reading);
                currentTime = currentTime.plusSeconds(10);
            }
        }
        
        LOGGER.info("Generated {} sensor readings for {} assets", readings.size(), numAssets);
        return readings;
    }

    public void loadData(int numAssets, int readingsPerAsset) {
        List<SensorReading> readings = generateSensorReadings(numAssets, readingsPerAsset);
        
        int batchCount = 0;
        for (SensorReading reading : readings) {
            session.execute(preparedStatement.bind(
                reading.getAssetId(),
                reading.getTimeBucket(),
                reading.getReadingTimestamp(),
                reading.getReadingId(),
                reading.getPowerOutput(),
                reading.getVoltage(),
                reading.getCurrent(),
                reading.getTemperature(),
                reading.getVibrationLevel(),
                reading.getFrequency(),
                reading.getPowerFactor(),
                reading.getAmbientTemperature(),
                reading.getWindSpeed(),
                reading.getSolarIrradiance(),
                reading.getAssetName(),
                reading.getAssetType(),
                reading.getFacilityId(),
                reading.getFacilityName(),
                reading.getRegion(),
                reading.getLatitude(),
                reading.getLongitude(),
                reading.getOperationalStatus(),
                reading.getAlertLevel(),
                reading.getEfficiency(),
                reading.getCapacityFactor()
            ));
            
            batchCount++;
            if (batchCount % BATCH_SIZE == 0) {
                LOGGER.info("Inserted {} readings...", batchCount);
            }
        }
        
        LOGGER.info("Inserted total {} sensor readings for {} assets.", readings.size(), numAssets);
        util.closeSession(session);
    }

    public static void main(String[] args) {
        int numAssets = NUM_ASSETS;
        int readingsPerAsset = READINGS_PER_ASSET;
        
        if (args.length > 0) {
            numAssets = Integer.parseInt(args[0]);
            LOGGER.info("Number of assets to generate: " + numAssets);
            if (args.length > 1) {
                readingsPerAsset = Integer.parseInt(args[1]);
                LOGGER.info("Readings per asset: " + readingsPerAsset);
            }
        }
        
        (new LoadEnergyReadings()).loadData(numAssets, readingsPerAsset);
    }
}
```

### 2. Update `CassandraToIceberg.java`

```java
package com.ibm.wxd.datalabs.demo.cass_spark_iceberg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.wxd.datalabs.demo.cass_spark_iceberg.utils.SparkUtil;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class CassandraToIceberg {
    private static Logger LOGGER = LoggerFactory.getLogger(CassandraToIceberg.class);

    private static String ICEBERG_SCHEMA = "spark_catalog.energy_data";

    private static String icebergDimAsset = ICEBERG_SCHEMA + ".dim_asset";
    private static String icebergDimFacility = ICEBERG_SCHEMA + ".dim_facility";
    private static String icebergDimDate = ICEBERG_SCHEMA + ".dim_date";
    private static String icebergDimStatus = ICEBERG_SCHEMA + ".dim_status";
    private static String icebergDimRegion = ICEBERG_SCHEMA + ".dim_region";
    private static String icebergFactSensorReading = ICEBERG_SCHEMA + ".fact_sensor_reading";

    public static void main(String[] args) {
        try {
            SparkUtil sparkUtil = new SparkUtil();
            SparkSession spark = sparkUtil.createSparkSession("EnergyDataETL");
            LOGGER.info("Spark Session created.");

            // Read sensor data from Cassandra
            Dataset<Row> sensorReadingDF = getSensorReadingDF(sparkUtil, spark);

            // DIM_ASSET: Extract unique assets
            Dataset<Row> dimAssetDF = sensorReadingDF
                .select("asset_id", "asset_name", "asset_type", "latitude", "longitude")
                .distinct()
                .withColumn("asset_key", monotonically_increasing_id());
            dimAssetDF.writeTo(icebergDimAsset).using("iceberg").createOrReplace();
            LOGGER.info("Dimension table 'dim_asset' created.");

            // DIM_FACILITY: Extract unique facilities
            Dataset<Row> dimFacilityDF = sensorReadingDF
                .select("facility_id", "facility_name", "region")
                .distinct()
                .withColumn("facility_key", monotonically_increasing_id())
                .withColumn("facility_type", 
                    when(col("facility_name").contains("Wind"), "wind_farm")
                    .when(col("facility_name").contains("Solar"), "solar_farm")
                    .otherwise("substation"));
            dimFacilityDF.writeTo(icebergDimFacility).using("iceberg").createOrReplace();
            LOGGER.info("Dimension table 'dim_facility' created.");

            // DIM_DATE: Extract and enrich time dimensions
            Dataset<Row> dimDateDF = sensorReadingDF
                .select(col("reading_timestamp").as("full_date"))
                .distinct()
                .withColumn("date_key", date_format(col("full_date"), "yyyyMMddHH"))
                .withColumn("year", year(col("full_date")))
                .withColumn("quarter", quarter(col("full_date")))
                .withColumn("month", month(col("full_date")))
                .withColumn("month_name", date_format(col("full_date"), "MMMM"))
                .withColumn("day", dayofmonth(col("full_date")))
                .withColumn("hour", hour(col("full_date")))
                .withColumn("day_of_week", dayofweek(col("full_date")))
                .withColumn("is_weekend", 
                    when(col("day_of_week").isin(1, 7), true).otherwise(false))
                .withColumn("is_peak_hour",
                    when(col("hour").between(8, 20), true).otherwise(false));
            dimDateDF.writeTo(icebergDimDate).using("iceberg").createOrReplace();
            LOGGER.info("Dimension table 'dim_date' created.");

            // DIM_STATUS: Extract status combinations
            Dataset<Row> dimStatusDF = sensorReadingDF
                .select("operational_status", "alert_level")
                .distinct()
                .withColumn("status_key", monotonically_increasing_id())
                .withColumn("requires_maintenance",
                    when(col("alert_level").equalTo("critical"), true)
                    .when(col("operational_status").equalTo("degraded"), true)
                    .otherwise(false));
            dimStatusDF.writeTo(icebergDimStatus).using("iceberg").createOrReplace();
            LOGGER.info("Dimension table 'dim_status' created.");

            // DIM_REGION: Extract regions
            Dataset<Row> dimRegionDF = sensorReadingDF
                .select("region")
                .distinct()
                .withColumnRenamed("region", "region_name")
                .withColumn("region_key", monotonically_increasing_id())
                .withColumn("region_code", upper(substring(col("region_name"), 0, 3)));
            dimRegionDF.writeTo(icebergDimRegion).using("iceberg").createOrReplace();
            LOGGER.info("Dimension table 'dim_region' created.");

            // Read back dimensions for joining
            Dataset<Row> dimAssetLookUp = spark.table(icebergDimAsset);
            Dataset<Row> dimFacilityLookUp = spark.table(icebergDimFacility);
            Dataset<Row> dimDateLookUp = spark.table(icebergDimDate);
            Dataset<Row> dimStatusLookUp = spark.table(icebergDimStatus);
            Dataset<Row> dimRegionLookUp = spark.table(icebergDimRegion);

            // FACT_SENSOR_READING: Join with dimensions
            Dataset<Row> factSensorReadingDF = sensorReadingDF
                .join(dimAssetLookUp,
                    sensorReadingDF.col("asset_id").equalTo(dimAssetLookUp.col("asset_id")),
                    "inner")
                .join(dimFacilityLookUp,
                    sensorReadingDF.col("facility_id").equalTo(dimFacilityLookUp.col("facility_id")),
                    "inner")
                .join(dimDateLookUp,
                    date_format(sensorReadingDF.col("reading_timestamp"), "yyyyMMddHH")
                        .equalTo(dimDateLookUp.col("date_key")),
                    "inner")
                .join(dimStatusLookUp,
                    sensorReadingDF.col("operational_status").equalTo(dimStatusLookUp.col("operational_status"))
                    .and(sensorReadingDF.col("alert_level").equalTo(dimStatusLookUp.col("alert_level"))),
                    "inner")
                .join(dimRegionLookUp,
                    sensorReadingDF.col("region").equalTo(dimRegionLookUp.col("region_name")),
                    "inner")
                .select(
                    sensorReadingDF.col("reading_id"),
                    dimAssetLookUp.col("asset_key"),
                    dimFacilityLookUp.col("facility_key"),
                    dimDateLookUp.col("date_key"),
                    dimStatusLookUp.col("status_key"),
                    dimRegionLookUp.col("region_key"),
                    sensorReadingDF.col("reading_timestamp"),
                    sensorReadingDF.col("power_output"),
                    sensorReadingDF.col("voltage"),
                    sensorReadingDF.col("current"),
                    sensorReadingDF.col("temperature"),
                    sensorReadingDF.col("vibration_level"),
                    sensorReadingDF.col("frequency"),
                    sensorReadingDF.col("power_factor"),
                    sensorReadingDF.col("ambient_temperature"),
                    sensorReadingDF.col("wind_speed"),
                    sensorReadingDF.col("solar_irradiance"),
                    sensorReadingDF.col("efficiency"),
                    sensorReadingDF.col("capacity_factor")
                );

            factSensorReadingDF.writeTo(icebergFactSensorReading).using("iceberg").createOrReplace();
            LOGGER.info("Fact table 'fact_sensor_reading' created.");

            // Run analytics
            // Optional: Run sample analytical queries here
            LOGGER.info("ETL transformation completed successfully");

            spark.stop();
            LOGGER.info("Spark session stopped.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Dataset<Row> getSensorReadingDF(SparkUtil sparkUtil, SparkSession spark) {
        String keyspace = "energy_ks";
        String table = "sensor_readings_by_asset";
        Dataset<Row> df = sparkUtil.readCassandraTable(spark, keyspace, table);
        LOGGER.info("Connected to Cassandra!");
        df.cache();
        LOGGER.info("Cassandra Keyspace: " + keyspace + ", Table: " + table + 
                    ", Record Count: " + df.count());
        return df;
    }
}
```

---

## Realistic Analytical Queries

These queries represent real-world use cases for energy operations teams.

### 1. Regional Power Generation Overview

```sql
-- Total power output by region for the last 24 hours
SELECT 
    region,
    asset_type,
    COUNT(DISTINCT asset_id) as num_assets,
    AVG(power_output) as avg_power_kw,
    SUM(power_output) as total_power_kw,
    MAX(power_output) as peak_power_kw
FROM iceberg_data.energy_data.sensor_readings
WHERE year = 2025 AND month = 11 AND day >= 27
GROUP BY region, asset_type
ORDER BY total_power_kw DESC;
```

**Use Case**: Executive dashboard showing which regions are producing the most energy.

### 2. Critical Alerts Requiring Immediate Action

```sql
-- Assets with critical alerts in the last hour
SELECT 
    asset_name,
    asset_type,
    facility_name,
    region,
    alert_level,
    operational_status,
    MAX(reading_timestamp) as last_alert_time,
    COUNT(*) as alert_count,
    AVG(temperature) as avg_temp,
    AVG(vibration_level) as avg_vibration
FROM iceberg_data.energy_data.sensor_readings
WHERE alert_level = 'critical'
  AND reading_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY asset_name, asset_type, facility_name, region, alert_level, operational_status
ORDER BY alert_count DESC, last_alert_time DESC
LIMIT 20;
```

**Use Case**: Operations team monitors critical equipment failures.

### 3. Wind Turbine Performance Analysis

```sql
-- Wind turbine power output vs wind speed correlation
SELECT 
    CAST(wind_speed AS INT) as wind_speed_ms,
    COUNT(*) as reading_count,
    AVG(power_output) as avg_power_kw,
    MAX(power_output) as max_power_kw,
    AVG(efficiency) as avg_efficiency,
    COUNT(CASE WHEN alert_level IN ('warning', 'critical') THEN 1 END) as alerts
FROM iceberg_data.energy_data.sensor_readings
WHERE asset_type = 'wind_turbine'
  AND wind_speed IS NOT NULL
  AND wind_speed > 0
GROUP BY CAST(wind_speed AS INT)
ORDER BY wind_speed_ms;
```

**Use Case**: Understand optimal wind speeds and identify underperforming turbines.

### 4. Solar Panel Output by Time of Day

```sql
-- Solar generation patterns throughout the day
SELECT 
    hour,
    AVG(power_output) as avg_power_kw,
    AVG(solar_irradiance) as avg_irradiance,
    AVG(efficiency) as avg_efficiency,
    AVG(temperature) as avg_panel_temp,
    COUNT(DISTINCT asset_id) as num_panels
FROM iceberg_data.energy_data.sensor_readings
WHERE asset_type = 'solar_panel'
  AND year = 2025 AND month = 11
GROUP BY hour
ORDER BY hour;
```

**Use Case**: Forecast solar generation capacity for grid balancing.

### 5. Temperature Anomaly Detection

```sql
-- Assets with abnormally high temperatures (predictive maintenance)
SELECT 
    asset_name,
    asset_type,
    facility_name,
    AVG(temperature) as avg_temp,
    MAX(temperature) as max_temp,
    STDDEV(temperature) as temp_variation,
    COUNT(CASE WHEN temperature > 75 THEN 1 END) as overtemp_count,
    COUNT(*) as total_readings
FROM iceberg_data.energy_data.sensor_readings
WHERE year = 2025 AND month = 11 AND day >= 27
  AND asset_type IN ('wind_turbine', 'substation')
GROUP BY asset_name, asset_type, facility_name
HAVING MAX(temperature) > 75 OR STDDEV(temperature) > 15
ORDER BY max_temp DESC, temp_variation DESC
LIMIT 20;
```

**Use Case**: Identify equipment at risk of thermal failure.

### 6. Peak Hour vs Off-Peak Analysis

```sql
-- Compare generation during peak demand hours (8 AM - 8 PM)
SELECT 
    CASE 
        WHEN hour BETWEEN 8 AND 20 THEN 'Peak Hours (8AM-8PM)'
        ELSE 'Off-Peak Hours'
    END as demand_period,
    region,
    AVG(power_output) as avg_power_kw,
    SUM(power_output) as total_power_kw,
    COUNT(*) as reading_count
FROM iceberg_data.energy_data.sensor_readings
WHERE year = 2025 AND month = 11 AND day >= 27
GROUP BY 
    CASE 
        WHEN hour BETWEEN 8 AND 20 THEN 'Peak Hours (8AM-8PM)'
        ELSE 'Off-Peak Hours'
    END,
    region
ORDER BY demand_period, total_power_kw DESC;
```

**Use Case**: Ensure sufficient generation during peak demand times.

### 7. Facility Performance Ranking

```sql
-- Rank facilities by production and efficiency
SELECT 
    facility_name,
    region,
    COUNT(DISTINCT asset_id) as num_assets,
    AVG(power_output) as avg_power_per_asset_kw,
    SUM(power_output) as total_facility_power_kw,
    AVG(efficiency) as avg_efficiency,
    AVG(capacity_factor) as avg_capacity_factor,
    COUNT(CASE WHEN alert_level = 'critical' THEN 1 END) as critical_alerts
FROM iceberg_data.energy_data.sensor_readings
WHERE year = 2025 AND month = 11 AND day >= 27
GROUP BY facility_name, region
ORDER BY total_facility_power_kw DESC, avg_efficiency DESC;
```

**Use Case**: Identify top-performing and underperforming facilities.

### 8. Grid Frequency Stability Monitoring

```sql
-- Monitor grid frequency deviations (critical for grid stability)
SELECT 
    DATE_TRUNC('hour', reading_timestamp) as hour,
    region,
    AVG(frequency) as avg_frequency,
    MIN(frequency) as min_frequency,
    MAX(frequency) as max_frequency,
    STDDEV(frequency) as frequency_stddev,
    COUNT(CASE WHEN ABS(frequency - 50.0) > 0.2 THEN 1 END) as warning_deviations,
    COUNT(CASE WHEN ABS(frequency - 50.0) > 0.5 THEN 1 END) as critical_deviations
FROM iceberg_data.energy_data.sensor_readings
WHERE frequency IS NOT NULL
  AND year = 2025 AND month = 11 AND day >= 27
GROUP BY DATE_TRUNC('hour', reading_timestamp), region
HAVING COUNT(CASE WHEN ABS(frequency - 50.0) > 0.2 THEN 1 END) > 0
ORDER BY hour DESC, critical_deviations DESC;
```

**Use Case**: Detect grid instability events that could cause blackouts.

### 9. Vibration-Based Predictive Maintenance

```sql
-- Identify wind turbines with excessive vibration (bearing failure indicator)
SELECT 
    asset_name,
    facility_name,
    region,
    AVG(vibration_level) as avg_vibration,
    MAX(vibration_level) as max_vibration,
    AVG(temperature) as avg_temp,
    AVG(power_output) as avg_power,
    COUNT(CASE WHEN vibration_level > 10 THEN 1 END) as high_vibration_count,
    COUNT(*) as total_readings,
    MAX(reading_timestamp) as last_reading
FROM iceberg_data.energy_data.sensor_readings
WHERE asset_type = 'wind_turbine'
  AND year = 2025 AND month = 11 AND day >= 27
GROUP BY asset_name, facility_name, region
HAVING AVG(vibration_level) > 8 OR MAX(vibration_level) > 12
ORDER BY avg_vibration DESC, max_vibration DESC
LIMIT 20;
```

**Use Case**: Schedule maintenance before catastrophic turbine failures.

### 10. Asset Downtime Analysis

```sql
-- Calculate downtime and availability for each asset
SELECT 
    asset_name,
    asset_type,
    facility_name,
    region,
    COUNT(CASE WHEN operational_status = 'online' THEN 1 END) as online_readings,
    COUNT(CASE WHEN operational_status IN ('offline', 'maintenance') THEN 1 END) as downtime_readings,
    COUNT(*) as total_readings,
    ROUND(100.0 * COUNT(CASE WHEN operational_status = 'online' THEN 1 END) / COUNT(*), 2) as availability_percent,
    AVG(CASE WHEN operational_status = 'online' THEN power_output END) as avg_power_when_online
FROM iceberg_data.energy_data.sensor_readings
WHERE year = 2025 AND month = 11
GROUP BY asset_name, asset_type, facility_name, region
HAVING COUNT(*) > 100  -- At least 100 readings to be statistically meaningful
ORDER BY availability_percent ASC, downtime_readings DESC
LIMIT 20;
```

**Use Case**: Track asset reliability and schedule preventive maintenance.

### 11. Time-Series Trend Analysis for Specific Asset

```sql
-- Hourly trend for a specific wind turbine over last 7 days
SELECT 
    DATE_TRUNC('hour', reading_timestamp) as hour,
    AVG(power_output) as avg_power_kw,
    AVG(wind_speed) as avg_wind_speed,
    AVG(temperature) as avg_temp,
    AVG(vibration_level) as avg_vibration,
    MAX(alert_level) as worst_alert_level,
    COUNT(*) as reading_count
FROM iceberg_data.energy_data.sensor_readings
WHERE asset_name = 'North-WT-001'
  AND year = 2025 AND month = 11 AND day >= 21
GROUP BY DATE_TRUNC('hour', reading_timestamp)
ORDER BY hour DESC;
```

**Use Case**: Deep-dive analysis of specific asset performance over time.

### 12. Cross-Facility Comparison

```sql
-- Compare performance across wind farms
SELECT 
    facility_name,
    COUNT(DISTINCT asset_id) as turbine_count,
    AVG(power_output) as avg_output_per_turbine,
    AVG(wind_speed) as avg_wind_speed,
    AVG(efficiency) as avg_efficiency,
    SUM(power_output) * 0.016667 as total_energy_kwh,  -- readings every 10 sec
    COUNT(CASE WHEN alert_level IN ('warning', 'critical') THEN 1 END) as total_alerts,
    ROUND(100.0 * COUNT(CASE WHEN operational_status = 'online' THEN 1 END) / COUNT(*), 2) as uptime_percent
FROM iceberg_data.energy_data.sensor_readings
WHERE asset_type = 'wind_turbine'
  AND year = 2025 AND month = 11 AND day >= 27
GROUP BY facility_name
ORDER BY total_energy_kwh DESC;
```

**Use Case**: Benchmark wind farm performance to identify best practices.

---

## Demo Narrative

### Introduction (2 minutes)

**Context**: "PowerGrid Energy operates 850+ distributed energy assets across 5 regions, generating millions of sensor readings daily. Traditional systems can't handle this scale or provide real-time insights."

**Challenge**: "How do you monitor equipment health, predict failures, and optimize grid performance across geographically distributed infrastructure?"

### Part 1: Operational Layer - Cassandra (3 minutes)

**Demo**: Show Cassandra handling high-velocity writes

```sql
-- Show recent readings for a specific wind turbine
SELECT asset_name, reading_timestamp, power_output, wind_speed, temperature, alert_level
FROM energy_ks.sensor_readings_by_asset
WHERE asset_id = <uuid> AND time_bucket = '2025-11-28-11'
ORDER BY reading_timestamp DESC
LIMIT 20;
```

**Key Points**:
- ✅ Partitioned by asset + time bucket for high write throughput
- ✅ Denormalized for fast operational queries
- ✅ Time-series optimized clustering
- ✅ Multi-region capable for geographic distribution

### Part 2: ETL Transformation - Spark (3 minutes)

**Demo**: Run the CassandraToIceberg Spark job

**Show**:
1. Reading 300K+ sensor readings from Cassandra
2. Transforming operational data → partitioned analytics format
3. Creating denormalized Iceberg table with time partitioning
4. Writing to Iceberg in MinIO

**Key Points**:
- ✅ Processes millions of records efficiently
- ✅ Simple denormalized structure (no complex joins)
- ✅ Adds time components (year, month, day, hour) for partitioning
- ✅ Stores in open table format (Iceberg)

### Part 3: Analytics Layer - watsonx.data (5 minutes)

**Demo**: Run analytical queries

**Query 1: Real-Time Monitoring**
```sql
-- Total power generation by region
SELECT region, 
       asset_type,
       SUM(power_output) as total_power_kw,
       AVG(efficiency) as avg_efficiency,
       COUNT(DISTINCT asset_id) as num_assets
FROM energy_data.sensor_readings
GROUP BY region, asset_type
ORDER BY total_power_kw DESC;
```

**Query 2: Predictive Maintenance**
```sql
-- Assets showing signs of degradation
SELECT asset_name, 
       asset_type,
       facility_name,
       AVG(temperature) as avg_temp,
       AVG(vibration_level) as avg_vibration,
       COUNT(CASE WHEN alert_level = 'critical' THEN 1 END) as critical_alerts
FROM energy_data.sensor_readings
GROUP BY asset_name, asset_type, facility_name
HAVING AVG(temperature) > 70 OR AVG(vibration_level) > 8
ORDER BY critical_alerts DESC
LIMIT 20;
```

**Query 3: Grid Optimization**
```sql
-- Peak vs off-peak generation
SELECT 
    CASE WHEN hour BETWEEN 8 AND 20 THEN 'Peak Hours' ELSE 'Off-Peak' END as period,
    SUM(power_output) as total_power_kw,
    AVG(power_output) as avg_power_kw
FROM energy_data.sensor_readings
GROUP BY CASE WHEN hour BETWEEN 8 AND 20 THEN 'Peak Hours' ELSE 'Off-Peak' END;
```

**Query 4: Performance Analysis**
```sql
-- Wind turbine performance vs wind speed
SELECT CAST(wind_speed AS INT) as wind_speed_ms,
       COUNT(*) as reading_count,
       AVG(power_output) as avg_power_kw,
       AVG(efficiency) as avg_efficiency
FROM energy_data.sensor_readings
WHERE asset_type = 'wind_turbine' AND wind_speed IS NOT NULL
GROUP BY CAST(wind_speed AS INT)
ORDER BY wind_speed_ms;
```

### Part 4: Federated Query (2 minutes)

**Demo**: Query both Cassandra and Iceberg simultaneously

```sql
-- Compare real-time (Cassandra) vs historical (Iceberg) for an asset
SELECT 'Real-Time (Cassandra)' as source, 
       AVG(power_output) as avg_power
FROM cassandra_catalog.energy_ks.sensor_readings_by_asset
WHERE asset_name = 'North-WT-001'
ALLOW FILTERING
UNION ALL
SELECT 'Historical (Iceberg)' as source,
       AVG(power_output) as avg_power
FROM iceberg_data.energy_data.sensor_readings
WHERE asset_name = 'North-WT-001';
```

### Conclusion (2 minutes)

**Value Delivered**:
1. ✅ **Scalability**: Handle millions of sensor readings with linear scalability
2. ✅ **Real-Time**: Sub-second operational queries on latest data
3. ✅ **Analytics**: Complex joins and aggregations for insights
4. ✅ **Predictive**: Identify maintenance needs before failures
5. ✅ **Open**: Iceberg format enables AI/ML integration
6. ✅ **Cost-Effective**: Separate storage/compute, pay for what you use

**Business Impact**:
- Reduce downtime through predictive maintenance
- Optimize energy generation and distribution
- Meet regulatory reporting requirements
- Enable digital twin and AI initiatives
- Future-proof with open standards

---

## Implementation Checklist

- [ ] Create DTO classes: `SensorReading.java`, `Asset.java`
- [ ] Create helper class: `EnergyDataHelper.java` (asset and reading generation logic)
- [ ] Update `CassUtil.java` to include `createEnergySchema()` method
- [ ] Create `LoadEnergyReadings.java` main class
- [ ] Update `CassandraToIceberg.java` for energy schema
- [ ] Run analytical queries in watsonx.data Query workspace
- [ ] Update pom.xml dependencies if needed
- [ ] Build JAR: `mvn clean package`
- [ ] Upload to MinIO spark-artifacts bucket
- [ ] Create Spark application in watsonx.data UI
- [ ] Submit job and monitor logs
- [ ] Register tables in Iceberg catalog
- [ ] Run analytics queries

---

**Next Steps**: Would you like me to:
1. Create the complete DTO classes and helper utilities?
2. Generate sample CQL schema creation scripts?
3. Create a detailed data generation algorithm?
4. Design more complex queries for specific use cases?
5. Create a presentation deck for the demo?

