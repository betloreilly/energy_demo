package com.ibm.wxd.datalabs.demo.cass_spark_iceberg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.wxd.datalabs.demo.cass_spark_iceberg.utils.SparkUtil;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

/**
 * ETL job to transform operational sensor data from Cassandra to analytical format in Iceberg
 * 
 * This simplified approach uses a denormalized Iceberg table instead of a star schema,
 * which is more appropriate for time-series IoT data.
 * 
 * Submit via Spark:
 * spark-submit  --conf spark.cassandra.connection.host="<host>" 
 *               --conf spark.cassandra.auth.username="cassandra" 
 *               --conf spark.cassandra.auth.password="cassandra" 
 *               --conf spark.sql.catalog.spark_catalog.warehouse="s3a://analytics/" 
 *               --master "local[*]" 
 *               --class com.ibm.wxd.datalabs.demo.cass_spark_iceberg.CassandraToIceberg 
 *               target/energy-iot-demo-1.0.0.jar
 */
public class CassandraToIceberg {
    private static Logger LOGGER = LoggerFactory.getLogger(CassandraToIceberg.class);

    private static String ICEBERG_SCHEMA = "spark_catalog.energy_data";
    private static String icebergSensorReadings = ICEBERG_SCHEMA + ".sensor_readings";

    public static void main(String[] args) {
        try {
            // Create Spark Session
            SparkUtil sparkUtil = new SparkUtil();
            SparkSession spark = sparkUtil.createSparkSession("EnergyDataETL");
            LOGGER.info("Spark Session created successfully");

            // Read sensor data from Cassandra
            LOGGER.info("Reading data from Cassandra...");
            Dataset<Row> cassandraDF = getSensorReadingDF(sparkUtil, spark);
            
            LOGGER.info("Cassandra data loaded: {} records", cassandraDF.count());
            LOGGER.info("Schema from Cassandra:");
            cassandraDF.printSchema();

            // Transform data for analytics
            // Add time components for efficient partitioning and filtering
            Dataset<Row> transformedDF = cassandraDF
                .withColumn("year", year(col("reading_timestamp")))
                .withColumn("month", month(col("reading_timestamp")))
                .withColumn("day", dayofmonth(col("reading_timestamp")))
                .withColumn("hour", hour(col("reading_timestamp")))
                .select(
                    col("reading_id").cast("string"),
                    col("reading_timestamp"),
                    col("asset_id").cast("string"),
                    col("asset_name"),
                    col("asset_type"),
                    col("facility_id").cast("string"),
                    col("facility_name"),
                    col("region"),
                    col("latitude"),
                    col("longitude"),
                    col("power_output"),
                    col("voltage"),
                    col("current"),
                    col("temperature"),
                    col("vibration_level"),
                    col("frequency"),
                    col("power_factor"),
                    col("ambient_temperature"),
                    col("wind_speed"),
                    col("solar_irradiance"),
                    col("operational_status"),
                    col("alert_level"),
                    col("efficiency"),
                    col("capacity_factor"),
                    col("year"),
                    col("month"),
                    col("day"),
                    col("hour")
                );

            LOGGER.info("Transformed data schema:");
            transformedDF.printSchema();
            
            LOGGER.info("Sample data (first 3 rows):");
            transformedDF.show(3, false);

            // Write to Iceberg with partitioning by date
            LOGGER.info("Writing {} records to Iceberg table: {}", 
                transformedDF.count(), icebergSensorReadings);
            
            transformedDF.writeTo(icebergSensorReadings)
                .using("iceberg")
                .tableProperty("write.format.default", "parquet")
                .tableProperty("write.parquet.compression-codec", "snappy")
                .partitionedBy(col("year"), col("month"), col("day"))
                .createOrReplace();
            
            LOGGER.info("Iceberg table '{}' created successfully", icebergSensorReadings);

            // Verify the data
            LOGGER.info("Verifying Iceberg table...");
            Dataset<Row> icebergDF = spark.table(icebergSensorReadings);
            long icebergCount = icebergDF.count();
            LOGGER.info("Iceberg table record count: {}", icebergCount);
            
            if (icebergCount == transformedDF.count()) {
                LOGGER.info("✓ Data verification successful - record counts match");
            } else {
                LOGGER.warn("⚠ Warning: Record count mismatch. Expected: {}, Got: {}", 
                    transformedDF.count(), icebergCount);
            }

            // Show summary statistics
            LOGGER.info("\n=== Data Summary ===");
            spark.sql("SELECT region, asset_type, COUNT(*) as count " +
                     "FROM " + icebergSensorReadings + " " +
                     "GROUP BY region, asset_type " +
                     "ORDER BY region, asset_type")
                .show(50, false);

            // Show alert summary
            LOGGER.info("\n=== Alert Summary ===");
            spark.sql("SELECT alert_level, COUNT(*) as count " +
                     "FROM " + icebergSensorReadings + " " +
                     "GROUP BY alert_level")
                .show(false);

            // Sample analytical query
            LOGGER.info("\n=== Sample: Top 10 Power Producers ===");
            spark.sql("SELECT asset_name, asset_type, AVG(power_output) as avg_power_kw " +
                     "FROM " + icebergSensorReadings + " " +
                     "WHERE operational_status = 'online' " +
                     "GROUP BY asset_name, asset_type " +
                     "ORDER BY avg_power_kw DESC " +
                     "LIMIT 10")
                .show(false);

            // Stop the Spark session
            spark.stop();
            LOGGER.info("ETL job completed successfully");
            
        } catch (Exception e) {
            LOGGER.error("ETL job failed", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Read sensor readings from Cassandra
     */
    private static Dataset<Row> getSensorReadingDF(SparkUtil sparkUtil, SparkSession spark) {
        String keyspace = "energy_ks";
        String table = "sensor_readings_by_asset";
        
        LOGGER.info("Connecting to Cassandra keyspace: {}, table: {}", keyspace, table);
        Dataset<Row> df = sparkUtil.readCassandraTable(spark, keyspace, table);
        
        LOGGER.info("✓ Connected to Cassandra successfully");
        df.cache();
        
        long recordCount = df.count();
        LOGGER.info("Record count: {}", recordCount);
        
        if (recordCount == 0) {
            LOGGER.warn("⚠ Warning: No data found in Cassandra table. " +
                       "Please run LoadEnergyReadings first to generate data.");
        }
        
        return df;
    }
}

