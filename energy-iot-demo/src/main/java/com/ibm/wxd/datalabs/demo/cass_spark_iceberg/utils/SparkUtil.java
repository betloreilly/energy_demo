package com.ibm.wxd.datalabs.demo.cass_spark_iceberg.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for Spark session creation and Cassandra table reading
 */
public class SparkUtil {
    private static Logger LOGGER = LoggerFactory.getLogger(SparkUtil.class);
    
    /**
     * Create a Spark session configured for Iceberg and Cassandra
     */
    public SparkSession createSparkSession(String appName) {
        LOGGER.info("Creating Spark session: {}", appName);
        
        // Get Cassandra host from system property or use default
        String cassandraHost = System.getProperty("spark.cassandra.connection.host", "localhost");
        LOGGER.info("Cassandra host: {}", cassandraHost);
        
        SparkSession spark = SparkSession.builder()
            .appName(appName)
            .config("spark.cassandra.connection.host", cassandraHost)
            .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", 
                "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .getOrCreate();
        
        LOGGER.info("Spark session created successfully");
        LOGGER.info("Spark version: {}", spark.version());
        
        return spark;
    }
    
    /**
     * Read a Cassandra table into a Spark DataFrame
     */
    public Dataset<Row> readCassandraTable(SparkSession spark, String keyspace, String table) {
        LOGGER.info("Reading Cassandra table: {}.{}", keyspace, table);
        
        Dataset<Row> df = spark.read()
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", keyspace)
            .option("table", table)
            .load();
        
        LOGGER.info("Successfully loaded table: {}.{}", keyspace, table);
        
        return df;
    }
}

