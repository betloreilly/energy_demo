package com.ibm.wxd.datalabs.demo.cass_spark_iceberg.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Utility class for Cassandra connection and schema management
 * 
 * Update the Cassandra connection settings here before building
 */
public class CassUtil {
    private static Logger LOGGER = LoggerFactory.getLogger(CassUtil.class);
    
    public static final String KEYSPACE_NAME = "energy_ks";
    
    // UPDATE THESE SETTINGS FOR YOUR ENVIRONMENT
    private static final String CASSANDRA_HOST = "127.0.0.1";  // Change to your Cassandra host
    private static final int CASSANDRA_PORT = 9042;
    private static final String CASSANDRA_DATACENTER = "datacenter1";  // Change if needed
    private static final String CASSANDRA_USERNAME = "cassandra";
    private static final String CASSANDRA_PASSWORD = "cassandra";
    
    /**
     * Create a CQL session with the configured connection settings
     */
    public CqlSession getLocalCQLSession() {
        LOGGER.info("Creating Cassandra session...");
        LOGGER.info("Connecting to: {}:{}", CASSANDRA_HOST, CASSANDRA_PORT);
        
        return CqlSession.builder()
            .addContactPoint(new InetSocketAddress(CASSANDRA_HOST, CASSANDRA_PORT))
            .withLocalDatacenter(CASSANDRA_DATACENTER)
            .withAuthCredentials(CASSANDRA_USERNAME, CASSANDRA_PASSWORD)
            .build();
    }
    
    /**
     * Create the energy schema (keyspace and table)
     */
    public void createEnergySchema(CqlSession session) {
        LOGGER.info("Creating energy schema...");
        
        // Create keyspace
        String createKeyspace = 
            "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME + " " +
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";
        
        session.execute(SimpleStatement.newInstance(createKeyspace));
        LOGGER.info("Keyspace '{}' created/verified", KEYSPACE_NAME);
        
        // Create table
        String createTable = 
            "CREATE TABLE IF NOT EXISTS " + KEYSPACE_NAME + ".sensor_readings_by_asset (" +
            "asset_id UUID, " +
            "time_bucket TEXT, " +
            "reading_timestamp TIMESTAMP, " +
            "reading_id UUID, " +
            "power_output DOUBLE, " +
            "voltage DOUBLE, " +
            "current DOUBLE, " +
            "temperature DOUBLE, " +
            "vibration_level DOUBLE, " +
            "frequency DOUBLE, " +
            "power_factor DOUBLE, " +
            "ambient_temperature DOUBLE, " +
            "wind_speed DOUBLE, " +
            "solar_irradiance DOUBLE, " +
            "asset_name TEXT, " +
            "asset_type TEXT, " +
            "facility_id UUID, " +
            "facility_name TEXT, " +
            "region TEXT, " +
            "latitude DOUBLE, " +
            "longitude DOUBLE, " +
            "operational_status TEXT, " +
            "alert_level TEXT, " +
            "efficiency DOUBLE, " +
            "capacity_factor DOUBLE, " +
            "PRIMARY KEY ((asset_id, time_bucket), reading_timestamp, reading_id)" +
            ") WITH CLUSTERING ORDER BY (reading_timestamp DESC, reading_id DESC);";
        
        session.execute(SimpleStatement.newInstance(createTable));
        LOGGER.info("Table 'sensor_readings_by_asset' created/verified");
    }
    
    /**
     * Close the CQL session
     */
    public void closeSession(CqlSession session) {
        if (session != null) {
            session.close();
            LOGGER.info("Cassandra session closed");
        }
    }
}

