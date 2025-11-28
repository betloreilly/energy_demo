# Energy IoT Demo - Maven Project

This is the Maven project for the Energy Sector IoT demo.

## Quick Build

```bash
cd energy-iot-demo
mvn clean package
```

**Result**: `target/energy-iot-demo-1.0.0.jar`

## Complete Documentation

**See the main README** in the parent directory:

```bash
cd ..
cat README.md
```

The main README contains:
- Complete installation instructions
- How to build this JAR
- How to load data
- How to run Spark jobs
- How to query results
- Troubleshooting

## Configuration

Before building, update Cassandra connection in:
`src/main/java/com/ibm/wxd/datalabs/demo/cass_spark_iceberg/utils/CassUtil.java`

```java
// Lines 16-21
private static final String CASSANDRA_HOST = "127.0.0.1";  // Change this
private static final int CASSANDRA_PORT = 9042;
```

## Project Structure

```
src/main/java/com/ibm/wxd/datalabs/demo/cass_spark_iceberg/
├── LoadEnergyReadings.java      # Data loader
├── CassandraToIceberg.java      # Spark ETL job
├── dto/
│   ├── Asset.java               # Asset data model
│   └── SensorReading.java       # Sensor reading model
└── utils/
    ├── CassUtil.java            # Cassandra utilities
    ├── EnergyDataHelper.java    # Data generation
    └── SparkUtil.java           # Spark utilities
```

---

**For complete instructions, see `../README.md`**
