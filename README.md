# 🚀 Energy IoT Demo - Cassandra to Iceberg on EC2

**Energy Sector IoT Analytics with IBM watsonx.data**

![IBM watsonx.data](https://img.shields.io/badge/IBM-watsonx.data-blue) ![Apache Cassandra](https://img.shields.io/badge/Apache-Cassandra-1287B1) ![Apache Spark](https://img.shields.io/badge/Apache-Spark-E25A1C) ![Apache Iceberg](https://img.shields.io/badge/Apache-Iceberg-00A1E0)

_Transform operational IoT sensor data from Cassandra to governed analytics in Iceberg_

---

## 📂 Repository Structure

```
watsonx_data/
├── README.md                          ← You are here! Complete guide
├── energy-iot-demo/                   ← Maven project (cd here, mvn clean package)
│   ├── pom.xml
│   └── src/main/java/...
└── energy-sector-demo-design.md       ← Additional queries & business context
```

**This README is your complete guide from start to finish.** Just follow it sequentially.

---

## 📋 Table of Contents

* [🎯 Overview](#-overview)
* [⚙️ Prerequisites](#️-prerequisites)
* [🔧 Installation Steps](#-installation-steps)
  * [A. EC2 Infrastructure Setup](#a-ec2-infrastructure-setup)
  * [B. Build the JAR File](#b-build-the-jar-file)
  * [C. DataStax HCD Setup](#c-datastax-hcd-setup)
* [📊 Load Sample Data](#-load-sample-data)
* [🔗 Connect Cassandra to watsonx.data](#-connect-cassandra-to-watsonxdata)
* [⚡ Run Spark ETL Job](#-run-spark-etl-job)
* [🔍 Query Iceberg Tables](#-query-iceberg-tables)
* [📚 References](#-references)
* [🛠️ Troubleshooting](#️-troubleshooting)

---

## 🎯 Overview

### Purpose

Demonstrate **real-world energy sector IoT analytics** using:
- **DataStax HCD (Cassandra)** for high-velocity operational data
- **IBM watsonx.data** for governed analytics and AI-ready data
- **Apache Spark** for ETL transformation
- **Apache Iceberg** for open table format

### Architecture

```
┌─────────────┐         SSH Tunnel          ┌──────────────────┐
│   Laptop    │◄──────────────────────────►│   EC2 Instance   │
│  (Browser)  │    Port 9443 forwarded     │   RHEL 8/9       │
└─────────────┘                             └──────────────────┘
                                                     │
                                            ┌────────┴────────┐
                                            │                 │
                                    ┌───────▼──────┐  ┌──────▼──────┐
                                    │  watsonx.data │  │ DataStax HCD│
                                    │   (kind)      │  │ (Cassandra) │
                                    │               │  │             │
                                    │  • Presto     │  │  • 850      │
                                    │  • Spark      │  │    assets   │
                                    │  • MinIO      │  │  • 306K     │
                                    │  • Iceberg    │  │    readings │
                                    └───────────────┘  └─────────────┘
```

### Use Case: Energy Infrastructure Monitoring

Monitor **850 distributed energy assets**:
- 500 wind turbines
- 200 solar panels  
- 50 substations
- 100 transmission lines

**Real-time operational queries** (Cassandra) + **Historical analytics** (Iceberg)

---

## ⚙️ Prerequisites

### EC2 Instance Requirements

| Requirement | Specification |
|------------|---------------|
| **Instance Type** | m5.2xlarge (8 vCPU, 32 GB RAM) minimum |
| **OS** | RHEL 8 or RHEL 9 |
| **Storage** | 100 GB+ EBS volume |
| **Security Group** | SSH (22) from your IP |

### Software Requirements on EC2

- ✅ Java 11+
- ✅ Maven 3.6+
- ✅ kubectl
- ✅ kind (Kubernetes in Docker)
- ✅ Docker or Podman
- ✅ MinIO client (`mc`)

### Local Machine Requirements

- ✅ SSH key pair (e.g., `your-key.pem`)
- ✅ Modern web browser
- ✅ Terminal/SSH client

---

## 🔧 Installation Steps

### A. EC2 Infrastructure Setup

#### 1. Launch EC2 Instance

```bash
# Launch RHEL 8/9 instance
Instance Type: m5.2xlarge
Storage: 100 GB gp3
Security Group: SSH (22) from your IP
```

#### 2. Connect and Install Prerequisites

```bash
# Connect to EC2
ssh -i your-key.pem ec2-user@<EC2_PUBLIC_IP>

# Install Java
sudo yum install -y java-11-openjdk java-11-openjdk-devel

# Install Maven
sudo yum install -y maven

# Install kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Install kind
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind

# Install Docker (or Podman)
sudo yum install -y docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user

# Install MinIO client
curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/

# Verify installations
java -version
mvn -version
kubectl version --client
kind version
docker --version
mc --version
```

#### 3. Install watsonx.data Developer Edition

```bash
# Create kind cluster
cat > kind-config.yaml <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: 9443
    hostPort: 9443
    protocol: TCP
EOF

kind create cluster --name watsonx-cluster --config kind-config.yaml

# Install watsonx.data Developer Edition
# Follow IBM documentation for latest version:
# https://www.ibm.com/docs/en/watsonxdata/2.0.x?topic=developer-edition-new-version

# Verify installation (should see 22 pods)
kubectl get pods -n wxd
```

#### 4. Set Up UI Access

**On EC2** (Terminal 1):
```bash
# Port-forward the UI service (keep running)
kubectl port-forward -n wxd svc/lhconsole-ui-svc 9443:443
```

**On your laptop** (Terminal 2):
```bash
# Create SSH tunnel (keep running)
ssh -i your-key.pem -L 9443:localhost:9443 ec2-user@<EC2_PUBLIC_IP>
```

**Access UI**: Open browser to `https://localhost:9443`
- Accept self-signed certificate warning
- Click "Advanced → Proceed to localhost"

---

### B. Build the JAR File

#### 1. Clone or Copy the Project

```bash
cd ~

# If this is a Git repo, clone it:
git clone <your-repo-url>
cd watsonx_data/energy-iot-demo

# OR copy the energy-iot-demo directory to EC2
scp -i your-key.pem -r energy-iot-demo ec2-user@<EC2_PUBLIC_IP>:~/
ssh -i your-key.pem ec2-user@<EC2_PUBLIC_IP>
cd ~/energy-iot-demo
```

#### 2. Update Cassandra Connection Settings

Edit `src/main/java/com/ibm/wxd/datalabs/demo/cass_spark_iceberg/utils/CassUtil.java`:

```java
// Line 16-21: Update these settings for your environment
private static final String CASSANDRA_HOST = "127.0.0.1";      // ← EC2 local IP
private static final int CASSANDRA_PORT = 9042;
private static final String CASSANDRA_DATACENTER = "datacenter1";
private static final String CASSANDRA_USERNAME = "cassandra";
private static final String CASSANDRA_PASSWORD = "cassandra";
```

#### 3. Build the Application

```bash
cd ~/energy-iot-demo

# Clean and compile
mvn clean compile

# Package with dependencies
mvn package
```

**Expected output:**
```
[INFO] Building jar: ~/energy-iot-demo/target/energy-iot-demo-1.0.0.jar
[INFO] BUILD SUCCESS
[INFO] Total time: X.XXX s
```

**Result**: `target/energy-iot-demo-1.0.0.jar` (~20-30 MB)

#### 4. Verify the JAR

```bash
# Check JAR size
ls -lh target/energy-iot-demo-1.0.0.jar

# List main classes
jar tf target/energy-iot-demo-1.0.0.jar | grep "LoadEnergyReadings\|CassandraToIceberg"
```

---

### C. DataStax HCD Setup

#### 1. Download and Install HCD

```bash
cd ~
wget https://github.com/datastax/hyper-converged-database/releases/download/1.2.3/hcd-1.2.3-bin.tar.gz
tar -xzf hcd-1.2.3-bin.tar.gz
cd hcd-1.2.3

# Start Cassandra
bin/cassandra -R
```

#### 2. Verify Cassandra is Running

```bash
# Wait ~30 seconds, then check status
bin/nodetool status

# Expected output:
# UN  127.0.0.1  ...  (Up/Normal)
```

---

## 📊 Load Sample Data

### 1. Create Cassandra Keyspace and Table

First, manually create the schema in Cassandra:

```bash
cd ~/hcd-1.2.3
bin/cqlsh
```

In the CQL shell, run:

```sql
-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS energy_ks 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Create table
CREATE TABLE IF NOT EXISTS energy_ks.sensor_readings_by_asset (
    asset_id UUID,
    time_bucket TEXT,
    reading_timestamp TIMESTAMP,
    reading_id UUID,
    power_output DOUBLE,
    voltage DOUBLE,
    current DOUBLE,
    temperature DOUBLE,
    vibration_level DOUBLE,
    frequency DOUBLE,
    power_factor DOUBLE,
    ambient_temperature DOUBLE,
    wind_speed DOUBLE,
    solar_irradiance DOUBLE,
    asset_name TEXT,
    asset_type TEXT,
    facility_id UUID,
    facility_name TEXT,
    region TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    operational_status TEXT,
    alert_level TEXT,
    efficiency DOUBLE,
    capacity_factor DOUBLE,
    PRIMARY KEY ((asset_id, time_bucket), reading_timestamp, reading_id)
) WITH CLUSTERING ORDER BY (reading_timestamp DESC, reading_id DESC);

-- Verify schema creation
DESCRIBE KEYSPACE energy_ks;
DESCRIBE TABLE energy_ks.sensor_readings_by_asset;

-- Exit CQL shell
exit;
```

### 2. Run Data Loader

Now load the sample data:

```bash
cd ~/energy-iot-demo

# Load 850 assets with 360 readings each (1 hour of data = 306,000 readings)
java -cp target/energy-iot-demo-1.0.0.jar \
  com.ibm.wxd.datalabs.demo.cass_spark_iceberg.LoadEnergyReadings \
  850 360
```

**This takes 5-10 minutes.** Expected output:

```
INFO  LoadEnergyReadings - === Energy Sector Data Generation ===
INFO  LoadEnergyReadings - Total readings to generate: 306000
INFO  LoadEnergyReadings - Generating 850 assets...
INFO  LoadEnergyReadings - Assets generated: 500 wind turbines, 200 solar panels...
INFO  LoadEnergyReadings - Inserted 1000 / 306000 readings...
INFO  LoadEnergyReadings - Successfully inserted 306000 sensor readings in XX seconds
```

### 3. Verify Data in Cassandra

```bash
cd ~/hcd-1.2.3
bin/cqlsh

# In CQL shell:
USE energy_ks;

SELECT COUNT(*) FROM sensor_readings_by_asset;
-- Should return: 306000

SELECT asset_name, asset_type, reading_timestamp, power_output 
FROM sensor_readings_by_asset 
LIMIT 10;

exit;
```

---

## 🔗 Connect Cassandra to watsonx.data

Now that we have data in Cassandra, let's connect it to watsonx.data for federated queries.

### 1. Access watsonx.data UI

Open `https://localhost:9443` in your browser (ensure SSH tunnel is still running)

### 2. Add Cassandra as Data Source

1. Go to **Infrastructure Manager**
2. Click **Add database** → **Apache Cassandra**
3. Fill in connection details:

```
Display name: cassandra-energy
Host: <EC2_PRIVATE_IP>    # Run this command ON EC2: curl http://169.254.169.254/latest/meta-data/local-ipv4
Port: 9042
Username: cassandra
Password: cassandra
```

4. Click **Test connection** → Should succeed
5. Click **Add**

### 3. Verify Cassandra Connection

Go to **Query workspace** and run:

```sql
-- List all catalogs (should see cassandra_energy)
SHOW CATALOGS;

-- List schemas in Cassandra catalog (should see energy_ks)
SHOW SCHEMAS FROM cassandra_energy;

-- List tables in energy_ks schema
SHOW TABLES FROM cassandra_energy.energy_ks;
```

### 4. Query via watsonx.data (Federated Query)

Now you can query Cassandra data through watsonx.data:

```sql
-- Count all readings
SELECT COUNT(*) FROM cassandra_energy.energy_ks.sensor_readings_by_asset;
-- Should return: 306000

-- Query operational data
SELECT asset_name, asset_type, power_output, alert_level
FROM cassandra_energy.energy_ks.sensor_readings_by_asset
WHERE alert_level = 'critical'
ALLOW FILTERING
LIMIT 20;

-- Count by asset type
SELECT asset_type, COUNT(*) as count
FROM cassandra_energy.energy_ks.sensor_readings_by_asset
GROUP BY asset_type
ALLOW FILTERING;
```

---

## ⚡ Run Spark ETL Job

### 1. Upload JAR to MinIO

**On EC2** (Terminal 1):
```bash
# Port-forward MinIO (keep running)
kubectl -n wxd port-forward svc/ibm-lh-minio-svc 9000:9000
```

**On EC2** (Terminal 2):
```bash
# Configure MinIO client
mc alias set wxd http://127.0.0.1:9000 dummyvalue dummyvalue

# List buckets
mc ls wxd/

# Upload JAR
cd ~/energy-iot-demo
mc cp target/energy-iot-demo-1.0.0.jar wxd/spark-artifacts/

# Verify upload
mc ls wxd/spark-artifacts/energy-iot-demo-1.0.0.jar
```

### 2. Create Spark Application

1. Open watsonx.data UI: `https://localhost:9443`
2. Go to **Infrastructure Manager** → **Applications**
3. Click **Create application**
4. Paste this JSON (update `spark.cassandra.connection.host` with your EC2 private IP):

```json
{
  "application_details": {
    "application": "s3a://spark-artifacts/energy-iot-demo-1.0.0.jar",
    "class": "com.ibm.wxd.datalabs.demo.cass_spark_iceberg.CassandraToIceberg",
    "conf": {
      "spark.cassandra.connection.host": "172.31.26.107",
      "spark.cassandra.connection.port": "9042",
      "spark.cassandra.auth.username": "cassandra",
      "spark.cassandra.auth.password": "cassandra",
      "spark.sql.catalog.spark_catalog.type": "iceberg",
      "spark.sql.catalog.spark_catalog.warehouse": "s3a://analytics/",
      "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "spark.hadoop.fs.s3a.path.style.access": "true",
      "spark.hadoop.fs.s3a.bucket.spark-artifacts.endpoint": "http://ibm-lh-minio-svc:9000",
      "spark.hadoop.fs.s3a.bucket.spark-artifacts.access.key": "dummyvalue",
      "spark.hadoop.fs.s3a.bucket.spark-artifacts.secret.key": "dummyvalue",
      "spark.hadoop.fs.s3a.bucket.spark-artifacts.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
      "spark.hadoop.fs.s3a.bucket.analytics.endpoint": "http://ibm-lh-minio-svc:9000",
      "spark.hadoop.fs.s3a.bucket.analytics.access.key": "dummyvalue",
      "spark.hadoop.fs.s3a.bucket.analytics.secret.key": "dummyvalue",
      "spark.hadoop.fs.s3a.bucket.analytics.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    }
  },
  "deploy_mode": "local"
}
```

**Get your EC2 private IP (run this ON the EC2 instance):**
```bash
curl http://169.254.169.254/latest/meta-data/local-ipv4
# Example output: 172.31.26.107
```

5. Click **Submit application**

### 3. Monitor Spark Job

Watch the logs in the UI. Expected messages:

```
INFO CassandraToIceberg - Spark Session created successfully
INFO CassandraToIceberg - Reading data from Cassandra...
INFO CassandraToIceberg - Record count: 306000
INFO CassandraToIceberg - Writing 306000 records to Iceberg table
INFO CassandraToIceberg - Iceberg table 'spark_catalog.energy_data.sensor_readings' created
INFO CassandraToIceberg - ✓ Data verification successful
INFO CassandraToIceberg - ETL job completed successfully
```

**This takes 3-5 minutes.**

### 4. Verify Data in MinIO

```bash
# Check that Iceberg files were created
mc tree wxd/analytics/

# Expected structure:
# wxd/analytics/
# └─ energy_data/
#    └─ sensor_readings/
#       ├─ data/
#       └─ metadata/
```

---

## 🔍 Query Iceberg Tables

### 1. Add MinIO Storage (If Not Added)

In **Infrastructure Manager**:
1. Click **Add component** → **Storage**
2. Fill in:
   - **Display name**: `analytics-storage`
   - **Bucket name**: `analytics`
   - **Endpoint**: `http://ibm-lh-minio-svc:9000`
   - **Access/Secret key**: `dummyvalue` / `dummyvalue`
3. Check **Associate Catalog**:
   - **Catalog type**: Apache Iceberg
   - **Catalog name**: `iceberg_data`
4. **Test connection** → **Create**

### 2. Register the Iceberg Table

In **Query workspace**:

```sql
-- Register the table
CALL iceberg_data.system.register_table(
  'energy_data', 
  'sensor_readings', 
  's3a://analytics/energy_data/sensor_readings'
);
```

### 3. Verify Table Registration

```sql
-- List schemas
SHOW SCHEMAS FROM iceberg_data;
-- Should show: energy_data

-- List tables
SHOW TABLES FROM iceberg_data.energy_data;
-- Should show: sensor_readings

-- Preview data
SELECT * FROM iceberg_data.energy_data.sensor_readings LIMIT 5;

-- Count records
SELECT COUNT(*) FROM iceberg_data.energy_data.sensor_readings;
-- Should return: 306000
```

### 4. Run Analytical Queries

#### Regional Power Generation

```sql
SELECT 
    region,
    asset_type,
    COUNT(DISTINCT asset_id) as num_assets,
    ROUND(AVG(power_output), 2) as avg_power_kw,
    ROUND(SUM(power_output), 2) as total_power_kw
FROM iceberg_data.energy_data.sensor_readings
GROUP BY region, asset_type
ORDER BY total_power_kw DESC;
```

#### Critical Alerts Requiring Maintenance

```sql
SELECT 
    asset_name,
    asset_type,
    facility_name,
    region,
    COUNT(*) as alert_count,
    ROUND(AVG(temperature), 2) as avg_temp,
    ROUND(AVG(vibration_level), 2) as avg_vibration
FROM iceberg_data.energy_data.sensor_readings
WHERE alert_level = 'critical'
GROUP BY asset_name, asset_type, facility_name, region
ORDER BY alert_count DESC
LIMIT 20;
```

#### Wind Turbine Performance Analysis

```sql
SELECT 
    CAST(wind_speed AS INT) as wind_speed_ms,
    COUNT(*) as reading_count,
    ROUND(AVG(power_output), 2) as avg_power_kw,
    ROUND(MAX(power_output), 2) as max_power_kw,
    ROUND(AVG(efficiency), 2) as avg_efficiency
FROM iceberg_data.energy_data.sensor_readings
WHERE asset_type = 'wind_turbine'
  AND wind_speed IS NOT NULL
  AND wind_speed > 0
GROUP BY CAST(wind_speed AS INT)
ORDER BY wind_speed_ms;
```

#### Predictive Maintenance Candidates

```sql
SELECT 
    asset_name,
    asset_type,
    facility_name,
    ROUND(AVG(temperature), 2) as avg_temp,
    ROUND(MAX(temperature), 2) as max_temp,
    ROUND(AVG(vibration_level), 2) as avg_vibration,
    COUNT(CASE WHEN temperature > 75 THEN 1 END) as overtemp_count
FROM iceberg_data.energy_data.sensor_readings
WHERE asset_type IN ('wind_turbine', 'substation')
GROUP BY asset_name, asset_type, facility_name
HAVING MAX(temperature) > 75 OR AVG(vibration_level) > 8
ORDER BY max_temp DESC
LIMIT 20;
```

#### Federated Query (Cassandra + Iceberg)

```sql
-- Compare real-time (Cassandra) vs historical (Iceberg) for same asset
SELECT 'Real-Time (Cassandra)' as source, AVG(power_output) as avg_power
FROM cassandra_energy.energy_ks.sensor_readings_by_asset
WHERE asset_name = 'North-WT-001'
ALLOW FILTERING

UNION ALL

SELECT 'Historical (Iceberg)' as source, AVG(power_output) as avg_power
FROM iceberg_data.energy_data.sensor_readings
WHERE asset_name = 'North-WT-001';
```

**More queries available in:** `energy-sector-demo-design.md`

---

## 📚 References

| Resource | Description | Link |
|----------|-------------|------|
| **IBM watsonx.data Docs** | Official installation guide | [Documentation](https://www.ibm.com/docs/en/watsonxdata/2.0.x) |
| **DataStax HCD** | Hyper-Converged Database | [Download](https://github.com/datastax/hyper-converged-database/releases) |
| **Apache Iceberg** | Open table format specification | [Iceberg Docs](https://iceberg.apache.org/) |
| **Original Example** | wxd-spark-hcd repository | [GitHub](https://github.com/michelderu/wxd-spark-hcd) |

### Project Structure

```
watsonx_data/
├── README.md                          ← **This file**
├── energy-iot-demo/                   ← Maven project (build here)
│   ├── pom.xml
│   └── src/main/java/...
├── energy-sector-demo-design.md       ← Complete queries & design
└── PROJECT-STRUCTURE.md               ← Detailed project organization
```

### Key Concepts

- **Operational (Cassandra)**: Real-time workloads - fast writes, recent data queries
- **Analytics (Iceberg)**: Historical analysis - complex aggregations, trends, and patterns
- **Federated Queries**: Query across Cassandra and Iceberg simultaneously
- **Denormalized Design**: For time-series IoT data, simpler than star schema
- **Time Partitioning**: Efficient pruning for date-based queries

---

## 🛠️ Troubleshooting

### Maven Build Fails

**Problem**: Compilation errors

**Solution**:
```bash
# Check Java version
java -version  # Must be 11+

# Clean and rebuild
cd ~/energy-iot-demo
mvn clean compile

# If dependencies fail
rm -rf ~/.m2/repository
mvn clean install
```

### Cannot Connect to Cassandra

**Problem**: `LoadEnergyReadings` or Spark job fails to connect

**Solution**:
```bash
# Verify Cassandra is running
cd ~/hcd-1.2.3
bin/nodetool status

# Check CassUtil.java has correct settings
grep "CASSANDRA_HOST" ~/energy-iot-demo/src/main/java/.../utils/CassUtil.java

# For Spark: verify EC2 private IP
curl http://169.254.169.254/latest/meta-data/local-ipv4
# Update JSON config with this IP
```

### UI Not Accessible

**Problem**: Cannot access `https://localhost:9443`

**Solution**:
```bash
# On EC2: Check port-forward is running
ps aux | grep port-forward | grep 9443
# If not running:
kubectl port-forward -n wxd svc/lhconsole-ui-svc 9443:443

# On laptop: Check SSH tunnel is active
# Re-run:
ssh -i your-key.pem -L 9443:localhost:9443 ec2-user@<EC2_PUBLIC_IP>
```

### Spark Job Cannot Find JAR

**Problem**: Application fails with ClassNotFoundException

**Solution**:
```bash
# Verify JAR is in MinIO
mc ls wxd/spark-artifacts/energy-iot-demo-1.0.0.jar

# Check JAR path in Spark config matches exactly:
"application": "s3a://spark-artifacts/energy-iot-demo-1.0.0.jar"

# Verify MinIO endpoint in config:
"spark.hadoop.fs.s3a.bucket.spark-artifacts.endpoint": "http://ibm-lh-minio-svc:9000"
```

### No Data in Iceberg After ETL

**Problem**: Spark job succeeds but tables are empty

**Solution**:
```bash
# Check if data files exist
mc tree wxd/analytics/energy_data/

# Verify Cassandra has data
cd ~/hcd-1.2.3
bin/cqlsh -e "SELECT COUNT(*) FROM energy_ks.sensor_readings_by_asset;"

# Review Spark logs for errors in watsonx.data UI
```

### Table Not Showing in Query Workspace

**Problem**: Registered table not visible

**Solution**:
```sql
-- Check catalog exists
SHOW CATALOGS;

-- Try re-registering
CALL iceberg_data.system.register_table(
  'energy_data', 
  'sensor_readings', 
  's3a://analytics/energy_data/sensor_readings'
);

-- Check if storage is associated with catalog in Infrastructure Manager
```

### Which PODS Should Be Running?

Check that all watsonx.data pods are healthy:

```bash
kubectl get pods -n wxd

# Expected: 22 pods total
# - 19-20 Running
# - 2-3 Completed (init jobs)
```

Key pods:
- `lhconsole-ui-*` - UI service
- `ibm-lh-presto-*` - Query engine
- `spark-hb-*` - Spark engine
- `ibm-lh-minio-*` - Object storage

---

## ✅ Success Checklist

- [ ] EC2 instance launched and accessible
- [ ] watsonx.data installed (22 pods running)
- [ ] UI accessible at `https://localhost:9443`
- [ ] Cassandra (HCD) running and accessible
- [ ] JAR built successfully (`target/energy-iot-demo-1.0.0.jar`)
- [ ] 306,000 records loaded into Cassandra
- [ ] JAR uploaded to MinIO
- [ ] Spark ETL job completed successfully
- [ ] Iceberg table registered and queryable
- [ ] Analytical queries return results

**Congratulations! Your energy sector demo is fully deployed!** 🎉

---

## 🚀 Next Steps

- **Scale Up**: Load 1000+ assets with longer time periods
- **Dashboards**: Create visualizations with your BI tool
- **AI/ML**: Build predictive maintenance models
- **Real-Time**: Integrate with Kafka for streaming data
- **Production**: Deploy to production watsonx.data cluster

---

**For detailed information:**
- Complete queries: `energy-sector-demo-design.md`
- Project structure: `PROJECT-STRUCTURE.md`

**IBM Internal Use** - Energy Sector Demo for watsonx.data

