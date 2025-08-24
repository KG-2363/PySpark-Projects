# PySpark Pipeline Learning Project

This project demonstrates common PySpark issues and their solutions through intentionally problematic pipelines followed by optimized versions.

## Project Structure
```
pyspark-pipeline-demo/
├── spark_pipeline_demo.py       # Main pipeline code
├── requirements.txt             # Python dependencies
├── setup_environment.py         # Environment setup script
├── docker-compose.yml          # Docker setup (optional)
├── README.md                   # This file
└── solutions/
    ├── memory_optimization.md   # Memory issue solutions
    ├── skewness_solutions.md   # Data skewness solutions
    └── best_practices.md       # General best practices
```

## Setup Instructions

### Option 1: Local Setup with Java/Spark

#### Prerequisites
- Python 3.8+
- Java 8 or 11
- Apache Spark 3.x

#### Step 1: Install Java
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install openjdk-11-jdk

# macOS
brew install openjdk@11

# Windows
# Download and install from Oracle or use chocolatey:
# choco install openjdk11
```

#### Step 2: Download and Setup Spark
```bash
# Download Spark
wget https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz
tar -xzf spark-3.4.0-bin-hadoop3.tgz
sudo mv spark-3.4.0-bin-hadoop3 /opt/spark

# Set environment variables
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
source ~/.bashrc
```

#### Step 3: Create Python Virtual Environment
```bash
python3 -m venv pyspark-env
source pyspark-env/bin/activate  # On Windows: pyspark-env\Scripts\activate
```

#### Step 4: Install Python Dependencies
```bash
pip install pyspark==3.4.0 py4j
```

### Option 2: Docker Setup (Recommended)

#### Prerequisites
- Docker
- Docker Compose

#### Docker Compose File
Create `docker-compose.yml`:

```yaml
version: '3.8'
services:
  spark-master:
    image: bitnami/spark:3.4.0
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - "8080:8080"
      - "4040:4040"
    volumes:
      - ./:/app
    working_dir: /app

  spark-worker:
    image: bitnami/spark:3.4.0
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=512m
      - SPARK_WORKER_CORES=2
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    depends_on:
      - spark-master
    volumes:
      - ./:/app
    working_dir: /app
```

#### Run with Docker
```bash
docker-compose up -d
docker-compose exec spark-master python spark_pipeline_demo.py
```

### Option 3: VS Code Setup

#### Prerequisites
- VS Code
- Python extension for VS Code

#### Step 1: Create Project Structure
```bash
mkdir pyspark-pipeline-demo
cd pyspark-pipeline-demo
mkdir solutions
```

#### Step 2: Create Requirements File
Create `requirements.txt`:
```
pyspark==3.4.0
py4j==0.10.9.7
jupyter==1.0.0
findspark==2.0.1
```

#### Step 3: Setup VS Code Configuration
Create `.vscode/settings.json`:
```json
{
    "python.defaultInterpreterPath": "./pyspark-env/bin/python",
    "python.terminal.activateEnvironment": true,
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "files.associations": {
        "*.py": "python"
    }
}
```

Create `.vscode/launch.json`:
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Spark Pipeline Demo",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/spark_pipeline_demo.py",
            "console": "integratedTerminal",
            "env": {
                "PYTHONPATH": "${workspaceFolder}",
                "SPARK_HOME": "/opt/spark",
                "PYSPARK_PYTHON": "python3"
            }
        }
    ]
}
```

## Running the Project

### 1. Start the Pipeline
```bash
python spark_pipeline_demo.py
```

### 2. Expected Outputs

#### Phase 1: OOM Failure
```
RUNNING PROBLEMATIC PIPELINE V1
❌ Pipeline failed with error: OutOfMemoryError
```

#### Phase 2: Memory Fixed, Skewness Issues
```
RUNNING PROBLEMATIC PIPELINE V2 - Fixed Memory Issues
✅ Processing completed in 45.23 seconds
⚠️  Note: Processing was slow due to data skewness
```

#### Phase 3: Fully Optimized
```
RUNNING OPTIMIZED PIPELINE V3 - All Issues Fixed
✅ Optimized processing completed in 8.45 seconds
```

## Issues Demonstrated and Solutions

### 1. Out of Memory (OOM) Issues

**Problem:** 
- Using `collect_list()` on large datasets
- Caching large DataFrames
- Driver memory exhaustion

**Solutions:**
- Remove memory-intensive operations
- Increase driver/executor memory
- Use `collect_set()` instead of `collect_list()`
- Avoid unnecessary caching

### 2. Data Skewness Issues

**Problem:**
- 80% of data has the same partition key
- Uneven task distribution
- Some executors are idle while others are overloaded

**Solutions:**
- Salting technique for skewed keys
- Two-phase aggregation
- Repartitioning before operations
- Using broadcast joins for small tables

### 3. Resource Allocation Issues

**Problem:**
- Insufficient memory allocation
- Wrong number of partitions
- Improper executor configuration

**Solutions:**
- Proper memory configuration
- Dynamic partition sizing
- Optimal executor configuration

## Monitoring and Debugging

### 1. Spark UI
Access Spark UI at: http://localhost:4040
- Monitor job progress
- Check executor utilization
- Identify bottlenecks

### 2. Key Metrics to Watch
- Task duration distribution
- Shuffle read/write sizes
- GC time
- Memory usage per executor

### 3. Common Warning Signs
- Tasks taking significantly longer than others
- High GC time (>10% of task time)
- Frequent spilling to disk
- Uneven data distribution

## Best Practices Demonstrated

1. **Memory Management**
   - Avoid collecting large results to driver
   - Use appropriate data types
   - Minimize shuffles

2. **Data Skewness Handling**
   - Identify skewed keys early
   - Use salting for aggregations
   - Consider broadcast joins

3. **Pipeline Optimization**
   - Cache only when necessary
   - Use appropriate file formats (Parquet)
   - Optimize partition counts

4. **Incremental Processing**
   - Process only new/changed data
   - Use partitioning strategies
   - Implement proper error handling

## Troubleshooting

### Common Errors and Solutions

1. **Java Not Found**
   ```bash
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   ```

2. **Spark Not Found**
   ```bash
   export SPARK_HOME=/opt/spark
   export PATH=$PATH:$SPARK_HOME/bin
   ```

3. **Permission Denied**
   ```bash
   sudo chown -R $USER:$USER /tmp/target_orders*
   ```

4. **Port Already in Use**
   - Change port in spark configuration
   - Kill existing Spark processes

## Next Steps

1. **Experiment with different resource configurations**
2. **Try different data skewness patterns**
3. **Implement custom partitioning strategies**
4. **Add monitoring and alerting**
5. **Integrate with data quality checks**

## Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
