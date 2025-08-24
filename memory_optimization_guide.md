# Memory Optimization Solutions

## Common Memory Issues in PySpark

### 1. Driver Out of Memory (OOM)

**Problem:** Driver runs out of memory when collecting large results or caching too much data.

**Symptoms:**
- `OutOfMemoryError: Java heap space`
- `OutOfMemoryError: GC overhead limit exceeded`
- Driver process crashes

**Solutions:**

#### A. Increase Driver Memory
```python
# In SparkSession configuration
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.maxResultSize", "1g") \
    .getOrCreate()
```

#### B. Avoid Collecting Large Results
```python
# ❌ BAD - Don't collect large datasets to driver
large_result = df.collect()  # This can cause OOM

# ✅ GOOD - Use actions that don't bring data to driver
df.write.parquet("output_path")  # Write directly to storage
df.count()  # Only returns a number
df.show(20)  # Only shows sample data
```

#### C. Remove Memory-Intensive Operations
```python
# ❌ BAD - collect_list can consume massive memory
df.groupBy("key").agg(collect_list("values").alias("all_values"))

# ✅ GOOD - Use alternatives
df.groupBy("key").agg(
    count("values").alias("count_values"),
    sum("values").alias("sum_values"),
    first("values").alias("first_value")
)
```

### 2. Executor Out of Memory

**Problem:** Individual executors run out of memory during task execution.

**Symptoms:**
- Tasks failing with OOM errors
- Excessive garbage collection
- Tasks taking much longer than expected

**Solutions:**

#### A. Increase Executor Memory
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.memoryFraction", "0.8") \
    .getOrCreate()
```

#### B. Optimize Data Types
```python
# ❌ BAD - Using string for numeric data
df = df.withColumn("amount", col("amount").cast("string"))

# ✅ GOOD - Use appropriate data types
df = df.withColumn("amount", col("amount").cast("double"))
df = df.withColumn("id", col("id").cast("int"))
```

#### C. Avoid Unnecessary Caching
```python
# ❌ BAD - Caching everything
df1 = df.cache()
df2 = df1.filter(...).cache()
df3 = df2.groupBy(...).cache()

# ✅ GOOD - Cache only when reused multiple times
df_filtered = df.filter(...)
if df_filtered.count() > threshold:  # Only cache if worth it
    df_filtered = df_filtered.cache()
```

### 3. Broadcast Variable Memory Issues

**Problem:** Large broadcast variables consuming too much memory.

**Solutions:**

#### A. Optimize Broadcast Size
```python
# ❌ BAD - Broadcasting large tables
large_lookup = spark.read.table("large_lookup_table")
broadcast_large = broadcast(large_lookup)

# ✅ GOOD - Filter before broadcasting
small_lookup = spark.read.table("large_lookup_table") \
    .filter(col("active") == True) \
    .select("key", "value")
broadcast_small = broadcast(small_lookup)
```

#### B. Use Broadcast Threshold
```python
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

### 4. Memory-Efficient Aggregations

#### A. Two-Phase Aggregation for Skewed Data
```python
# Instead of single aggregation that causes memory issues
def memory_efficient_aggregation(df, group_cols, agg_cols):
    # Phase 1: Add random salt for initial aggregation
    salted_df = df.withColumn("salt", (rand() * 100).cast("int"))
    
    phase1 = salted_df.groupBy(*group_cols, "salt").agg(
        *[sum(col).alias(f"sum_{col}") for col in agg_cols],
        *[count(col).alias(f"count_{col}") for col in agg_cols]
    )
    
    # Phase 2: Final aggregation without salt
    final_result = phase1.groupBy(*group_cols).agg(
        *[sum(f"sum_{col}").alias(f"total_{col}") for col in agg_cols],
        *[sum(f"count_{col}").alias(f"count_{col}") for col in agg_cols]
    )
    
    return final_result
```

#### B. Streaming Aggregations for Large Data
```python
# For very large datasets, use streaming approach
from pyspark.sql import functions as F

def streaming_aggregation(input_path, output_path, checkpoint_path):
    df = spark.readStream \
        .format("parquet") \
        .load(input_path)
    
    aggregated = df.groupBy("key") \
        .agg(F.sum("amount").alias("total_amount"))
    
    query = aggregated.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    return query
```

### 5. Memory Monitoring and Debugging

#### A. Enable Memory Monitoring
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.memoryFraction", "0.8") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+PrintGCDetails") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+PrintGCDetails") \
    .getOrCreate()
```

#### B. Monitor Memory Usage
```python
def monitor_memory_usage(df, operation_name):
    print(f"--- Memory Status for {operation_name} ---")
    
    # Get partition sizes
    partition_counts = df.glom().map(len).collect()
    print(f"Partition sizes: {partition_counts}")
    
    # Estimate memory usage
    sample_row = df.first()
    estimated_row_size = len(str(sample_row)) * 2  # Rough estimate
    total_rows = df.count()
    estimated_memory = (estimated_row_size * total_rows) / (1024 * 1024)  # MB
    
    print(f"Estimated memory usage: {estimated_memory:.2f} MB")
    print(f"Number of partitions: {df.rdd.getNumPartitions()}")
```

#### C. Optimize Garbage Collection
```python
# GC tuning for memory-intensive jobs
gc_options = [
    "-XX:+UseG1GC",
    "-XX:MaxGCPauseMillis=200",
    "-XX:G1HeapRegionSize=16m",
    "-XX:+G1UseAdaptiveIHOP",
    "-XX:G1MixedGCCountTarget=8",
    "-XX:G1MixedGCLiveThresholdPercent=85"
]

spark = SparkSession.builder \
    .config("spark.executor.extraJavaOptions", " ".join(gc_options)) \
    .config("spark.driver.extraJavaOptions", " ".join(gc_options)) \
    .getOrCreate()
```

### 6. Best Practices for Memory Management

#### A. Data Format Optimization
```python
# Use columnar formats for better memory efficiency
df.write.mode("overwrite").parquet("optimized_data")

# Use compression
df.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("compressed_data")
```

#### B. Partition Size Management
```python
# Optimal partition size: 100-200MB per partition
target_partition_size_mb = 128
total_size_mb = 1000  # Your data size
optimal_partitions = max(1, total_size_mb // target_partition_size_mb)

df_repartitioned = df.repartition(optimal_partitions)
```

#### C. Memory-Efficient Joins
```python
# Use broadcast joins for small tables
small_df = spark.table("small_table")
large_df = spark.table("large_table")

# Broadcast the smaller table
result = large_df.join(broadcast(small_df), "key")

# For large-large joins, ensure proper partitioning
result = large_df.repartition("join_key") \
    .join(other_large_df.repartition("join_key"), "join_key")
```

### 7. Emergency Memory Fixes

If you're facing immediate memory issues:

```python
# Quick fixes for memory problems:

# 1. Reduce parallelism
spark.conf.set("spark.default.parallelism", "50")  # Reduce from default

# 2. Increase memory allocation
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.driver.memory", "2g")

# 3. Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 4. Use disk-based operations
spark.conf.set("spark.sql.execution.sortMergeJoinExec.buffer.in.memory.threshold", "0")

# 5. Clear cache if not needed
spark.catalog.clearCache()
```

This guide provides comprehensive solutions for memory-related issues in PySpark. The key is to understand your data patterns and choose the appropriate optimization strategy.