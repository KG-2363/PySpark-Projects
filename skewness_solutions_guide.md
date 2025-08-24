# Data Skewness Solutions

## Understanding Data Skewness

Data skewness occurs when data is unevenly distributed across partitions, causing some tasks to take much longer than others. This is one of the most common performance bottlenecks in Spark applications.

## Identifying Data Skewness

### 1. Symptoms of Data Skewness
- Wide variation in task execution times
- Some executors are idle while others are overloaded
- A few tasks taking 10x longer than others
- Uneven partition sizes
- Memory issues on specific executors

### 2. Detecting Skewness
```python
def detect_skewness(df, key_column):
    """Detect skewness in a DataFrame"""
    print(f"Analyzing skewness for column: {key_column}")
    
    # Check key distribution
    key_distribution = df.groupBy(key_column) \
        .count() \
        .orderBy(desc("count"))
    
    key_distribution.show(20)
    
    # Calculate statistics
    stats = key_distribution.agg(
        avg("count").alias("avg_count"),
        max("count").alias("max_count"),
        min("count").alias("min_count"),
        stddev("count").alias("stddev_count")
    ).collect()[0]
    
    skew_ratio = stats["max_count"] / stats["avg_count"]
    
    print(f"Skewness Analysis:")
    print(f"  Average records per key: {stats['avg_count']:.0f}")
    print(f"  Maximum records per key: {stats['max_count']}")
    print(f"  Skewness ratio (max/avg): {skew_ratio:.2f}")
    
    if skew_ratio > 10:
        print("⚠️  HIGH SKEWNESS DETECTED - Consider applying skewness solutions")
    elif skew_ratio > 3:
        print("⚠️  MODERATE SKEWNESS - Monitor performance")
    else:
        print("✅ LOW SKEWNESS - Good distribution")
    
    return skew_ratio

# Usage
skew_ratio = detect_skewness(df, "partition_key")
```

## Skewness Solutions

### 1. Salting Technique

The most effective solution for aggregation skewness.

#### A. Basic Salting Implementation
```python
def apply_salting(df, skewed_column, salt_range=10):
    """Apply salting to handle skewed aggregations"""
    
    # Add salt to skewed keys
    salted_df = df.withColumn("salt", 
        when(col(skewed_column) == "SKEWED_KEY", 
             (rand() * salt_range).cast("int"))
        .otherwise(lit(0))
    )
    
    # Create salted key
    salted_df = salted_df.withColumn("salted_key", 
        concat(col(skewed_column), lit("_"), col("salt"))
    )
    
    return salted_df

# Example usage for aggregation
def skew_resistant_aggregation(df, group_cols, agg_expressions):
    """Perform aggregation resistant to skewness"""
    
    # Phase 1: Aggregate with salted keys
    phase1_result = df.groupBy("salted_key", *group_cols) \
        .agg(*agg_expressions)
    
    # Phase 2: Re-aggregate by original keys
    final_result = phase1_result \
        .withColumn("original_key", split(col("salted_key"), "_")[0]) \
        .drop("salted_key") \
        .groupBy("original_key", *group_cols) \
        .agg(*[sum(col).alias(col) for col in phase1_result.columns 
               if col not in group_cols + ["salted_key"]])
    
    return final_result
```

#### B. Advanced Adaptive Salting
```python
def adaptive_salting(df, key_column, target_partition_size=100000):
    """Dynamically determine salt range based on key distribution"""
    
    # Analyze key distribution
    key_stats = df.groupBy(key_column).count().collect()
    key_counts = {row[key_column]: row["count"] for row in key_stats}
    
    # Calculate salt range for each key
    def get_salt_range(key):
        count = key_counts.get(key, 1)
        return max(1, count // target_partition_size)
    
    # Create UDF for dynamic salting
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    
    @udf(returnType=IntegerType())
    def dynamic_salt(key):
        salt_range = get_salt_range(key)
        return int(random.random() * salt_range)
    
    # Apply dynamic salting
    salted_df = df.withColumn("dynamic_salt", dynamic_salt(col(key_column)))
    salted_df = salted_df.withColumn("salted_key", 
        concat(col(key_column), lit("_"), col("dynamic_salt"))
    )
    
    return salted_df
```

### 2. Isolated Skewed Key Processing

Handle severely skewed keys separately.

```python
def isolated_skewed_processing(df, key_column, skewed_keys, regular_salt_range=5):
    """Process skewed and regular keys separately"""
    
    # Separate skewed and regular data
    skewed_data = df.filter(col(key_column).isin(skewed_keys))
    regular_data = df.filter(~col(key_column).isin(skewed_keys))
    
    # Process regular data normally
    regular_result = regular_data.groupBy(key_column) \
        .agg(sum("amount").alias("total_amount"))
    
    # Process skewed data with heavy salting
    skewed_salted = skewed_data.withColumn("salt", 
        (rand() * 50).cast("int")  # Heavy salting for skewed keys
    )
    
    # Two-phase aggregation for skewed data
    skewed_phase1 = skewed_salted.groupBy(key_column, "salt") \
        .agg(sum("amount").alias("partial_sum"))
    
    skewed_result = skewed_phase1.groupBy(key_column) \
        .agg(sum("partial_sum").alias("total_amount"))
    
    # Combine results
    final_result = regular_result.union(skewed_result)
    
    return final_result
```

### 3. Broadcast Join for Skewed Joins

Use broadcast joins to avoid shuffle-based join skewness.

```python
def skew_resistant_join(large_df, small_df, join_key):
    """Perform join resistant to skewness"""
    
    # Check if small table can be broadcasted
    small_df_size = small_df.count()
    
    if small_df_size < 1000000:  # If small enough, broadcast it
        print("Using broadcast join to avoid skewness")
        result = large_df.join(broadcast(small_df), join_key)
    else:
        # Use bucket join for large tables
        print("Using bucketed join for large tables")
        
        # Repartition both tables by join key
        large_repartitioned = large_df.repartition(200, join_key)
        small_repartitioned = small_df.repartition(200, join_key)
        
        result = large_repartitioned.join(small_repartitioned, join_key)
    
    return result
```

### 4. Custom Partitioning Strategies

#### A. Range Partitioning
```python
def range_partition_by_frequency(df, key_column, num_partitions=100):
    """Partition data based on key frequency ranges"""
    
    # Get key frequencies
    key_freq = df.groupBy(key_column).count() \
        .orderBy("count").collect()
    
    # Create frequency-based ranges
    total_records = sum(row["count"] for row in key_freq)
    records_per_partition = total_records // num_partitions
    
    partition_ranges = []
    current_partition = []
    current_size = 0
    
    for row in key_freq:
        current_partition.append(row[key_column])
        current_size += row["count"]
        
        if current_size >= records_per_partition:
            partition_ranges.append(current_partition)
            current_partition = []
            current_size = 0
    
    if current_partition:
        partition_ranges.append(current_partition)
    
    # Apply custom partitioning
    def custom_partitioner(key):
        for i, partition_keys in enumerate(partition_ranges):
            if key in partition_keys:
                return i
        return len(partition_ranges) - 1
    
    # This requires custom partitioner implementation
    # For simplicity, use repartitioning with computed ranges
    return df.repartition(num_partitions, key_column)
```

#### B. Hash-Based Partitioning with Salt
```python
def hash_partition_with_salt(df, key_column, num_partitions=100):
    """Use hash-based partitioning with salt for better distribution"""
    
    # Add hash-based partition column
    df_partitioned = df.withColumn("partition_id", 
        (hash(concat(col(key_column), lit(rand()))) % num_partitions)
    )
    
    # Repartition using the computed partition ID
    return df_partitioned.repartition(col("partition_id"))
```

### 5. Window Function Optimizations

Handle skewed window operations efficiently.

```python
def skew_resistant_window_operation(df, partition_cols, order_cols, window_func):
    """Perform window operations resistant to skewness"""
    
    # Check for skewness in partition columns
    partition_key = concat(*[col(c) for c in partition_cols])
    skew_ratio = detect_skewness(df.withColumn("combined_key", partition_key), 
                                 "combined_key")
    
    if skew_ratio > 5:
        print("Applying skewness mitigation for window operation")
        
        # Add salt to partition specification
        salted_df = df.withColumn("window_salt", 
            (rand() * 10).cast("int")
        )
        
        # Create salted partition columns
        salted_partition_cols = partition_cols + ["window_salt"]
        
        # Apply window function with salted partitions
        window_spec = Window.partitionBy(*salted_partition_cols) \
                           .orderBy(*order_cols)
        
        result = salted_df.withColumn("window_result", 
                                     window_func.over(window_spec))
        
        # Aggregate results if needed (depends on window function)
        return result.drop("window_salt")
    else:
        # Normal window operation
        window_spec = Window.partitionBy(*partition_cols) \
                           .orderBy(*order_cols)
        
        return df.withColumn("window_result", 
                           window_func.over(window_spec))
```

### 6. Monitoring and Alerting for Skewness

```python
def setup_skewness_monitoring(df, key_columns):
    """Set up monitoring for data skewness"""
    
    monitoring_results = {}
    
    for column in key_columns:
        # Calculate skewness metrics
        stats = df.groupBy(column).count()
        
        metrics = stats.agg(
            avg("count").alias("avg"),
            max("count").alias("max"),
            min("count").alias("min"),
            stddev("count").alias("stddev")
        ).collect()[0]
        
        skew_ratio = metrics["max"] / metrics["avg"] if metrics["avg"] > 0 else 0
        coefficient_variation = metrics["stddev"] / metrics["avg"] if metrics["avg"] > 0 else 0
        
        monitoring_results[column] = {
            "skew_ratio": skew_ratio,
            "coefficient_variation": coefficient_variation,
            "max_records": metrics["max"],
            "avg_records": metrics["avg"]
        }
        
        # Alert if skewness detected
        if skew_ratio > 10:
            print(f"🚨 CRITICAL SKEWNESS in {column}: ratio={skew_ratio:.2f}")
        elif skew_ratio > 3:
            print(f"⚠️  WARNING SKEWNESS in {column}: ratio={skew_ratio:.2f}")
    
    return monitoring_results
```

### 7. Complete Skewness Solution Framework

```python
class SkewnessHandler:
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def analyze_and_fix_skewness(self, df, operation_type, **kwargs):
        """Analyze skewness and apply appropriate solution"""
        
        key_column = kwargs.get("key_column")
        skew_threshold = kwargs.get("skew_threshold", 5)
        
        # Detect skewness
        skew_ratio = detect_skewness(df, key_column)
        
        if skew_ratio < skew_threshold:
            print("✅ No significant skewness detected")
            return df
        
        print(f"🔧 Applying skewness solution for {operation_type}")
        
        if operation_type == "aggregation":
            return self._handle_aggregation_skewness(df, **kwargs)
        elif operation_type == "join":
            return self._handle_join_skewness(df, **kwargs)
        elif operation_type == "window":
            return self._handle_window_skewness(df, **kwargs)
        else:
            print("Unknown operation type, applying generic salting")
            return apply_salting(df, key_column)
    
    def _handle_aggregation_skewness(self, df, key_column, agg_expressions, **kwargs):
        """Handle aggregation skewness"""
        salt_range = kwargs.get("salt_range", 10)
        
        # Apply salting technique
        salted_df = apply_salting(df, key_column, salt_range)
        
        # Two-phase aggregation
        return skew_resistant_aggregation(salted_df, [key_column], agg_expressions)
    
    def _handle_join_skewness(self, df1, df2, join_key, **kwargs):
        """Handle join skewness"""
        return skew_resistant_join(df1, df2, join_key)
    
    def _handle_window_skewness(self, df, partition_cols, order_cols, window_func, **kwargs):
        """Handle window operation skewness"""
        return skew_resistant_window_operation(df, partition_cols, order_cols, window_func)

# Usage example