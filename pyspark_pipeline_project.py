# PySpark Pipeline Project with Simulated Issues and Solutions
# File: spark_pipeline_demo.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
import time
import os

class SparkPipelineDemo:
    def __init__(self):
        self.spark = None
        self.setup_spark()
    
    def setup_spark(self):
        """Initialize Spark session with intentionally limited resources"""
        print("Setting up Spark session with limited resources...")
        
        self.spark = SparkSession.builder \
            .appName("PipelineDemo") \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.memory", "512m") \
            .config("spark.driver.maxResultSize", "256m") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("Spark session created with limited resources")
    
    def create_sample_data(self):
        """Create sample data with skewed distribution"""
        print("Creating sample data with skewed distribution...")
        
        # Create skewed data - 80% of records have same key
        data = []
        
        # 80% records with skewed key
        for i in range(8000):
            data.append((
                "SKEWED_KEY",  # This will cause data skew
                f"customer_{random.randint(1, 100)}",
                random.randint(100, 1000),
                random.uniform(10.0, 500.0),
                f"product_{random.randint(1, 50)}",
                "2024-01-15"
            ))
        
        # 20% records with normal distribution
        for i in range(2000):
            data.append((
                f"normal_key_{random.randint(1, 100)}",
                f"customer_{random.randint(1, 100)}",
                random.randint(100, 1000),
                random.uniform(10.0, 500.0),
                f"product_{random.randint(1, 50)}",
                "2024-01-15"
            ))
        
        schema = StructType([
            StructField("partition_key", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("product", StringType(), True),
            StructField("date", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        
        # Create temp table
        df.createOrReplaceTempView("temp_orders")
        print(f"Created temp table with {df.count()} records")
        return df
    
    def create_target_table(self):
        """Create target table structure"""
        print("Creating target table...")
        
        # Create empty target table
        schema = StructType([
            StructField("partition_key", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("product", StringType(), True),
            StructField("date", StringType(), True),
            StructField("processed_timestamp", TimestampType(), True)
        ])
        
        empty_df = self.spark.createDataFrame([], schema)
        empty_df.createOrReplaceTempView("target_orders")
        print("Target table created")
        return empty_df
    
    def problematic_pipeline_v1(self):
        """Version 1: Pipeline that will fail with OOM and skewness issues"""
        print("\n" + "="*50)
        print("RUNNING PROBLEMATIC PIPELINE V1")
        print("="*50)
        
        try:
            print("Step 1: Reading from temp table...")
            temp_df = self.spark.sql("SELECT * FROM temp_orders")
            
            print("Step 2: Adding processing timestamp...")
            processed_df = temp_df.withColumn("processed_timestamp", current_timestamp())
            
            print("Step 3: Performing expensive operations that cause OOM...")
            # This will cause memory issues
            result_df = processed_df \
                .groupBy("partition_key", "customer_id", "product") \
                .agg(
                    sum("amount").alias("total_amount"),
                    count("*").alias("order_count"),
                    collect_list("order_id").alias("order_list"),  # Memory intensive
                    first("date").alias("date"),
                    first("processed_timestamp").alias("processed_timestamp")
                ) \
                .withColumn("avg_amount", col("total_amount") / col("order_count")) \
                .cache()  # This will consume memory
            
            print("Step 4: Collecting results (this will likely fail)...")
            result_df.show(20)
            result_count = result_df.count()
            print(f"Processed {result_count} records")
            
        except Exception as e:
            print(f"❌ Pipeline failed with error: {str(e)}")
            print("This is expected - the pipeline has resource and skewness issues")
            return False
        
        return True
    
    def problematic_pipeline_v2(self):
        """Version 2: Fix memory issues but still have skewness problems"""
        print("\n" + "="*50)
        print("RUNNING PROBLEMATIC PIPELINE V2 - Fixed Memory Issues")
        print("="*50)
        
        try:
            print("Step 1: Reading from temp table with optimized operations...")
            temp_df = self.spark.sql("SELECT * FROM temp_orders")
            
            print("Step 2: Adding processing timestamp...")
            processed_df = temp_df.withColumn("processed_timestamp", current_timestamp())
            
            print("Step 3: Performing aggregations without memory-intensive operations...")
            # Fixed: Removed collect_list and cache
            result_df = processed_df \
                .groupBy("partition_key", "customer_id", "product") \
                .agg(
                    sum("amount").alias("total_amount"),
                    count("*").alias("order_count"),
                    first("date").alias("date"),
                    first("processed_timestamp").alias("processed_timestamp")
                ) \
                .withColumn("avg_amount", col("total_amount") / col("order_count"))
            
            print("Step 4: Writing to target table (will be slow due to skewness)...")
            start_time = time.time()
            
            result_df.write \
                .mode("append") \
                .partitionBy("partition_key") \
                .option("path", "/tmp/target_orders") \
                .saveAsTable("target_orders_v2")
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            print(f"✅ Processing completed in {processing_time:.2f} seconds")
            print("⚠️  Note: Processing was slow due to data skewness on partition_key")
            
            # Show partition distribution
            print("\nPartition distribution:")
            self.spark.sql("""
                SELECT partition_key, COUNT(*) as record_count 
                FROM target_orders_v2 
                GROUP BY partition_key 
                ORDER BY record_count DESC
            """).show()
            
        except Exception as e:
            print(f"❌ Pipeline failed with error: {str(e)}")
            return False
        
        return True
    
    def optimized_pipeline_v3(self):
        """Version 3: Fully optimized pipeline with all fixes"""
        print("\n" + "="*50)
        print("RUNNING OPTIMIZED PIPELINE V3 - All Issues Fixed")
        print("="*50)
        
        try:
            print("Step 1: Reading from temp table...")
            temp_df = self.spark.sql("SELECT * FROM temp_orders")
            
            print("Step 2: Handling data skewness with salting technique...")
            # Add salt to skewed keys
            salted_df = temp_df \
                .withColumn("salt", 
                    when(col("partition_key") == "SKEWED_KEY", 
                         (rand() * 10).cast("int"))
                    .otherwise(lit(0))) \
                .withColumn("salted_partition_key", 
                    concat(col("partition_key"), lit("_"), col("salt"))) \
                .withColumn("processed_timestamp", current_timestamp())
            
            print("Step 3: Performing aggregations with salted keys...")
            # First aggregation with salted keys
            intermediate_df = salted_df \
                .groupBy("salted_partition_key", "customer_id", "product") \
                .agg(
                    sum("amount").alias("total_amount"),
                    count("*").alias("order_count"),
                    first("partition_key").alias("original_partition_key"),
                    first("date").alias("date"),
                    first("processed_timestamp").alias("processed_timestamp")
                )
            
            # Second aggregation to merge salted results
            final_df = intermediate_df \
                .groupBy("original_partition_key", "customer_id", "product") \
                .agg(
                    sum("total_amount").alias("total_amount"),
                    sum("order_count").alias("order_count"),
                    first("date").alias("date"),
                    first("processed_timestamp").alias("processed_timestamp")
                ) \
                .withColumn("avg_amount", col("total_amount") / col("order_count")) \
                .withColumnRenamed("original_partition_key", "partition_key")
            
            print("Step 4: Optimizing partitions before writing...")
            # Repartition for balanced writing
            balanced_df = final_df.repartition(4, col("partition_key"))
            
            print("Step 5: Writing to target table with optimized settings...")
            start_time = time.time()
            
            balanced_df.write \
                .mode("append") \
                .option("path", "/tmp/target_orders_optimized") \
                .saveAsTable("target_orders_v3")
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            print(f"✅ Optimized processing completed in {processing_time:.2f} seconds")
            
            # Show final results
            print("\nFinal results:")
            self.spark.sql("SELECT * FROM target_orders_v3").show(20)
            
            print("\nRecord count by partition:")
            self.spark.sql("""
                SELECT partition_key, COUNT(*) as record_count 
                FROM target_orders_v3 
                GROUP BY partition_key 
                ORDER BY record_count DESC
            """).show()
            
        except Exception as e:
            print(f"❌ Pipeline failed with error: {str(e)}")
            return False
        
        return True
    
    def demonstrate_incremental_append(self):
        """Demonstrate incremental data append"""
        print("\n" + "="*50)
        print("DEMONSTRATING INCREMENTAL APPEND")
        print("="*50)
        
        # Create new batch of data
        new_data = []
        for i in range(1000):
            new_data.append((
                f"batch2_key_{random.randint(1, 10)}",
                f"customer_{random.randint(1, 100)}",
                random.randint(2000, 3000),
                random.uniform(10.0, 500.0),
                f"product_{random.randint(1, 50)}",
                "2024-01-16"
            ))
        
        schema = StructType([
            StructField("partition_key", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("product", StringType(), True),
            StructField("date", StringType(), True)
        ])
        
        new_batch_df = self.spark.createDataFrame(new_data, schema)
        new_batch_df.createOrReplaceTempView("temp_orders_batch2")
        
        print("Processing new batch with optimized pipeline...")
        
        # Process new batch using optimized approach
        processed_batch = new_batch_df \
            .withColumn("processed_timestamp", current_timestamp()) \
            .groupBy("partition_key", "customer_id", "product") \
            .agg(
                sum("amount").alias("total_amount"),
                count("*").alias("order_count"),
                first("date").alias("date"),
                first("processed_timestamp").alias("processed_timestamp")
            ) \
            .withColumn("avg_amount", col("total_amount") / col("order_count"))
        
        # Append to existing target table
        processed_batch.write \
            .mode("append") \
            .option("path", "/tmp/target_orders_optimized") \
            .saveAsTable("target_orders_v3")
        
        print("✅ Incremental append completed")
        
        # Show updated counts
        total_count = self.spark.sql("SELECT COUNT(*) as total FROM target_orders_v3").collect()[0][0]
        print(f"Total records in target table: {total_count}")
    
    def cleanup(self):
        """Clean up resources"""
        print("\nCleaning up resources...")
        if self.spark:
            self.spark.stop()
        print("Cleanup completed")

def main():
    """Main execution function"""
    demo = SparkPipelineDemo()
    
    try:
        # Create sample data
        demo.create_sample_data()
        demo.create_target_table()
        
        # Run problematic pipeline v1 (will fail)
        print("\n🔥 Running pipeline that WILL FAIL due to OOM...")
        demo.problematic_pipeline_v1()
        
        # Run problematic pipeline v2 (memory fixed, but slow due to skewness)
        print("\n⚡ Running pipeline with memory fixes but skewness issues...")
        demo.problematic_pipeline_v2()
        
        # Run optimized pipeline v3 (all issues fixed)
        print("\n🚀 Running fully optimized pipeline...")
        demo.optimized_pipeline_v3()
        
        # Demonstrate incremental append
        demo.demonstrate_incremental_append()
        
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
    finally:
        demo.cleanup()

if __name__ == "__main__":
    main()