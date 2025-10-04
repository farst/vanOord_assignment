# Databricks notebook source
# MAGIC %md
# MAGIC # Data Pipeline Demo: CSV to Parquet to Delta Lake
# MAGIC 
# MAGIC This notebook demonstrates:
# MAGIC 1. Reading CSV data from storage
# MAGIC 2. Data cleaning and transformation
# MAGIC 3. Writing to Parquet format
# MAGIC 4. Converting to Delta Lake with versioning
# MAGIC 5. Time travel and audit capabilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime
import json

# Get storage configuration from environment
storage_account = "voodatabricks10344"
raw_container = "raw"
curated_container = "curated"

# Storage paths
raw_path = f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net/"
curated_path = f"abfss://{curated_container}@{storage_account}.dfs.core.windows.net/"

print(f"Raw data path: {raw_path}")
print(f"Curated data path: {curated_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Sample CSV Data

# COMMAND ----------

# Create sample invoice data for demonstration
sample_data = [
    {"invoice_id": "INV001", "customer_id": "CUST001", "amount": 1500.50, "date": "2024-01-15", "status": "paid"},
    {"invoice_id": "INV002", "customer_id": "CUST002", "amount": 2300.75, "date": "2024-01-16", "status": "pending"},
    {"invoice_id": "INV003", "customer_id": "CUST001", "amount": 890.25, "date": "2024-01-17", "status": "paid"},
    {"invoice_id": "INV004", "customer_id": "CUST003", "amount": 4500.00, "date": "2024-01-18", "status": "overdue"},
    {"invoice_id": "INV005", "customer_id": "CUST002", "amount": 1200.00, "date": "2024-01-19", "status": "paid"},
    {"invoice_id": "INV006", "customer_id": "CUST004", "amount": 3200.50, "date": "2024-01-20", "status": "pending"},
    {"invoice_id": "INV007", "customer_id": "CUST001", "amount": 1800.75, "date": "2024-01-21", "status": "paid"},
    {"invoice_id": "INV008", "customer_id": "CUST005", "amount": 5600.25, "date": "2024-01-22", "status": "pending"},
    {"invoice_id": "INV009", "customer_id": "CUST003", "amount": 2100.00, "date": "2024-01-23", "status": "overdue"},
    {"invoice_id": "INV010", "customer_id": "CUST002", "amount": 3400.50, "date": "2024-01-24", "status": "paid"}
]

# Create DataFrame
df_raw = spark.createDataFrame(sample_data)

# Write to raw storage as CSV
raw_csv_path = f"{raw_path}invoices/invoices_2024_01.csv"
df_raw.coalesce(1).write.mode("overwrite").option("header", "true").csv(raw_csv_path)

print(f"Sample CSV data written to: {raw_csv_path}")
df_raw.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read and Clean CSV Data

# COMMAND ----------

# Read CSV data from storage
df_csv = spark.read.option("header", "true").csv(raw_csv_path)

print("Raw CSV data:")
df_csv.show()

# Data cleaning and transformation
df_cleaned = df_csv.withColumn("amount", col("amount").cast("double")) \
                   .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
                   .withColumn("processed_timestamp", current_timestamp()) \
                   .withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date"))) \
                   .withColumn("status_category", 
                              when(col("status") == "paid", "completed")
                              .when(col("status") == "pending", "in_progress")
                              .otherwise("needs_attention"))

print("Cleaned and transformed data:")
df_cleaned.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Parquet Format

# COMMAND ----------

# Write cleaned data to Parquet format
parquet_path = f"{curated_path}invoices/invoices_2024_01.parquet"
df_cleaned.write.mode("overwrite").parquet(parquet_path)

print(f"Cleaned data written to Parquet: {parquet_path}")

# Verify Parquet data
df_parquet = spark.read.parquet(parquet_path)
print("Parquet data verification:")
df_parquet.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Convert to Delta Lake with Versioning

# COMMAND ----------

# Convert Parquet to Delta Lake
delta_path = f"{curated_path}invoices/invoices_delta"

# Write as Delta table with partitioning
df_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save(delta_path)

print(f"Delta table created at: {delta_path}")

# Create Delta table for easier querying
spark.sql(f"""
CREATE TABLE IF NOT EXISTS invoices_delta
USING DELTA
LOCATION '{delta_path}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Demonstrate Delta Lake Versioning

# COMMAND ----------

# Check current version
history_df = spark.sql("DESCRIBE HISTORY delta.`{}`".format(delta_path))
print("Delta table history:")
history_df.show()

# Add more data to demonstrate versioning
new_data = [
    {"invoice_id": "INV011", "customer_id": "CUST006", "amount": 2750.00, "date": "2024-01-25", "status": "paid"},
    {"invoice_id": "INV012", "customer_id": "CUST007", "amount": 4100.25, "date": "2024-01-26", "status": "pending"}
]

df_new = spark.createDataFrame(new_data)
df_new_cleaned = df_new.withColumn("amount", col("amount").cast("double")) \
                      .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
                      .withColumn("processed_timestamp", current_timestamp()) \
                      .withColumn("year", year(col("date"))) \
                      .withColumn("month", month(col("date"))) \
                      .withColumn("day", dayofmonth(col("date"))) \
                      .withColumn("status_category", 
                                 when(col("status") == "paid", "completed")
                                 .when(col("status") == "pending", "in_progress")
                                 .otherwise("needs_attention"))

# Append to Delta table
df_new_cleaned.write \
    .format("delta") \
    .mode("append") \
    .save(delta_path)

print("New data appended to Delta table")

# Check updated history
history_df = spark.sql("DESCRIBE HISTORY delta.`{}`".format(delta_path))
print("Updated Delta table history:")
history_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Time Travel and Audit Capabilities

# COMMAND ----------

# Query current data
print("Current data (latest version):")
spark.sql("SELECT * FROM invoices_delta ORDER BY date DESC").show()

# Time travel to previous version
print("Data from previous version (time travel):")
spark.sql("SELECT * FROM delta.`{}` VERSION AS OF 0 ORDER BY date DESC".format(delta_path)).show()

# Show version differences
print("Version comparison:")
current_count = spark.sql("SELECT COUNT(*) as current_count FROM invoices_delta").collect()[0]["current_count"]
previous_count = spark.sql("SELECT COUNT(*) as previous_count FROM delta.`{}` VERSION AS OF 0".format(delta_path)).collect()[0]["previous_count"]

print(f"Current version count: {current_count}")
print(f"Previous version count: {previous_count}")
print(f"Records added: {current_count - previous_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Data Quality and Validation

# COMMAND ----------

# Data quality checks
print("Data Quality Report:")
print("=" * 50)

# Check for null values
null_counts = df_cleaned.select([count(when(col(c).isNull(), c)).alias(c) for c in df_cleaned.columns])
print("Null value counts:")
null_counts.show()

# Check data types
print("Data schema:")
df_cleaned.printSchema()

# Summary statistics
print("Summary statistics:")
df_cleaned.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Performance Optimization

# COMMAND ----------

# Optimize Delta table
spark.sql("OPTIMIZE delta.`{}`".format(delta_path))

# Z-order by frequently queried columns
spark.sql("OPTIMIZE delta.`{}` ZORDER BY (customer_id, status)".format(delta_path))

print("Delta table optimized for better query performance")

# Check table statistics
print("Table statistics:")
spark.sql("ANALYZE TABLE invoices_delta COMPUTE STATISTICS")
spark.sql("DESCRIBE EXTENDED invoices_delta").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This demonstration showed:
# MAGIC 1. ✅ **CSV Processing**: Reading and cleaning CSV data
# MAGIC 2. ✅ **Parquet Storage**: Writing to efficient Parquet format
# MAGIC 3. ✅ **Delta Lake**: Converting to Delta with ACID properties
# MAGIC 4. ✅ **Versioning**: Tracking changes with version history
# MAGIC 5. ✅ **Time Travel**: Querying previous versions
# MAGIC 6. ✅ **Data Quality**: Validation and statistics
# MAGIC 7. ✅ **Optimization**: Performance tuning techniques
# MAGIC 
# MAGIC **Storage Locations:**
# MAGIC - Raw CSV: `{raw_path}invoices/`
# MAGIC - Parquet: `{parquet_path}`
# MAGIC - Delta Table: `{delta_path}`
# MAGIC - SQL Table: `invoices_delta`
