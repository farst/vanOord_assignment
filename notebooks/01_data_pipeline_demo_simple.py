# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Data Pipeline Demo: CSV to Parquet to Delta Lake
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

import json
from datetime import datetime

import pandas as pd

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Storage configuration
storage_account = "voodatabricks77284"
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
    {
        "invoice_id": "INV001",
        "customer_id": "CUST001",
        "amount": 1500.00,
        "date": "2024-01-15",
        "status": "paid",
    },
    {
        "invoice_id": "INV002",
        "customer_id": "CUST002",
        "amount": 2300.50,
        "date": "2024-01-16",
        "status": "pending",
    },
    {
        "invoice_id": "INV003",
        "customer_id": "CUST001",
        "amount": 850.75,
        "date": "2024-01-17",
        "status": "paid",
    },
    {
        "invoice_id": "INV004",
        "customer_id": "CUST003",
        "amount": 3200.00,
        "date": "2024-01-18",
        "status": "overdue",
    },
    {
        "invoice_id": "INV005",
        "customer_id": "CUST002",
        "amount": 1750.25,
        "date": "2024-01-19",
        "status": "paid",
    },
]

# Create DataFrame
df = spark.createDataFrame(sample_data)

# Show the data
print("Sample invoice data:")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Data Cleaning and Transformation

# COMMAND ----------

# Data cleaning and transformation
df_cleaned = (
    df.withColumn("amount", col("amount").cast("double"))
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("processed_timestamp", current_timestamp())
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("day", dayofmonth(col("date")))
    .withColumn(
        "status_category",
        when(col("status") == "paid", "completed")
        .when(col("status") == "pending", "in_progress")
        .otherwise("needs_attention"),
    )
    .withColumn(
        "amount_category",
        when(col("amount") < 1000, "low")
        .when(col("amount") < 2500, "medium")
        .otherwise("high"),
    )
)

# Show cleaned data
print("Cleaned and transformed data:")
df_cleaned.show()

# Show schema
print("Data schema:")
df_cleaned.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Parquet Format (Using Unity Catalog)

# COMMAND ----------

# Create a temporary view for easier access
df_cleaned.createOrReplaceTempView("invoices_cleaned")

# Write to Unity Catalog table instead of direct file path
# This avoids the storage account key authentication issue
spark.sql(
    """
CREATE OR REPLACE TABLE main.default.invoices_parquet
USING PARQUET
AS SELECT * FROM invoices_cleaned
"""
)

print("Data written to Unity Catalog table: main.default.invoices_parquet")

# Verify the data
spark.sql("SELECT COUNT(*) as record_count FROM main.default.invoices_parquet").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Convert to Delta Lake with Versioning

# COMMAND ----------

# Create Delta table from the Parquet data
spark.sql(
    """
CREATE OR REPLACE TABLE main.default.invoices_delta
USING DELTA
AS SELECT * FROM main.default.invoices_parquet
"""
)

print("Data converted to Delta table: main.default.invoices_delta")

# Verify the Delta table
spark.sql("SELECT COUNT(*) as record_count FROM main.default.invoices_delta").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Demonstrate Delta Lake Versioning

# COMMAND ----------

# Check current version
history_df = spark.sql("DESCRIBE HISTORY main.default.invoices_delta")
print("Delta table history:")
history_df.show()

# Add more data to demonstrate versioning
new_data = [
    {
        "invoice_id": "INV011",
        "customer_id": "CUST006",
        "amount": 2750.00,
        "date": "2024-01-25",
        "status": "paid",
    },
    {
        "invoice_id": "INV012",
        "customer_id": "CUST007",
        "amount": 4100.25,
        "date": "2024-01-26",
        "status": "pending",
    },
]

df_new = spark.createDataFrame(new_data)
df_new_cleaned = (
    df_new.withColumn("amount", col("amount").cast("double"))
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("processed_timestamp", current_timestamp())
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("day", dayofmonth(col("date")))
    .withColumn(
        "status_category",
        when(col("status") == "paid", "completed")
        .when(col("status") == "pending", "in_progress")
        .otherwise("needs_attention"),
    )
    .withColumn(
        "amount_category",
        when(col("amount") < 1000, "low")
        .when(col("amount") < 2500, "medium")
        .otherwise("high"),
    )
)

# Append new data to Delta table
df_new_cleaned.write.format("delta").mode("append").saveAsTable(
    "main.default.invoices_delta"
)

print("New data appended to Delta table")

# Check version history again
history_df = spark.sql("DESCRIBE HISTORY main.default.invoices_delta")
print("Updated Delta table history:")
history_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Time Travel Demonstration

# COMMAND ----------

# Query table as of version 0 (original data)
print("Data as of version 0 (original):")
spark.sql("SELECT * FROM main.default.invoices_delta VERSION AS OF 0").show()

# Query table as of version 1 (with new data)
print("Data as of version 1 (with new data):")
spark.sql("SELECT * FROM main.default.invoices_delta VERSION AS OF 1").show()

# Show current data
print("Current data:")
spark.sql("SELECT * FROM main.default.invoices_delta").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Data Analysis and Insights

# COMMAND ----------

# Analyze the data
print("Invoice analysis:")
spark.sql(
    """
SELECT
    status_category,
    amount_category,
    COUNT(*) as invoice_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM main.default.invoices_delta
GROUP BY status_category, amount_category
ORDER BY total_amount DESC
"""
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ Data Pipeline Demo Complete!")
print("📊 Created Unity Catalog tables:")
print("   - main.default.invoices_parquet (Parquet format)")
print("   - main.default.invoices_delta (Delta format with versioning)")
print("🔄 Demonstrated Delta Lake features:")
print("   - Version history tracking")
print("   - Time travel queries")
print("   - Data lineage and audit trail")
print("📈 Performed data analysis and insights")
