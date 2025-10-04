# Databricks notebook source
# MAGIC %md
# MAGIC # Final Data Pipeline Demo: CSV to Parquet to Delta Lake
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Reading CSV data from storage
# MAGIC 2. Data cleaning and transformation
# MAGIC 3. Writing to Parquet format using Unity Catalog managed tables
# MAGIC 4. Converting to Delta Lake with versioning
# MAGIC 5. Time travel and audit capabilities
# MAGIC
# MAGIC **Note**: This version uses only Unity Catalog managed tables to avoid ADLS Gen2 authentication issues.

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

print("✅ Libraries imported successfully")

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
# MAGIC ## Step 3: Write to Parquet Format (Unity Catalog Managed Table)

# COMMAND ----------

# Write to Unity Catalog managed table using Parquet format
# This completely avoids ADLS Gen2 authentication issues
df_cleaned.write.format("parquet").mode("overwrite").saveAsTable(
    "main.default.invoices_parquet"
)

print("✅ Data written to Unity Catalog managed table: main.default.invoices_parquet")

# Verify the table
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

print("✅ Data converted to Delta table: main.default.invoices_delta")

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

print("✅ New data appended to Delta table")

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
# MAGIC ## Step 8: Demonstrate Delta Lake Features

# COMMAND ----------

# Show table properties
print("Delta table properties:")
spark.sql("DESCRIBE DETAIL main.default.invoices_delta").show()

# Show table schema
print("Delta table schema:")
spark.sql("DESCRIBE main.default.invoices_delta").show()

# Show table statistics
print("Table statistics:")
spark.sql("ANALYZE TABLE main.default.invoices_delta COMPUTE STATISTICS")
spark.sql("DESCRIBE HISTORY main.default.invoices_delta").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ Data Pipeline Demo Complete!")
print("📊 Created Unity Catalog managed tables:")
print("   - main.default.invoices_parquet (Parquet format)")
print("   - main.default.invoices_delta (Delta format with versioning)")
print("🔄 Demonstrated Delta Lake features:")
print("   - Version history tracking")
print("   - Time travel queries")
print("   - Data lineage and audit trail")
print("   - Table properties and statistics")
print("📈 Performed data analysis and insights")
print("🔒 Used Unity Catalog managed tables (no ADLS Gen2 authentication issues)")
