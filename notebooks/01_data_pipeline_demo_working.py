# Databricks notebook source
# MAGIC %md
# MAGIC # Working Data Pipeline Demo: CSV to Parquet to Delta Lake
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Reading CSV data from storage
# MAGIC 2. Data cleaning and transformation
# MAGIC 3. Writing to Parquet format using Unity Catalog external locations
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
# MAGIC ## Step 3: Write to Parquet Format (Using Unity Catalog External Locations)

# COMMAND ----------

# Write to Unity Catalog external location using the configured external location
# This uses the Unity Catalog external location that's already configured
df_cleaned.write.format("parquet").mode("overwrite").partitionBy("year", "month").save(
    "abfss://curated@voodatabricks77284.dfs.core.windows.net/invoices/invoices_parquet"
)

print("✅ Data written to Parquet format in Unity Catalog external location")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Convert to Delta Lake with Versioning

# COMMAND ----------

# Read the Parquet data and convert to Delta
df_parquet = spark.read.parquet(
    "abfss://curated@voodatabricks77284.dfs.core.windows.net/invoices/invoices_parquet"
)

# Write as Delta table with partitioning
df_parquet.write.format("delta").mode("overwrite").partitionBy("year", "month").save(
    "abfss://curated@voodatabricks77284.dfs.core.windows.net/invoices/invoices_delta"
)

print("✅ Data converted to Delta Lake format")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Unity Catalog Table for Easy Access

# COMMAND ----------

# Create a Unity Catalog table pointing to the Delta location
spark.sql(
    """
CREATE TABLE IF NOT EXISTS main.default.invoices_delta_table
USING DELTA
LOCATION 'abfss://curated@voodatabricks77284.dfs.core.windows.net/invoices/invoices_delta'
"""
)

print("✅ Unity Catalog table created: main.default.invoices_delta_table")

# Verify the table
spark.sql(
    "SELECT COUNT(*) as record_count FROM main.default.invoices_delta_table"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Demonstrate Delta Lake Versioning

# COMMAND ----------

# Check current version
history_df = spark.sql("DESCRIBE HISTORY main.default.invoices_delta_table")
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
df_new_cleaned.write.format("delta").mode("append").save(
    "abfss://curated@voodatabricks77284.dfs.core.windows.net/invoices/invoices_delta"
)

print("✅ New data appended to Delta table")

# Check version history again
history_df = spark.sql("DESCRIBE HISTORY main.default.invoices_delta_table")
print("Updated Delta table history:")
history_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Time Travel Demonstration

# COMMAND ----------

# Query table as of version 0 (original data)
print("Data as of version 0 (original):")
spark.sql("SELECT * FROM main.default.invoices_delta_table VERSION AS OF 0").show()

# Query table as of version 1 (with new data)
print("Data as of version 1 (with new data):")
spark.sql("SELECT * FROM main.default.invoices_delta_table VERSION AS OF 1").show()

# Show current data
print("Current data:")
spark.sql("SELECT * FROM main.default.invoices_delta_table").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Data Analysis and Insights

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
FROM main.default.invoices_delta_table
GROUP BY status_category, amount_category
ORDER BY total_amount DESC
"""
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ Data Pipeline Demo Complete!")
print("📊 Created files in Unity Catalog external location:")
print(
    "   - abfss://curated@voodatabricks77284.dfs.core.windows.net/invoices/invoices_parquet"
)
print(
    "   - abfss://curated@voodatabricks77284.dfs.core.windows.net/invoices/invoices_delta"
)
print("🗄️ Created Unity Catalog table:")
print("   - main.default.invoices_delta_table")
print("🔄 Demonstrated Delta Lake features:")
print("   - Version history tracking")
print("   - Time travel queries")
print("   - Data lineage and audit trail")
print("📈 Performed data analysis and insights")
