# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Time Travel
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Creating multiple versions of a Delta table
# MAGIC 2. Querying previous versions using version numbers
# MAGIC 3. Querying previous versions using timestamps
# MAGIC 4. Comparing different versions
# MAGIC 5. Rolling back to previous versions
# MAGIC
# MAGIC **Focus**: Time travel capabilities for data versioning and recovery

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("✅ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Initial Table (Version 0)

# COMMAND ----------

# Create initial product data
initial_data = [
    (1, "Laptop", "Electronics", 999.99, 50),
    (2, "Mouse", "Electronics", 25.99, 200),
    (3, "Keyboard", "Electronics", 75.99, 100),
    (4, "Monitor", "Electronics", 299.99, 30),
]

schema = StructType(
    [
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock", IntegerType(), True),
    ]
)

df_v0 = spark.createDataFrame(initial_data, schema)

# Create Delta table
table_path = "/tmp/delta_demo/products"
df_v0.write.format("delta").mode("overwrite").save(table_path)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS products
USING DELTA
LOCATION '{table_path}'
"""
)

print("✅ Version 0 created - Initial product catalog")
df_v0.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Add New Products (Version 1)

# COMMAND ----------

# Add new products
new_products = [
    (5, "Tablet", "Electronics", 399.99, 25),
    (6, "Headphones", "Electronics", 149.99, 75),
]

df_new = spark.createDataFrame(new_products, schema)
df_new.write.format("delta").mode("append").save(table_path)

print("✅ Version 1 created - Added new products")
spark.sql("SELECT * FROM products ORDER BY product_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Update Prices (Version 2)

# COMMAND ----------

# Update prices
delta_table = DeltaTable.forPath(spark, table_path)

# Increase laptop price
delta_table.update(
    condition=col("product_name") == "Laptop", set={"price": col("price") + 100.00}
)

# Decrease mouse price
delta_table.update(
    condition=col("product_name") == "Mouse", set={"price": col("price") - 5.00}
)

print("✅ Version 2 created - Updated prices")
spark.sql("SELECT * FROM products ORDER BY product_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Update Stock Levels (Version 3)

# COMMAND ----------

# Update stock levels
delta_table.update(
    condition=col("category") == "Electronics", set={"stock": col("stock") + 10}
)

print("✅ Version 3 created - Updated stock levels")
spark.sql("SELECT * FROM products ORDER BY product_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. View Version History

# COMMAND ----------

print("📚 Version History:")
delta_table.history().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Time Travel by Version Number

# COMMAND ----------

print("🕐 Time Travel by Version Number:")
print()

# Version 0 - Initial data
print("Version 0 (Initial catalog):")
df_v0_time = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
df_v0_time.show()

print("Version 1 (After adding products):")
df_v1_time = spark.read.format("delta").option("versionAsOf", 1).load(table_path)
df_v1_time.show()

print("Version 2 (After price updates):")
df_v2_time = spark.read.format("delta").option("versionAsOf", 2).load(table_path)
df_v2_time.show()

print("Version 3 (Current - after stock updates):")
df_v3_time = spark.read.format("delta").option("versionAsOf", 3).load(table_path)
df_v3_time.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Time Travel by Timestamp

# COMMAND ----------

# Get timestamps from history
history_df = delta_table.history()
timestamps = history_df.select("timestamp").collect()


if len(timestamps) >= 2:
    # Query by timestamp from version 1
    timestamp_v1 = timestamps[1]["timestamp"]
    df_timestamp = (
        spark.read.format("delta")
        .option("timestampAsOf", timestamp_v1)
        .load(table_path)
    )
    df_timestamp.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Compare Versions

# COMMAND ----------


# Compare product counts and total value
versions = [0, 1, 2, 3]
comparison_data = []

for version in versions:
    df_version = (
        spark.read.format("delta").option("versionAsOf", version).load(table_path)
    )
    product_count = df_version.count()
    total_value = df_version.select(
        sum(col("price") * col("stock")).alias("total_value")
    ).collect()[0]["total_value"]

    comparison_data.append((version, product_count, total_value))

comparison_df = spark.createDataFrame(
    comparison_data, ["version", "product_count", "total_inventory_value"]
)
comparison_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Rollback to Previous Version

# COMMAND ----------

# Rollback to Version 1 (before price updates)

# Rollback to version 1
delta_table.restoreToVersion(1)

spark.sql("SELECT * FROM products ORDER BY product_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Practical Use Cases

# COMMAND ----------

print("🎯 Practical Use Cases for Time Travel:")
print()
print("1. 🔍 Data Auditing:")
print("   • Track all changes to sensitive data")
print("   • Compliance with data governance requirements")
print("   • Forensic analysis of data modifications")
print()
print("2. 🚨 Error Recovery:")
print("   • Rollback accidental data corruption")
print("   • Recover from failed ETL processes")
print("   • Restore data after incorrect transformations")
print()
print("3. 📊 Point-in-Time Analysis:")
print("   • Historical trend analysis")
print("   • Compare data at different time points")
print("   • Analyze impact of changes")
print()
print("4. 🧪 A/B Testing:")
print("   • Test different data processing approaches")
print("   • Validate changes before production")
print("   • Compare results of different algorithms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ Time Travel Demo Complete!")
print()
print("Key Concepts Demonstrated:")
print("1. ✅ Creating multiple table versions")
print("2. ✅ Querying by version number")
print("3. ✅ Querying by timestamp")
print("4. ✅ Comparing different versions")
print("5. ✅ Rolling back to previous versions")
print()
print("Next: Optimization techniques in notebook 03")
