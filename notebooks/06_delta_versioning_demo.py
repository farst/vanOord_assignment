# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table Versioning and Time Travel Demo
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Creating a Delta table with multiple versions
# MAGIC 2. Time travel queries (querying previous versions)
# MAGIC 3. Version history and metadata
# MAGIC 4. Rollback capabilities
# MAGIC 5. Use cases for versioning

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Initial Delta Table

# COMMAND ----------

import time

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create sample data for demonstration
data_v1 = [
    (1, "Alice", "Engineering", 75000, "2024-01-15"),
    (2, "Bob", "Marketing", 65000, "2024-01-20"),
    (3, "Charlie", "Engineering", 80000, "2024-01-25"),
    (4, "Diana", "Sales", 70000, "2024-02-01"),
    (5, "Eve", "HR", 60000, "2024-02-05"),
]

schema = StructType(
    [
        StructField("employee_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("hire_date", StringType(), True),
    ]
)

# Create initial DataFrame
df_v1 = spark.createDataFrame(data_v1, schema)

# Define Delta table path
delta_path = (
    "abfss://curated@voodatabricks77284.dfs.core.windows.net/employee_data_delta/"
)

# Write initial version (Version 0)
df_v1.write.format("delta").mode("overwrite").save(delta_path)

print("✅ Version 0 created - Initial employee data")
df_v1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Version 1 - Add New Employee

# COMMAND ----------

# Add new employee data
new_employee = [(6, "Frank", "Engineering", 85000, "2024-02-10")]

df_new = spark.createDataFrame(new_employee, schema)

# Append to Delta table (Version 1)
df_new.write.format("delta").mode("append").save(delta_path)

print("✅ Version 1 created - Added new employee")
spark.read.format("delta").load(delta_path).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Version 2 - Update Salary

# COMMAND ----------

# Update salary for Alice
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, delta_path)

# Update Alice's salary
delta_table.update(
    condition=col("name") == "Alice", set={"salary": col("salary") + 5000}
)

print("✅ Version 2 created - Updated Alice's salary")
spark.read.format("delta").load(delta_path).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Version 3 - Delete Employee

# COMMAND ----------

# Delete employee with ID 5 (Eve)
delta_table.delete(col("employee_id") == 5)

print("✅ Version 3 created - Deleted employee Eve")
spark.read.format("delta").load(delta_path).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. View Version History

# COMMAND ----------

# Show version history
print("📚 Version History:")
delta_table.history().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Time Travel Queries

# COMMAND ----------

# Query Version 0 (Initial data)
print("🕐 Version 0 - Initial Data:")
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
df_v0.show()

# COMMAND ----------

# Query Version 1 (After adding Frank)
print("🕐 Version 1 - After Adding Frank:")
df_v1 = spark.read.format("delta").option("versionAsOf", 1).load(delta_path)
df_v1.show()

# COMMAND ----------

# Query Version 2 (After salary update)
print("🕐 Version 2 - After Salary Update:")
df_v2 = spark.read.format("delta").option("versionAsOf", 2).load(delta_path)
df_v2.show()

# COMMAND ----------

# Query Version 3 (Current - After deletion)
print("🕐 Version 3 - Current (After Deletion):")
df_v3 = spark.read.format("delta").option("versionAsOf", 3).load(delta_path)
df_v3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Time Travel by Timestamp

# COMMAND ----------

# Get timestamps from history
history_df = delta_table.history()
timestamps = history_df.select("timestamp").collect()

if len(timestamps) >= 2:
    # Query by timestamp (example: query data as of 1 hour ago)
    print("🕐 Query by Timestamp:")

    # Get a timestamp from version 1
    timestamp_v1 = timestamps[1]["timestamp"]
    print(f"Querying data as of: {timestamp_v1}")

    df_timestamp = (
        spark.read.format("delta")
        .option("timestampAsOf", timestamp_v1)
        .load(delta_path)
    )
    df_timestamp.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Compare Versions

# COMMAND ----------

# Compare different versions
print("📊 Version Comparison:")

# Get all versions
v0_count = spark.read.format("delta").option("versionAsOf", 0).load(delta_path).count()
v1_count = spark.read.format("delta").option("versionAsOf", 1).load(delta_path).count()
v2_count = spark.read.format("delta").option("versionAsOf", 2).load(delta_path).count()
v3_count = spark.read.format("delta").option("versionAsOf", 3).load(delta_path).count()

print(f"Version 0 (Initial): {v0_count} employees")
print(f"Version 1 (Added Frank): {v1_count} employees")
print(f"Version 2 (Salary Update): {v2_count} employees")
print(f"Version 3 (Deleted Eve): {v3_count} employees")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Rollback to Previous Version

# COMMAND ----------

# Rollback to Version 1 (before salary update and deletion)
print("🔄 Rolling back to Version 1...")

# Restore to version 1
delta_table.restoreToVersion(1)

print("✅ Rollback completed - Restored to Version 1")
spark.read.format("delta").load(delta_path).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Use Cases for Delta Versioning

# COMMAND ----------

print("🎯 Use Cases for Delta Table Versioning:")
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
print("3. 📊 A/B Testing:")
print("   • Compare different data processing approaches")
print("   • Test new algorithms on historical data")
print("   • Validate data quality improvements")
print()
print("4. 🔄 Development Workflow:")
print("   • Test changes without affecting production")
print("   • Debug data pipeline issues")
print("   • Validate data transformations")
print()
print("5. 📈 Business Intelligence:")
print("   • Historical trend analysis")
print("   • Point-in-time reporting")
print("   • Data lineage tracking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Best Practices

# COMMAND ----------

print("📋 Best Practices for Delta Versioning:")
print()
print("1. 🏷️ Use Descriptive Commit Messages:")
print("   • Document what changed and why")
print("   • Include business context")
print("   • Reference tickets or requirements")
print()
print("2. ⏰ Set Retention Policies:")
print("   • Configure VACUUM to manage storage")
print("   • Balance history vs. storage costs")
print("   • Consider compliance requirements")
print()
print("3. 🔒 Access Control:")
print("   • Limit who can perform rollbacks")
print("   • Audit version access")
print("   • Use Unity Catalog for governance")
print()
print("4. 📊 Monitor Version Growth:")
print("   • Track version history size")
print("   • Monitor storage usage")
print("   • Set up alerts for rapid changes")
print()
print("5. 🧪 Test Rollback Procedures:")
print("   • Practice rollback scenarios")
print("   • Document rollback procedures")
print("   • Train team on version management")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Cleanup

# COMMAND ----------

# Clean up the demo table
# dbutils.fs.rm(delta_path, True)
print("🧹 Demo completed - Delta table preserved for further exploration")
print(f"📍 Table location: {delta_path}")
print("💡 Try querying different versions and timestamps!")
