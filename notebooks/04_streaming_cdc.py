# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Streaming with CDC
# MAGIC
# MAGIC Demonstrates streaming data sources, Change Data Capture patterns, and real-time processing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Streaming Source Data

# COMMAND ----------


# Create sample streaming data (simulating real-time events)
def create_streaming_data():
    """Create sample streaming data for CDC demonstration"""

    # Simulate different types of changes
    events = [
        # Initial inserts
        {
            "event_type": "INSERT",
            "customer_id": "CUST001",
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "status": "active",
            "last_updated": "2024-01-01 10:00:00",
        },
        {
            "event_type": "INSERT",
            "customer_id": "CUST002",
            "name": "Bob Smith",
            "email": "bob@example.com",
            "status": "active",
            "last_updated": "2024-01-01 10:01:00",
        },
        {
            "event_type": "INSERT",
            "customer_id": "CUST003",
            "name": "Charlie Brown",
            "email": "charlie@example.com",
            "status": "pending",
            "last_updated": "2024-01-01 10:02:00",
        },
        # Updates
        {
            "event_type": "UPDATE",
            "customer_id": "CUST001",
            "name": "Alice Johnson",
            "email": "alice.johnson@company.com",
            "status": "active",
            "last_updated": "2024-01-01 11:00:00",
        },
        {
            "event_type": "UPDATE",
            "customer_id": "CUST003",
            "name": "Charlie Brown",
            "email": "charlie@example.com",
            "status": "active",
            "last_updated": "2024-01-01 11:30:00",
        },
        # More inserts
        {
            "event_type": "INSERT",
            "customer_id": "CUST004",
            "name": "Diana Prince",
            "email": "diana@example.com",
            "status": "active",
            "last_updated": "2024-01-01 12:00:00",
        },
        {
            "event_type": "INSERT",
            "customer_id": "CUST005",
            "name": "Eve Wilson",
            "email": "eve@example.com",
            "status": "pending",
            "last_updated": "2024-01-01 12:15:00",
        },
        # Delete (soft delete by updating status)
        {
            "event_type": "UPDATE",
            "customer_id": "CUST002",
            "name": "Bob Smith",
            "email": "bob@example.com",
            "status": "inactive",
            "last_updated": "2024-01-01 13:00:00",
        },
    ]

    return events


# Create streaming data directory
streaming_data_path = "/tmp/delta_demo/streaming_data"
dbutils.fs.mkdirs(streaming_data_path)

# Write events as JSON files (simulating streaming source)
events = create_streaming_data()
for i, event in enumerate(events):
    file_path = f"{streaming_data_path}/event_{i:03d}.json"
    dbutils.fs.put(file_path, json.dumps(event), True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define Streaming Schema

# COMMAND ----------

# Define schema for streaming data
streaming_schema = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("status", StringType(), True),
        StructField("last_updated", StringType(), True),
    ]
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Target Delta Table

# COMMAND ----------

# Create target Delta table for CDC
target_table_path = "/tmp/delta_demo/customers_cdc"

# Initial empty table
empty_df = spark.createDataFrame([], streaming_schema)
empty_df.write.format("delta").mode("overwrite").save(target_table_path)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS customers_cdc
USING DELTA
LOCATION '{target_table_path}'
"""
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Streaming Query with CDC Processing

# COMMAND ----------

# Create streaming DataFrame
streaming_df = (
    spark.readStream.format("json").schema(streaming_schema).load(streaming_data_path)
)

# Add watermark for late data handling
streaming_df_with_watermark = streaming_df.withColumn(
    "last_updated_ts", to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm:ss")
).withWatermark("last_updated_ts", "1 hour")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Process Stream with CDC Logic

# COMMAND ----------


def process_cdc_batch(df, epoch_id):
    """Process each batch of streaming data with CDC logic"""

    # Convert to Pandas for easier processing
    df_pandas = df.toPandas()

    # Process each event
    for _, row in df_pandas.iterrows():
        event_type = row["event_type"]
        customer_id = row["customer_id"]

        if event_type == "INSERT":
            # Insert new record
            new_record = spark.createDataFrame(
                [
                    {
                        "customer_id": row["customer_id"],
                        "name": row["name"],
                        "email": row["email"],
                        "status": row["status"],
                        "last_updated": row["last_updated"],
                        "cdc_operation": "INSERT",
                        "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                ]
            )

            new_record.write.format("delta").mode("append").save(target_table_path)

        elif event_type == "UPDATE":
            # Update existing record using DeltaTable
            from delta.tables import DeltaTable

            delta_table = DeltaTable.forPath(spark, target_table_path)

            delta_table.update(
                condition=col("customer_id") == customer_id,
                set={
                    "name": lit(row["name"]),
                    "email": lit(row["email"]),
                    "status": lit(row["status"]),
                    "last_updated": lit(row["last_updated"]),
                    "cdc_operation": lit("UPDATE"),
                    "processed_at": lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                },
            )

        elif event_type == "DELETE":
            # Delete record (soft delete by updating status)
            from delta.tables import DeltaTable

            delta_table = DeltaTable.forPath(spark, target_table_path)

            delta_table.update(
                condition=col("customer_id") == customer_id,
                set={
                    "status": lit("deleted"),
                    "last_updated": lit(row["last_updated"]),
                    "cdc_operation": lit("DELETE"),
                    "processed_at": lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                },
            )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Start Streaming Query

# COMMAND ----------

# Start streaming query
query = (
    streaming_df_with_watermark.writeStream.foreachBatch(process_cdc_batch)
    .option("checkpointLocation", "/tmp/delta_demo/checkpoint")
    .trigger(once=True)
    .start()
)

# Wait for completion
query.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. View CDC Results

# COMMAND ----------

# View the final CDC results
spark.sql("SELECT * FROM customers_cdc ORDER BY last_updated").show()

spark.sql(
    """
SELECT
    cdc_operation,
    COUNT(*) as operation_count
FROM customers_cdc
GROUP BY cdc_operation
ORDER BY operation_count DESC
"""
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Streaming Analytics

# COMMAND ----------

# Real-time analytics on streaming data
spark.sql(
    """
SELECT
    status,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM customers_cdc
WHERE status != 'deleted'
GROUP BY status
ORDER BY customer_count DESC
"""
).show()

spark.sql(
    """
SELECT
    customer_id,
    name,
    cdc_operation,
    last_updated,
    processed_at
FROM customers_cdc
ORDER BY processed_at DESC
LIMIT 5
"""
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Continuous Streaming Example

# COMMAND ----------

# For continuous streaming:
# query = streaming_df_with_watermark.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", "/tmp/delta_demo/checkpoint") \
#     .trigger(processingTime='10 seconds') \
#     .toTable("customers_streaming")
# query.start()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. CDC Use Cases

# COMMAND ----------

# CDC use cases:
# - Real-time analytics and dashboards
# - Data synchronization across systems
# - Event-driven architecture
# - Audit and compliance tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Best Practices

# COMMAND ----------

# Best practices:
# - Set appropriate watermark intervals
# - Always use checkpointing for fault tolerance
# - Ensure operations are idempotent
# - Monitor streaming query health

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Streaming CDC demonstration complete
