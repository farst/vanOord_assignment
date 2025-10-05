# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Optimization
# MAGIC
# MAGIC Demonstrates OPTIMIZE, partitioning, Z-ordering, and liquid clustering for query performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Large Dataset for Performance Testing

# COMMAND ----------


# Create a larger dataset for meaningful performance comparisons
def create_sales_data(num_records=10000):
    """Create sample sales data for optimization testing"""

    data = []
    for i in range(num_records):
        # Generate realistic data
        year = 2020 + (i % 4)  # 2020-2023
        month = (i % 12) + 1
        day = (i % 28) + 1
        customer_id = f"CUST{i % 1000:04d}"
        product_id = f"PROD{i % 100:03d}"
        region = ["North", "South", "East", "West"][i % 4]
        category = ["Electronics", "Clothing", "Books", "Home"][i % 4]

        data.append(
            {
                "sale_id": i + 1,
                "customer_id": customer_id,
                "product_id": product_id,
                "sale_date": f"{year}-{month:02d}-{day:02d}",
                "year": year,
                "month": month,
                "region": region,
                "category": category,
                "quantity": (i % 10) + 1,
                "unit_price": round(10.0 + (i % 500), 2),
                "total_amount": round(((i % 10) + 1) * (10.0 + (i % 500)), 2),
            }
        )

    return spark.createDataFrame(data)


# Create dataset
df_sales = create_sales_data(10000)
df_sales.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Unoptimized Table (Baseline)

# COMMAND ----------

# Create unoptimized table
unoptimized_path = "/tmp/delta_demo/sales_unoptimized"
df_sales.write.format("delta").mode("overwrite").save(unoptimized_path)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS sales_unoptimized
USING DELTA
LOCATION '{unoptimized_path}'
"""
)

spark.sql(f"SELECT COUNT(*) as file_count FROM delta.`{unoptimized_path}`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. OPTIMIZE Command - File Compaction

# COMMAND ----------

# Run OPTIMIZE to compact small files
spark.sql(f"OPTIMIZE delta.`{unoptimized_path}`")

spark.sql(f"SELECT COUNT(*) as file_count FROM delta.`{unoptimized_path}`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Partitioned Table

# COMMAND ----------

# Create partitioned table
partitioned_path = "/tmp/delta_demo/sales_partitioned"

# Partition by year and month
df_sales.write.format("delta").mode("overwrite").partitionBy("year", "month").save(
    partitioned_path
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS sales_partitioned
USING DELTA
LOCATION '{partitioned_path}'
"""
)

spark.sql("SHOW PARTITIONS sales_partitioned").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Z-Ordered Table

# COMMAND ----------

# Create Z-ordered table
zordered_path = "/tmp/delta_demo/sales_zordered"

# First create the table
df_sales.write.format("delta").mode("overwrite").partitionBy("year").save(zordered_path)

# Apply Z-ordering on frequently queried columns
spark.sql(
    f"OPTIMIZE delta.`{zordered_path}` ZORDER BY (customer_id, product_id, region)"
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS sales_zordered
USING DELTA
LOCATION '{zordered_path}'
"""
)

# Z-ordering applied on: customer_id, product_id, region

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Liquid Clustering (Delta 3.0+ Feature)

# COMMAND ----------

# Create liquid clustered table (if supported)
liquid_clustered_path = "/tmp/delta_demo/sales_liquid_clustered"

try:
    # Create table with liquid clustering
    df_sales.write.format("delta").mode("overwrite").save(liquid_clustered_path)

    # Apply liquid clustering
    spark.sql(
        f"""
    ALTER TABLE delta.`{liquid_clustered_path}`
    CLUSTER BY (customer_id, region)
    """
    )

    # Run OPTIMIZE to apply clustering
    spark.sql(f"OPTIMIZE delta.`{liquid_clustered_path}`")

    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS sales_liquid_clustered
    USING DELTA
    LOCATION '{liquid_clustered_path}'
    """
    )

    # Liquid clustering applied on: customer_id, region

except Exception as e:
    print(f"Liquid clustering not supported in this version: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Performance Testing

# COMMAND ----------


def run_performance_test(query, table_name, description):
    """Run performance test and measure execution time"""

    print(f"\n{description}")
    print("=" * 50)

    start_time = time.time()
    result = spark.sql(query)
    row_count = result.count()
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Table: {table_name}")
    print(f"Rows returned: {row_count:,}")
    print(f"Execution time: {execution_time:.2f} seconds")

    return execution_time


# Test queries
test_queries = {
    "Query 1 - Filter by Region": """
        SELECT region, COUNT(*) as sale_count, SUM(total_amount) as total_revenue
        FROM {}
        WHERE region = 'North' AND year = 2022
        GROUP BY region
    """,
    "Query 2 - Filter by Customer": """
        SELECT customer_id, COUNT(*) as purchase_count, SUM(total_amount) as total_spent
        FROM {}
        WHERE customer_id = 'CUST0001'
        GROUP BY customer_id
    """,
    "Query 3 - Complex Filter": """
        SELECT category, region, COUNT(*) as sale_count
        FROM {}
        WHERE category = 'Electronics' AND region IN ('North', 'South') AND year = 2023
        GROUP BY category, region
        ORDER BY sale_count DESC
    """,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Performance Comparison

# COMMAND ----------

# Performance comparison

performance_results = {}

for query_name, query_template in test_queries.items():
    print(f"\n{query_name}")

    # Test unoptimized
    unopt_query = query_template.format("sales_unoptimized")
    unopt_time = run_performance_test(unopt_query, "sales_unoptimized", "Unoptimized")

    # Test partitioned
    part_query = query_template.format("sales_partitioned")
    part_time = run_performance_test(part_query, "sales_partitioned", "Partitioned")

    # Test Z-ordered
    zorder_query = query_template.format("sales_zordered")
    zorder_time = run_performance_test(zorder_query, "sales_zordered", "Z-ordered")

    # Store results
    performance_results[query_name] = {
        "unoptimized": unopt_time,
        "partitioned": part_time,
        "zordered": zorder_time,
        "partition_improvement": ((unopt_time - part_time) / unopt_time) * 100,
        "zorder_improvement": ((unopt_time - zorder_time) / unopt_time) * 100,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Performance Summary

# COMMAND ----------

# Performance summary

for query_name, results in performance_results.items():
    print(f"\n{query_name}:")
    print(f"  Unoptimized: {results['unoptimized']:.2f}s")
    print(
        f"  Partitioned: {results['partitioned']:.2f}s ({results['partition_improvement']:+.1f}%)"
    )
    print(
        f"  Z-ordered:   {results['zordered']:.2f}s ({results['zorder_improvement']:+.1f}%)"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. File Statistics Comparison

# COMMAND ----------

# File statistics comparison

tables = [
    ("sales_unoptimized", "Unoptimized"),
    ("sales_partitioned", "Partitioned"),
    ("sales_zordered", "Z-ordered"),
]

for table_name, description in tables:
    try:
        file_count = spark.sql(
            f"SELECT COUNT(*) as count FROM delta.`/tmp/delta_demo/{table_name}`"
        ).collect()[0]["count"]
        print(f"{description}: {file_count} files")
    except:
        print(f"{description}: Unable to get file count")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Optimization Best Practices

# COMMAND ----------

# Best practices:
# - Partition by frequently filtered columns
# - Apply Z-ordering on WHERE clause columns
# - Run OPTIMIZE regularly after data loads
# - Use liquid clustering for evolving query patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Optimization demonstration complete
