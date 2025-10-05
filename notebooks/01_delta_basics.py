# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Basics
# MAGIC
# MAGIC Core Delta Lake functionality: table creation, CRUD operations, schema evolution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Sample Data

# COMMAND ----------

# Create sample employee data
sample_data = [
    (1, "Alice Johnson", "Engineering", 75000, "2024-01-15"),
    (2, "Bob Smith", "Marketing", 65000, "2024-01-20"),
    (3, "Charlie Brown", "Engineering", 80000, "2024-01-25"),
    (4, "Diana Prince", "Sales", 70000, "2024-02-01"),
    (5, "Eve Wilson", "HR", 60000, "2024-02-05"),
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

df = spark.createDataFrame(sample_data, schema)

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Delta Table

# COMMAND ----------

# Define table path (using temporary location for demo)
table_path = "/tmp/delta_demo/employees"

# Write as Delta table
df.write.format("delta").mode("overwrite").save(table_path)

# Create table reference for easier querying
spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS employees
USING DELTA
LOCATION '{table_path}'
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Basic Queries

# COMMAND ----------

# Query the Delta table
spark.sql("SELECT * FROM employees ORDER BY employee_id").show()

spark.sql("SELECT name, salary FROM employees WHERE department = 'Engineering'").show()

spark.sql(
    """
SELECT
    department,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary,
    MAX(salary) as max_salary,
    MIN(salary) as min_salary
FROM employees
GROUP BY department
ORDER BY avg_salary DESC
"""
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. INSERT Operations

# COMMAND ----------

# Add new employee
new_employee = spark.createDataFrame(
    [(6, "Frank Miller", "Engineering", 85000, "2024-02-10")], schema
)

# Insert new record
new_employee.write.format("delta").mode("append").save(table_path)

spark.sql("SELECT * FROM employees ORDER BY employee_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. UPDATE Operations

# COMMAND ----------

# Update salary using DeltaTable API
delta_table = DeltaTable.forPath(spark, table_path)

# Give Alice a raise
delta_table.update(
    condition=col("name") == "Alice Johnson", set={"salary": col("salary") + 5000}
)

spark.sql("SELECT name, salary FROM employees WHERE name = 'Alice Johnson'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. DELETE Operations

# COMMAND ----------

# Delete employee
delta_table.delete(col("employee_id") == 5)

spark.sql("SELECT * FROM employees ORDER BY employee_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. MERGE Operations (Upsert)

# COMMAND ----------

# Prepare data for merge (some updates, some new records)
merge_data = [
    (2, "Bob Smith", "Marketing", 70000, "2024-01-20"),  # Update salary
    (7, "Grace Hopper", "Engineering", 90000, "2024-02-15"),  # New employee
    (8, "Alan Turing", "Engineering", 95000, "2024-02-20"),  # New employee
]

merge_df = spark.createDataFrame(merge_data, schema)

# Perform merge
delta_table.alias("target").merge(
    merge_df.alias("source"), "target.employee_id = source.employee_id"
).whenMatchedUpdate(
    set={
        "name": "source.name",
        "department": "source.department",
        "salary": "source.salary",
        "hire_date": "source.hire_date",
    }
).whenNotMatchedInsert(
    values={
        "employee_id": "source.employee_id",
        "name": "source.name",
        "department": "source.department",
        "salary": "source.salary",
        "hire_date": "source.hire_date",
    }
).execute()

spark.sql("SELECT * FROM employees ORDER BY employee_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Table Metadata

# COMMAND ----------

# Show table details
spark.sql("DESCRIBE employees").show()

delta_table.history().show()

spark.sql("SHOW TBLPROPERTIES employees").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Schema Evolution

# COMMAND ----------

# Add new column (bonus)
spark.sql("ALTER TABLE employees ADD COLUMNS (bonus INT)")

# Update bonus for some employees
delta_table.update(condition=col("department") == "Engineering", set={"bonus": 5000})

delta_table.update(condition=col("department") != "Engineering", set={"bonus": 2000})

spark.sql("SELECT name, department, salary, bonus FROM employees").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Delta Lake basics demonstration complete
