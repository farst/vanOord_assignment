# Databricks Technical Questions - Comprehensive Answers

## 1. 🎯 Cluster Sizing, Autoscaling, and Runtime Version Decisions

### **Cluster Sizing Decision Framework**

#### **Factors to Consider:**

- **Data volume**: Size of datasets being processed
- **Processing complexity**: CPU/memory intensive operations
- **Concurrent users**: Number of simultaneous users/jobs
- **Budget constraints**: Cost optimization requirements
- **Performance SLAs**: Response time requirements

#### **Sizing Guidelines:**

```python
# Small workloads (development, testing)
cluster_config = {
    "min_workers": 1,
    "max_workers": 2,
    "node_type_id": "i3.xlarge",  # 4 cores, 30.5 GB RAM
    "driver_node_type_id": "i3.xlarge"
}

# Medium workloads (production ETL)
cluster_config = {
    "min_workers": 2,
    "max_workers": 8,
    "node_type_id": "i3.2xlarge",  # 8 cores, 61 GB RAM
    "driver_node_type_id": "i3.2xlarge"
}

# Large workloads (big data processing)
cluster_config = {
    "min_workers": 4,
    "max_workers": 20,
    "node_type_id": "i3.4xlarge",  # 16 cores, 122 GB RAM
    "driver_node_type_id": "i3.4xlarge"
}
```

#### **Autoscaling Configuration:**

- **Scale-up**: Immediate for responsive performance
- **Scale-down**: 10-15 minutes to allow job completion
- **Min workers**: 1-2 for cost optimization
- **Max workers**: Based on workload requirements and budget

#### **Runtime Version Selection:**

- **LTS versions**: For production stability (e.g., 13.3.x)
- **Latest versions**: For new features (e.g., 14.x)
- **Custom images**: For specific library requirements

---

## 2. 📦 Databricks Asset Bundle Usage

### **What are Asset Bundles?**

Asset bundles are a declarative approach to manage Databricks resources as code, enabling:

- **Infrastructure as Code**: Complete environment management
- **Version Control**: Track changes across environments
- **CI/CD Integration**: Automated deployments
- **Environment Promotion**: Dev → Staging → Production

### **Use Cases:**

```yaml
# databricks.yml
bundle:
  name: van-oord-databricks

resources:
  jobs:
    etl_pipeline:
      name: "ETL Pipeline"
      tasks:
        - task_key: "extract_data"
          notebook_task:
            notebook_path: "notebooks/data_ingestion/extract.ipynb"
        - task_key: "transform_data"
          notebook_task:
            notebook_path: "notebooks/data_ingestion/transform.ipynb"

  pipelines:
    data_pipeline:
      name: "Data Pipeline"
      libraries:
        - notebook:
            path: "notebooks/pipelines/main.ipynb"
```

### **Benefits:**

- **Reproducible deployments**
- **Environment consistency**
- **Rollback capabilities**
- **Team collaboration**

---

## 3. 🔐 Secure ADLS Gen2 Connectivity

### **Authentication Methods:**

#### **1. Managed Identity (Recommended)**

```python
# Using Access Connector
storage_credential = {
    "name": "adls_mi_cred",
    "azure_managed_identity": {
        "access_connector_id": "/subscriptions/.../accessConnectors/my-connector"
    }
}
```

#### **2. Service Principal**

```python
# Using service principal
storage_credential = {
    "name": "adls_sp_cred",
    "azure_service_principal": {
        "directory_id": "tenant-id",
        "application_id": "client-id",
        "client_secret": "client-secret"  # pragma: allowlist secret
    }
}
```

### **Security Best Practices:**

- **Principle of least privilege**: Grant minimal required permissions
- **Access Connector**: Use for managed identity authentication
- **Network security**: VNet integration and private endpoints
- **Encryption**: Data encryption at rest and in transit
- **Audit logging**: Complete access audit trail

---

## 4. 📊 Data Processing Pipeline (CSV → Parquet)

### **Implementation Example:**

```python
# Read CSV from ADLS Gen2
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://raw@storageaccount.dfs.core.windows.net/data/sample.csv")

# Data cleaning and transformation
cleaned_df = df \
    .dropDuplicates() \
    .fillna({"missing_column": "default_value"}) \
    .withColumn("processed_date", current_timestamp()) \
    .filter(col("status") == "active")

# Write as Parquet with partitioning
cleaned_df.write \
    .mode("overwrite") \
    .partitionBy("date_partition") \
    .parquet("abfss://curated@storageaccount.dfs.core.windows.net/data/")
```

### **Benefits of Parquet:**

- **Columnar storage**: Better compression and query performance
- **Schema evolution**: Handle schema changes gracefully
- **Type safety**: Preserve data types
- **Compression**: Efficient storage utilization

---

## 5. ⏰ Delta Table Versioning and Time Travel

### **Time Travel Queries:**

```sql
-- Query table as of a specific version
SELECT * FROM my_table VERSION AS OF 5;

-- Query table as of a specific timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2024-01-15 10:30:00';

-- Query table between versions
SELECT * FROM my_table VERSION BETWEEN 3 AND 7;

-- Query table between timestamps
SELECT * FROM my_table TIMESTAMP BETWEEN '2024-01-15' AND '2024-01-16';
```

### **Use Cases:**

- **Data recovery**: Restore accidentally deleted data
- **Audit trails**: Track data changes over time
- **Reproducible analytics**: Ensure consistent results
- **Compliance**: Meet regulatory requirements
- **Debugging**: Investigate data quality issues

### **When to Use:**

- **Data governance**: Track all data changes
- **Compliance requirements**: Audit trails for sensitive data
- **Data quality issues**: Rollback bad data
- **Experimentation**: Safe data exploration

---

## 6. 🔄 Incremental Pipeline Design

### **Change Data Capture (CDC) Pattern:**

```python
# Track last processed timestamp
last_processed = spark.sql("SELECT MAX(processed_timestamp) FROM watermark_table").collect()[0][0]

# Read only new data
new_data = spark.read \
    .format("delta") \
    .option("timestampAsOf", last_processed) \
    .load("abfss://raw@storage.dfs.core.windows.net/data/")

# Process incremental data
processed_data = new_data \
    .filter(col("created_date") > last_processed) \
    .withColumn("processed_timestamp", current_timestamp())

# Merge with existing data
processed_data.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("abfss://curated@storage.dfs.core.windows.net/data/")
```

### **Incremental Processing Strategies:**

- **Timestamp-based**: Use creation/modification timestamps
- **Change Data Capture**: Database CDC streams
- **File-based**: Track processed file names
- **Hash-based**: Compare data hashes for changes

### **Benefits:**

- **Cost efficiency**: Process only new data
- **Performance**: Faster processing times
- **Resource optimization**: Reduced compute usage
- **Real-time capabilities**: Near real-time data processing

---

## 7. 🚀 Query Performance Optimization

### **Delta Table Optimization Techniques:**

#### **1. Z-Ordering (Clustering)**

```python
# Optimize table with Z-ordering
spark.sql("""
    OPTIMIZE my_table
    ZORDER BY (customer_id, date_column)
""")
```

#### **2. Partitioning**

```python
# Partition by date for time-series data
df.write \
    .partitionBy("year", "month", "day") \
    .format("delta") \
    .save("path/to/partitioned/table")
```

#### **3. Compaction**

```python
# Compact small files
spark.sql("""
    OPTIMIZE my_table
""")
```

#### **4. Vacuum Old Files**

```python
# Remove old files (retain 7 days)
spark.sql("""
    VACUUM my_table RETAIN 168 HOURS
""")
```

### **Query Optimization Best Practices:**

- **Predicate pushdown**: Filter early in the query
- **Column pruning**: Select only needed columns
- **Broadcast joins**: For small lookup tables
- **Bucketing**: For large join operations
- **Statistics collection**: Enable automatic statistics

---

## 8. ✅ Data Quality Integration

### **Great Expectations Integration:**

```python
import great_expectations as ge

# Create data context
context = ge.get_context()

# Create expectation suite
suite = context.create_expectation_suite("data_quality_suite")

# Add expectations
suite.add_expectation(
    ge.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
)

suite.add_expectation(
    ge.expectations.ExpectColumnValuesToBeBetween(
        column="age", min_value=18, max_value=100
    )
)

# Validate data
results = context.run_validation_operator(
    "action_list_operator",
    assets_to_validate=[batch],
    run_id="validation_run"
)
```

### **Deequ Integration (AWS):**

```python
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

# Define data quality checks
check = Check(spark, CheckLevel.Error, "Data Quality Check") \
    .hasSize(lambda x: x >= 1000) \
    .isComplete("customer_id") \
    .isUnique("customer_id") \
    .hasCompleteness("email", lambda x: x >= 0.8)

# Run verification
verificationResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(check) \
    .run()

# Get results
results = verificationResult.checkResults
```

### **Quality Monitoring:**

- **Automated validation**: Run checks on data ingestion
- **Alerting**: Notify on quality failures
- **Documentation**: Data quality documentation
- **Metrics tracking**: Quality score trends

---

## 9. 🏛️ Unity Catalog Best Practices

### **Organization Structure:**

```text
Catalog (Business Domain)
├── Schema (Data Product)
│   ├── Table (Entity)
│   ├── View (Derived Entity)
│   └── Function (Business Logic)
```

### **Naming Conventions:**

```sql
-- Catalogs: Business domain
CREATE CATALOG finance;
CREATE CATALOG hr;
CREATE CATALOG operations;

-- Schemas: Data product or environment
CREATE SCHEMA finance.reporting;
CREATE SCHEMA finance.raw_data;
CREATE SCHEMA finance.curated;

-- Tables: Entity with version
CREATE TABLE finance.curated.customers_v1;
CREATE TABLE finance.curated.transactions_v2;
```

### **Access Control Matrix:**

| Role | Catalog | Schema | Table | Permissions |
|------|---------|--------|-------|-------------|
| Data Engineer | All | All | All | CREATE, MODIFY, SELECT |
| Data Analyst | finance, hr | reporting, analytics | All | SELECT |
| Business User | finance | reporting | specific tables | SELECT |
| Data Steward | All | All | All | CREATE, MODIFY, SELECT, APPLY |

### **Governance Framework:**

- **Data classification**: Public, Internal, Confidential, Restricted
- **Retention policies**: Automated data lifecycle management
- **Audit logging**: Complete access and modification audit trail
- **Compliance**: GDPR, SOX, HIPAA compliance features
- **Data lineage**: Track data flow and dependencies

### **Implementation Checklist:**

- [ ] Create catalog hierarchy based on business domains
- [ ] Implement consistent naming conventions
- [ ] Set up role-based access control
- [ ] Configure data classification
- [ ] Enable audit logging
- [ ] Implement retention policies
- [ ] Set up data lineage tracking
- [ ] Configure compliance features

---

## 10. 🎯 Summary and Recommendations

### **Key Takeaways:**

1. **Start with Unity Catalog**: Implement governance from day one
2. **Use Managed Identity**: Secure and simple authentication
3. **Implement Incremental Processing**: Cost-effective and performant
4. **Monitor Data Quality**: Automated validation and alerting
5. **Optimize Continuously**: Regular performance tuning
6. **Document Everything**: Maintain comprehensive documentation

### **Next Steps:**

1. Deploy the infrastructure using Terraform
2. Set up Unity Catalog with proper governance
3. Implement data quality checks
4. Build incremental data pipelines
5. Configure Power BI integration
6. Train users on best practices

This comprehensive approach ensures a robust, scalable, and well-governed data platform that meets enterprise requirements while enabling self-service analytics.
