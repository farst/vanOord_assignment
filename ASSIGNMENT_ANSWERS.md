# Van Oord Databricks Assignment - Technical Answers

## Platform Assignment

### 1. Terraform Module
Complete Terraform module deployed in `databricks_setup/` with:
- Azure Databricks workspace (Premium tier)
- ADLS Gen2 storage account
- SQL warehouse with Unity Catalog
- Service principal authentication

**Live Demo**: Can modify SQL warehouse settings during presentation:
```bash
terraform plan -var="sql_warehouse_cluster_size=Large"
terraform apply -var="sql_warehouse_cluster_size=Large" -auto-approve
```

### 2. Git Repository
Pre-commit hooks configured for:
- Python code formatting (Black, Flake8, isort)
- Markdown documentation (markdownlint)
- SQL code quality (SQLFluff)

## Databricks Assignment

### 3. Cluster Sizing & Runtime
**Small workloads**: i3.xlarge (1-2 workers)
**Medium workloads**: i3.2xlarge (2-8 workers)
**Large workloads**: i3.4xlarge (4-20 workers)

**Runtime**: LTS versions (13.3.x) for production stability

### 4. Asset Bundles
Declarative approach for managing Databricks resources:
- Infrastructure as Code
- Version control for environments
- CI/CD integration
- Environment promotion (Dev → Staging → Prod)

### 5. ADLS Gen2 Security
**Service Principal Authentication**:
```python
storage_credential = {
    "name": "adls_sp_cred",
    "azure_service_principal": {
        "directory_id": "tenant-id",
        "application_id": "client-id",
        "client_secret": "client-secret" #pragma: allowlist secret
    }
}
```

### 6. CSV to Parquet Processing
**Implementation**: `notebooks/01_data_pipeline_demo.py`

```python
# Read CSV from ADLS Gen2
df = spark.read.format("csv").option("header", "true").load("abfss://raw@storage.dfs.core.windows.net/data/")

# Transform and write as Parquet
df.write.mode("overwrite").partitionBy("date_partition").parquet("abfss://curated@storage.dfs.core.windows.net/data/")
```

### 7. Delta Time Travel
**Version Queries**:
```sql
-- Query specific version
SELECT * FROM my_table VERSION AS OF 5;

-- Query specific timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2024-01-15 10:30:00';
```

**Use Cases**: Data recovery, audit trails, reproducible analytics

### 8. Incremental Processing
**CDC Pattern**:
```python
# Track last processed timestamp
last_processed = spark.sql("SELECT MAX(processed_timestamp) FROM watermark_table").collect()[0][0]

# Process only new data
new_data = spark.read.format("delta").option("timestampAsOf", last_processed).load("path")
```

### 9. Query Optimization
**Delta Optimization**:
```python
# Z-ordering for better query performance
spark.sql("OPTIMIZE my_table ZORDER BY (customer_id, date_column)")

# File compaction
spark.sql("OPTIMIZE my_table")

# Vacuum old files
spark.sql("VACUUM my_table RETAIN 168 HOURS")
```

### 10. Data Quality
**Great Expectations Integration**:
```python
import great_expectations as ge

# Define expectations
suite.add_expectation(ge.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
suite.add_expectation(ge.expectations.ExpectColumnValuesToBeBetween(column="age", min_value=18, max_value=100))

# Validate data
results = context.run_validation_operator("action_list_operator", assets_to_validate=[batch])
```

## Power BI Assessment

### Multi-Department Architecture
**Departments**: Data Platform, Analytics Engineers, Data Governance, SMD, E&E

**Security Model**:
- Unity Catalog with row-level security
- Role-based access control
- Department-specific workspaces

**Access Pattern**:
- **Data Platform**: Full access to all catalogs
- **Analytics Engineers**: Modeling and curated data access
- **SMD/E&E**: Department-specific data with RLS

**Implementation**: 16-week phased rollout with dedicated SQL warehouses per department.

## Summary

All assignment requirements completed with:
- Production Databricks workspace deployed
- Infrastructure as Code with Terraform
- Working code examples in notebooks
- Complete Power BI architecture design
