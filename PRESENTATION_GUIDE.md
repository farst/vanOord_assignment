# Presentation Guide - Van Oord Databricks Assignment

## Overview (5 minutes)
Complete enterprise Databricks platform addressing all assignment requirements:
- Production environment deployed via Terraform
- Infrastructure as Code with live demo capability
- All technical questions answered with working code
- Multi-department Power BI architecture

## Live Demo (15 minutes)

### Environment Details
- **Workspace**: https://adb-734420944977024.4.azuredatabricks.net
- **Storage**: voodatabricks77284 (ADLS Gen2)
- **SQL Warehouse**: demo-sql-warehouse

### Infrastructure as Code Demo
```bash
cd databricks_setup

# Show current configuration
terraform output sql_warehouse_cluster_size

# Plan infrastructure change
terraform plan -var="sql_warehouse_cluster_size=Large"

# Apply change to production
terraform apply -var="sql_warehouse_cluster_size=Large" -auto-approve

# Verify change
terraform output sql_warehouse_cluster_size
```

**Key Message**: "Changing infrastructure through code, not clicking buttons in the UI."

## Technical Solutions (15 minutes)

### Show Key Notebooks
- `01_data_pipeline_demo.py` - CSV to Parquet processing
- `06_delta_versioning_demo.py` - Delta time travel
- `02_incremental_pipeline.py` - Incremental processing
- `03_query_optimization.py` - Performance optimization

### Key Technical Points
- **Unity Catalog**: Enterprise governance and security
- **Service Principal**: Secure ADLS Gen2 connectivity
- **Delta Tables**: ACID transactions and time travel
- **Incremental Processing**: Cost-effective data pipelines

## Power BI Architecture (10 minutes)

### Multi-Department Design
- **5 Departments**: Data Platform, Analytics Engineers, Data Governance, SMD, E&E
- **Security**: Unity Catalog with row-level security
- **Access Control**: Role-based permissions per department
- **Implementation**: 16-week phased approach

### Key Benefits
- Self-service analytics for each department
- Centralized governance with distributed access
- Scalable architecture for future growth

## Files Reference
- `ASSIGNMENT_ANSWERS.md` - All technical answers
- `databricks_setup/` - Terraform infrastructure
- `notebooks/` - Working code examples

## Key Messages
1. **Infrastructure as Code**: Real production environment managed through code
2. **Enterprise-Grade**: Unity Catalog, security, and governance
3. **Complete Solution**: Every requirement addressed with working implementations
4. **Production Ready**: Scalable architecture for Van Oord's needs
