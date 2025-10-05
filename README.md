# Van Oord Databricks Assignment

Databricks platform implementation with infrastructure as code and working demonstrations.

## Implementation

- Production Azure Databricks workspace deployed via Terraform
- Infrastructure as Code with live demo capability
- Technical solutions for all assignment questions
- Multi-department Power BI architecture

## Quick Start

```bash
cd databricks_setup
terraform plan -var="sql_warehouse_cluster_size=Large"
terraform apply -var="sql_warehouse_cluster_size=Large" -auto-approve
```

## Environment

- Workspace: https://adb-734420944977024.4.azuredatabricks.net
- Storage: voodatabricks77284 (ADLS Gen2)
- SQL Warehouse: demo-sql-warehouse

## Files

- `ASSIGNMENT_ANSWERS.md` - Technical answers
- `PRESENTATION_GUIDE.md` - Demo guide
- `databricks_setup/` - Terraform infrastructure
- `notebooks/` - Working examples

## Status

Complete - All requirements addressed with working implementations
