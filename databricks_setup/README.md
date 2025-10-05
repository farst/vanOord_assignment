# Databricks Infrastructure Setup

Terraform module for deploying Azure Databricks workspace with Unity Catalog and ADLS Gen2 storage.

## Prerequisites

- Azure CLI installed and authenticated
- Terraform >= 1.0
- Appropriate Azure permissions

## Deployment

```bash
# Initialize Terraform
terraform init

# Review plan
terraform plan

# Deploy infrastructure
terraform apply
```

## Configuration

Key variables in `terraform.tfvars`:
- `sql_warehouse_cluster_size`: Warehouse size (2X-Small, Small, Medium, Large, 2X-Large)
- `location`: Azure region
- `workspace_name`: Databricks workspace name

## Outputs

After deployment, outputs include:
- Workspace URL
- Storage account details
- SQL warehouse information
- Service principal credentials

## Live Demo

Modify warehouse size during presentation:
```bash
terraform plan -var="sql_warehouse_cluster_size=Large"
terraform apply -var="sql_warehouse_cluster_size=Large" -auto-approve
```
