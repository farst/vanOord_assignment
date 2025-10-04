# Terraform configuration for Azure Databricks with Unity Catalog
# This configuration creates a complete Databricks environment with Unity Catalog

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

# Configure providers
provider "azurerm" {
  features {}
  subscription_id = "5376c45f-875a-490c-9316-fa645769b3d7"
}

# Use Azure CLI authentication for initial setup
# This allows the current user to create service principals
provider "databricks" {
  host                        = azurerm_databricks_workspace.this.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
  # Using Azure CLI authentication for initial setup
}

provider "azuread" {}

# Data sources
data "azurerm_client_config" "current" {}

# Random integer for unique naming
resource "random_integer" "suffix" {
  min = 10000
  max = 99999
}

# Resource Group
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name != "" ? var.resource_group_name : "rg-databricks-${var.environment}"
  location = var.location

  tags = merge(var.tags, {
    Environment = var.environment
    Project     = var.project_name
  })
}

# Grant admin permissions to current user
resource "azurerm_role_assignment" "rg_admin" {
  scope                = azurerm_resource_group.rg.id
  role_definition_name = "Owner"
  principal_id         = data.azurerm_client_config.current.object_id
}

# User Assigned Managed Identity for Databricks
resource "azurerm_user_assigned_identity" "databricks_identity" {
  name                = "databricks-mi"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "this" {
  name                         = var.workspace_name != "" ? var.workspace_name : "databricks-${var.environment}-ws"
  resource_group_name          = azurerm_resource_group.rg.name
  location                     = azurerm_resource_group.rg.location
  sku                          = var.workspace_sku
  managed_resource_group_name  = "databricks-rg-${azurerm_resource_group.rg.name}"
  customer_managed_key_enabled = false

  tags = merge(var.tags, {
    Environment = var.environment
    Project     = var.project_name
  })

  depends_on = [azurerm_role_assignment.rg_admin]
}

# Grant admin permissions to current user on Databricks workspace
resource "azurerm_role_assignment" "databricks_admin" {
  scope                = azurerm_databricks_workspace.this.id
  role_definition_name = "Owner"
  principal_id         = data.azurerm_client_config.current.object_id
}

# Storage Account for data
resource "azurerm_storage_account" "databricks_storage" {
  name                     = var.storage_account_name != "" ? var.storage_account_name : "voodatabricks${random_integer.suffix.result}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  tags = merge(var.tags, {
    Environment = var.environment
    Project     = var.project_name
  })
}

# Storage containers
resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_id    = azurerm_storage_account.databricks_storage.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "curated" {
  name                  = "curated"
  storage_account_id    = azurerm_storage_account.databricks_storage.id
  container_access_type = "private"
}

# Grant storage permissions to managed identity
resource "azurerm_role_assignment" "mi_storage_blob_contributor" {
  scope                            = azurerm_storage_account.databricks_storage.id
  role_definition_name             = "Storage Blob Data Contributor"
  principal_id                     = azurerm_user_assigned_identity.databricks_identity.principal_id
  skip_service_principal_aad_check = true
}

# Add service principal to Databricks workspace
resource "databricks_service_principal" "databricks_sp" {
  application_id = azuread_application.databricks_sp.client_id
  display_name   = azuread_application.databricks_sp.display_name
  active         = true
}

# Data source to get the admins group ID
data "databricks_group" "admins" {
  display_name = "admins"
  depends_on   = [azurerm_databricks_workspace.this]
}

# Add service principal to workspace admins group
resource "databricks_group_member" "service_principal_admin" {
  group_id  = data.databricks_group.admins.id
  member_id = databricks_service_principal.databricks_sp.id
}

# SQL Warehouse for data processing
resource "databricks_sql_endpoint" "sql_wh" {
  name                      = var.sql_warehouse_name
  cluster_size              = var.sql_warehouse_cluster_size
  auto_stop_mins            = var.sql_warehouse_auto_stop_mins
  max_num_clusters          = var.sql_warehouse_max_clusters
  min_num_clusters          = var.sql_warehouse_min_clusters
  enable_serverless_compute = var.enable_serverless_compute

  depends_on = [databricks_service_principal.databricks_sp]
}

# Service Principal for Unity Catalog (if needed later)
resource "azuread_application" "databricks_sp" {
  display_name = "databricks-unity-catalog-sp"
}

resource "azuread_service_principal" "databricks_sp" {
  client_id = azuread_application.databricks_sp.client_id
}

resource "azuread_service_principal_password" "databricks_sp" {
  service_principal_id = azuread_service_principal.databricks_sp.id
  display_name         = "databricks-sp-secret"
}

# Grant storage permissions to service principal
resource "azurerm_role_assignment" "sp_storage_blob_contributor" {
  scope                            = azurerm_storage_account.databricks_storage.id
  role_definition_name             = "Storage Blob Data Contributor"
  principal_id                     = azuread_service_principal.databricks_sp.object_id
  skip_service_principal_aad_check = true
}
