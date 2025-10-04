# Unity Catalog Configuration
# This file configures Unity Catalog with proper external locations and storage credentials

# Access Connector for Unity Catalog
resource "azurerm_databricks_access_connector" "unity_catalog" {
  name                = "databricks-access-connector"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# Grant Access Connector permissions to storage
resource "azurerm_role_assignment" "access_connector_storage" {
  scope                = azurerm_storage_account.databricks_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity_catalog.identity[0].principal_id
}

# Note: Due to metastore limits in the region, we'll use the default metastore
# The workspace should already have access to a default metastore
# We'll focus on creating external locations and storage credentials

# Storage Credential for ADLS Gen2
resource "databricks_storage_credential" "external" {
  name = "adls_credential"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.unity_catalog.id
  }
  comment = "Access Connector credential for ADLS Gen2 access"

  depends_on = [azurerm_databricks_access_connector.unity_catalog]
}

# External Location for Raw Data
resource "databricks_external_location" "raw_data" {
  name            = "raw_data_location"
  url             = "abfss://raw@${azurerm_storage_account.databricks_storage.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.external.name
  comment         = "External location for raw data"

  depends_on = [databricks_storage_credential.external]
}

# External Location for Curated Data
resource "databricks_external_location" "curated_data" {
  name            = "curated_data_location"
  url             = "abfss://curated@${azurerm_storage_account.databricks_storage.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.external.name
  comment         = "External location for curated data"

  depends_on = [databricks_storage_credential.external]
}

# Note: Permissions on external locations will be managed manually in the Databricks UI
# or through SQL commands once Unity Catalog is properly configured
# This avoids issues with group name resolution in Terraform

# Note: Catalogs and schemas will be created manually in the Databricks UI
# or through SQL commands once Unity Catalog is properly configured
# This is because we cannot create a new metastore due to regional limits
