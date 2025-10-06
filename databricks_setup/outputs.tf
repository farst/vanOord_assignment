# Outputs for Databricks Infrastructure

# Workspace information
output "workspace_url" {
  description = "Databricks workspace URL"
  value       = azurerm_databricks_workspace.this.workspace_url
}

output "workspace_id" {
  description = "Databricks workspace ID"
  value       = azurerm_databricks_workspace.this.workspace_id
}

# Storage information
output "storage_account_name" {
  description = "Name of the storage account"
  value       = azurerm_storage_account.databricks_storage.name
}

output "storage_account_url" {
  description = "URL of the storage account"
  value       = azurerm_storage_account.databricks_storage.primary_dfs_endpoint
}

output "raw_container_url" {
  description = "URL for raw data container"
  value       = "abfss://raw@${azurerm_storage_account.databricks_storage.name}.dfs.core.windows.net/"
}

output "curated_container_url" {
  description = "URL for curated data container"
  value       = "abfss://curated@${azurerm_storage_account.databricks_storage.name}.dfs.core.windows.net/"
}

# Managed Identity information
output "managed_identity_client_id" {
  description = "Client ID of the managed identity"
  value       = azurerm_user_assigned_identity.databricks_identity.client_id
}

output "managed_identity_principal_id" {
  description = "Principal ID of the managed identity"
  value       = azurerm_user_assigned_identity.databricks_identity.principal_id
}

# Service Principal information
output "service_principal_client_id" {
  description = "Client ID of the service principal"
  value       = azuread_application.databricks_sp.client_id
}

output "service_principal_object_id" {
  description = "Object ID of the service principal"
  value       = azuread_service_principal.databricks_sp.object_id
}

output "service_principal_secret" {
  description = "Client secret of the service principal"
  value       = azuread_service_principal_password.databricks_sp.value
  sensitive   = true
}

# SQL Warehouse information
output "sql_warehouse_id" {
  description = "ID of the SQL warehouse"
  value       = databricks_sql_endpoint.sql_wh.id
}

output "sql_warehouse_name" {
  description = "Name of the SQL warehouse"
  value       = databricks_sql_endpoint.sql_wh.name
}

output "sql_warehouse_cluster_size" {
  description = "Current cluster size of the SQL warehouse"
  value       = databricks_sql_endpoint.sql_wh.cluster_size
}

# Connection information for notebooks
output "connection_info" {
  description = "Connection information for Databricks notebooks"
  value = {
    workspace_url   = "${azurerm_databricks_workspace.this.workspace_url}/?o=${azurerm_databricks_workspace.this.workspace_id}"
    storage_account = azurerm_storage_account.databricks_storage.name
    sql_warehouse   = databricks_sql_endpoint.sql_wh.name
  }
}

# Setup completion message
output "workspace_ready_message" {
  description = "Message indicating workspace is ready"
  value       = "Databricks workspace is ready! Access it at: ${azurerm_databricks_workspace.this.workspace_url}/?o=${azurerm_databricks_workspace.this.workspace_id}"
}

# Access instructions
output "access_instructions" {
  description = "Instructions for accessing the workspace"
  value       = <<-EOT
    Access Instructions:

    1. Open your browser and go to: ${azurerm_databricks_workspace.this.workspace_url}/?o=${azurerm_databricks_workspace.this.workspace_id}
    2. Sign in with your Azure credentials
    3. Create a new notebook and use these connection details:
       - Storage Account: ${azurerm_storage_account.databricks_storage.name}
       - Raw Data: abfss://raw@${azurerm_storage_account.databricks_storage.name}.dfs.core.windows.net/
       - Curated Data: abfss://curated@${azurerm_storage_account.databricks_storage.name}.dfs.core.windows.net/
       - SQL Warehouse: ${databricks_sql_endpoint.sql_wh.name}

    4. For Unity Catalog setup (requires account admin):
       - Service Principal Client ID: ${azuread_application.databricks_sp.client_id}
       - Service Principal Secret: [See service_principal_secret output]

    Ready to start building data pipelines!
  EOT
}
