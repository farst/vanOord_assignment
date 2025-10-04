# databricks_setup

<!-- BEGINNING OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.0 |
| <a name="requirement_azuread"></a> [azuread](#requirement\_azuread) | ~> 3.0 |
| <a name="requirement_azurerm"></a> [azurerm](#requirement\_azurerm) | ~> 4.0 |
| <a name="requirement_databricks"></a> [databricks](#requirement\_databricks) | ~> 1.0 |
| <a name="requirement_random"></a> [random](#requirement\_random) | ~> 3.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azuread"></a> [azuread](#provider\_azuread) | 3.6.0 |
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | 4.47.0 |
| <a name="provider_databricks"></a> [databricks](#provider\_databricks) | 1.91.0 |
| <a name="provider_random"></a> [random](#provider\_random) | 3.7.2 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [azuread_application.databricks_sp](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/application) | resource |
| [azuread_service_principal.databricks_sp](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/service_principal) | resource |
| [azuread_service_principal_password.databricks_sp](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/service_principal_password) | resource |
| [azurerm_databricks_access_connector.unity_catalog](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/databricks_access_connector) | resource |
| [azurerm_databricks_workspace.this](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/databricks_workspace) | resource |
| [azurerm_resource_group.rg](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |
| [azurerm_role_assignment.access_connector_storage](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.databricks_admin](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.mi_storage_blob_contributor](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.rg_admin](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.sp_storage_blob_contributor](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_storage_account.databricks_storage](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_account) | resource |
| [azurerm_storage_container.curated](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_container) | resource |
| [azurerm_storage_container.metastore](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_container) | resource |
| [azurerm_storage_container.raw](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_container) | resource |
| [azurerm_user_assigned_identity.databricks_identity](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/user_assigned_identity) | resource |
| [databricks_external_location.curated_data](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/external_location) | resource |
| [databricks_external_location.raw_data](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/external_location) | resource |
| [databricks_group_member.service_principal_admin](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/group_member) | resource |
| [databricks_service_principal.databricks_sp](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/service_principal) | resource |
| [databricks_sql_endpoint.sql_wh](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/sql_endpoint) | resource |
| [databricks_storage_credential.external](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/storage_credential) | resource |
| [random_integer.suffix](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/integer) | resource |
| [azurerm_client_config.current](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/data-sources/client_config) | data source |
| [databricks_group.admins](https://registry.terraform.io/providers/databricks/databricks/latest/docs/data-sources/group) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_enable_serverless_compute"></a> [enable\_serverless\_compute](#input\_enable\_serverless\_compute) | Enable serverless compute for SQL warehouse | `bool` | `true` | no |
| <a name="input_environment"></a> [environment](#input\_environment) | Environment name (dev, staging, prod) | `string` | `"demo"` | no |
| <a name="input_location"></a> [location](#input\_location) | Azure region for resources | `string` | `"westeurope"` | no |
| <a name="input_project_name"></a> [project\_name](#input\_project\_name) | Name of the project | `string` | `"van-oord-databricks"` | no |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | Name of the resource group | `string` | `""` | no |
| <a name="input_sql_warehouse_auto_stop_mins"></a> [sql\_warehouse\_auto\_stop\_mins](#input\_sql\_warehouse\_auto\_stop\_mins) | Auto-stop timeout in minutes for SQL warehouse | `number` | `25` | no |
| <a name="input_sql_warehouse_cluster_size"></a> [sql\_warehouse\_cluster\_size](#input\_sql\_warehouse\_cluster\_size) | SQL warehouse cluster size | `string` | `"2X-Small"` | no |
| <a name="input_sql_warehouse_max_clusters"></a> [sql\_warehouse\_max\_clusters](#input\_sql\_warehouse\_max\_clusters) | Maximum number of clusters for SQL warehouse | `number` | `1` | no |
| <a name="input_sql_warehouse_min_clusters"></a> [sql\_warehouse\_min\_clusters](#input\_sql\_warehouse\_min\_clusters) | Minimum number of clusters for SQL warehouse | `number` | `1` | no |
| <a name="input_sql_warehouse_name"></a> [sql\_warehouse\_name](#input\_sql\_warehouse\_name) | Name of the SQL warehouse | `string` | `"main-sql-warehouse"` | no |
| <a name="input_storage_account_name"></a> [storage\_account\_name](#input\_storage\_account\_name) | Name of the storage account (will have random suffix if empty) | `string` | `""` | no |
| <a name="input_storage_account_tier"></a> [storage\_account\_tier](#input\_storage\_account\_tier) | Storage account tier | `string` | `"Standard"` | no |
| <a name="input_storage_replication_type"></a> [storage\_replication\_type](#input\_storage\_replication\_type) | Storage account replication type | `string` | `"LRS"` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | Tags to apply to all resources | `map(string)` | <pre>{<br/>  "Environment": "Demo",<br/>  "ManagedBy": "Terraform",<br/>  "Owner": "Data Platform Team",<br/>  "Project": "Van Oord Databricks Assignment"<br/>}</pre> | no |
| <a name="input_workspace_name"></a> [workspace\_name](#input\_workspace\_name) | Name of the Databricks workspace | `string` | `""` | no |
| <a name="input_workspace_sku"></a> [workspace\_sku](#input\_workspace\_sku) | Databricks workspace SKU | `string` | `"premium"` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_access_instructions"></a> [access\_instructions](#output\_access\_instructions) | Instructions for accessing the workspace |
| <a name="output_connection_info"></a> [connection\_info](#output\_connection\_info) | Connection information for Databricks notebooks |
| <a name="output_curated_container_url"></a> [curated\_container\_url](#output\_curated\_container\_url) | URL for curated data container |
| <a name="output_managed_identity_client_id"></a> [managed\_identity\_client\_id](#output\_managed\_identity\_client\_id) | Client ID of the managed identity |
| <a name="output_managed_identity_principal_id"></a> [managed\_identity\_principal\_id](#output\_managed\_identity\_principal\_id) | Principal ID of the managed identity |
| <a name="output_raw_container_url"></a> [raw\_container\_url](#output\_raw\_container\_url) | URL for raw data container |
| <a name="output_service_principal_client_id"></a> [service\_principal\_client\_id](#output\_service\_principal\_client\_id) | Client ID of the service principal |
| <a name="output_service_principal_object_id"></a> [service\_principal\_object\_id](#output\_service\_principal\_object\_id) | Object ID of the service principal |
| <a name="output_service_principal_secret"></a> [service\_principal\_secret](#output\_service\_principal\_secret) | Client secret of the service principal |
| <a name="output_sql_warehouse_cluster_size"></a> [sql\_warehouse\_cluster\_size](#output\_sql\_warehouse\_cluster\_size) | Current cluster size of the SQL warehouse |
| <a name="output_sql_warehouse_id"></a> [sql\_warehouse\_id](#output\_sql\_warehouse\_id) | ID of the SQL warehouse |
| <a name="output_sql_warehouse_name"></a> [sql\_warehouse\_name](#output\_sql\_warehouse\_name) | Name of the SQL warehouse |
| <a name="output_storage_account_name"></a> [storage\_account\_name](#output\_storage\_account\_name) | Name of the storage account |
| <a name="output_storage_account_url"></a> [storage\_account\_url](#output\_storage\_account\_url) | URL of the storage account |
| <a name="output_workspace_id"></a> [workspace\_id](#output\_workspace\_id) | Databricks workspace ID |
| <a name="output_workspace_ready_message"></a> [workspace\_ready\_message](#output\_workspace\_ready\_message) | Message indicating workspace is ready |
| <a name="output_workspace_url"></a> [workspace\_url](#output\_workspace\_url) | Databricks workspace URL |
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
