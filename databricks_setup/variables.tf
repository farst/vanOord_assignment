# =============================================================================
# VARIABLES - Databricks Workspace Configuration
# =============================================================================

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "demo"
  validation {
    condition     = contains(["dev", "staging", "prod", "demo"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod, demo."
  }
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "van-oord-databricks"
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "westeurope"
  validation {
    condition = contains([
      "westeurope", "northeurope", "eastus", "westus2", "southeastasia"
    ], var.location)
    error_message = "Location must be a supported Azure region."
  }
}

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = ""
}

variable "workspace_name" {
  description = "Name of the Databricks workspace"
  type        = string
  default     = ""
}

variable "workspace_sku" {
  description = "Databricks workspace SKU"
  type        = string
  default     = "premium"
  validation {
    condition     = contains(["standard", "premium", "trial"], var.workspace_sku)
    error_message = "Workspace SKU must be one of: standard, premium, trial."
  }
}

variable "storage_account_name" {
  description = "Name of the storage account (will have random suffix if empty)"
  type        = string
  default     = ""
}

variable "storage_account_tier" {
  description = "Storage account tier"
  type        = string
  default     = "Standard"
  validation {
    condition     = contains(["Standard", "Premium"], var.storage_account_tier)
    error_message = "Storage account tier must be Standard or Premium."
  }
}

variable "storage_replication_type" {
  description = "Storage account replication type"
  type        = string
  default     = "LRS"
  validation {
    condition     = contains(["LRS", "GRS", "RAGRS", "ZRS"], var.storage_replication_type)
    error_message = "Storage replication type must be one of: LRS, GRS, RAGRS, ZRS."
  }
}

variable "sql_warehouse_name" {
  description = "Name of the SQL warehouse"
  type        = string
  default     = "main-sql-warehouse"
}

variable "sql_warehouse_cluster_size" {
  description = "SQL warehouse cluster size"
  type        = string
  default     = "2X-Small"
  validation {
    condition = contains([
      "2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"
    ], var.sql_warehouse_cluster_size)
    error_message = "SQL warehouse cluster size must be a valid size."
  }
}

variable "sql_warehouse_min_clusters" {
  description = "Minimum number of clusters for SQL warehouse"
  type        = number
  default     = 1
  validation {
    condition     = var.sql_warehouse_min_clusters >= 1 && var.sql_warehouse_min_clusters <= 10
    error_message = "Minimum clusters must be between 1 and 10."
  }
}

variable "sql_warehouse_max_clusters" {
  description = "Maximum number of clusters for SQL warehouse"
  type        = number
  default     = 1
  validation {
    condition     = var.sql_warehouse_max_clusters >= 1 && var.sql_warehouse_max_clusters <= 10
    error_message = "Maximum clusters must be between 1 and 10."
  }
}

variable "sql_warehouse_auto_stop_mins" {
  description = "Auto-stop timeout in minutes for SQL warehouse"
  type        = number
  default     = 25
  validation {
    condition     = var.sql_warehouse_auto_stop_mins >= 1 && var.sql_warehouse_auto_stop_mins <= 10080
    error_message = "Auto-stop minutes must be between 1 and 10080 (1 week)."
  }
}

variable "enable_serverless_compute" {
  description = "Enable serverless compute for SQL warehouse"
  type        = bool
  default     = true
}

variable "metastore_name" {
  description = "Name of the Unity Catalog metastore"
  type        = string
  default     = "main"
}

variable "external_locations" {
  description = "List of external locations to create"
  type = map(object({
    name        = string
    url         = string
    comment     = string
    permissions = map(list(string))
  }))
  default = {
    raw_data = {
      name    = "raw_data"
      url     = ""
      comment = "External location for raw data"
      permissions = {
        "finance_analysts" = ["READ FILES"]
        "data_engineers"   = ["READ FILES", "WRITE FILES"]
      }
    }
    curated_data = {
      name    = "curated_data"
      url     = ""
      comment = "External location for curated data"
      permissions = {
        "finance_analysts" = ["READ FILES"]
        "data_engineers"   = ["READ FILES", "WRITE FILES"]
      }
    }
  }
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "Van Oord Databricks Assignment"
    Environment = "Demo"
    ManagedBy   = "Terraform"
    Owner       = "Data Platform Team"
  }
}

variable "client_secret" {
  description = "Client secret for Service Principal authentication"
  type        = string
  sensitive   = true
  default     = null
}

variable "service_principal_app_id" {
  description = "Application ID of the Service Principal for Unity Catalog"
  type        = string
  default     = "326236ad-b8f2-4dd7-bc86-bc4d462398b1"
}
