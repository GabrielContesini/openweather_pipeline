variable "resource_group_name" {
  description = "Existing Azure Resource Group for the pipeline."
  type        = string
  default     = "openweather_pipeline"
}

variable "location" {
  description = "Azure region for new resources."
  type        = string
  default     = "eastus"
}

variable "storage_account_name" {
  description = "Storage account name used by the pipeline."
  type        = string
  default     = "tropowxdlprod"
}

variable "storage_container_name" {
  description = "Container used by pipeline for raw/bronze/silver/gold."
  type        = string
  default     = "openweather-data"
}

variable "create_storage_resources" {
  description = "When true, creates storage account/container; when false, uses existing."
  type        = bool
  default     = false
}

variable "log_analytics_workspace_name" {
  description = "Log Analytics workspace name for centralized observability."
  type        = string
  default     = "law-openweather-prd"
}

variable "log_analytics_retention_days" {
  description = "Retention period for Log Analytics workspace."
  type        = number
  default     = 30
}

variable "monitor_action_group_name" {
  description = "Action group name for alerts."
  type        = string
  default     = "ag-openweather-prd"
}

variable "alert_email_receiver" {
  description = "Email to receive alerts. Leave empty to skip email receiver."
  type        = string
  default     = ""
}

variable "enable_storage_diagnostics" {
  description = "Enable storage diagnostic settings to Log Analytics."
  type        = bool
  default     = true
}

variable "enable_databricks_diagnostics" {
  description = "Enable Databricks workspace diagnostic settings to Log Analytics."
  type        = bool
  default     = false
}

variable "databricks_workspace_id" {
  description = "Existing Databricks workspace resource ID for diagnostics."
  type        = string
  default     = ""
}

variable "storage_egress_threshold_bytes" {
  description = "Threshold for Egress alert in bytes (Total aggregation over 5 min)."
  type        = number
  default     = 1073741824
}
