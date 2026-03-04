output "resource_group_name" {
  description = "Resource group used by this stack."
  value       = data.azurerm_resource_group.this.name
}

output "storage_account_name" {
  description = "Storage account used by pipeline."
  value       = local.storage_account_name
}

output "storage_container_name" {
  description = "Storage container used by pipeline."
  value       = local.storage_container_name
}

output "log_analytics_workspace_id" {
  description = "Log Analytics workspace ID."
  value       = azurerm_log_analytics_workspace.this.id
}

output "monitor_action_group_id" {
  description = "Action group used by metric alerts."
  value       = azurerm_monitor_action_group.this.id
}
