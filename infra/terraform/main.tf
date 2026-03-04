data "azurerm_resource_group" "this" {
  name = var.resource_group_name
}

locals {
  location = coalesce(
    var.location,
    data.azurerm_resource_group.this.location,
  )
  databricks_workspace_id = trimspace(var.databricks_workspace_id)
}

data "azurerm_storage_account" "existing" {
  count               = var.create_storage_resources ? 0 : 1
  name                = var.storage_account_name
  resource_group_name = data.azurerm_resource_group.this.name
}

resource "azurerm_storage_account" "this" {
  count                    = var.create_storage_resources ? 1 : 0
  name                     = var.storage_account_name
  resource_group_name      = data.azurerm_resource_group.this.name
  location                 = local.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"
}

data "azurerm_storage_container" "existing" {
  count                = var.create_storage_resources ? 0 : 1
  name                 = var.storage_container_name
  storage_account_name = data.azurerm_storage_account.existing[0].name
}

resource "azurerm_storage_container" "this" {
  count                 = var.create_storage_resources ? 1 : 0
  name                  = var.storage_container_name
  storage_account_name  = azurerm_storage_account.this[0].name
  container_access_type = "private"
}

locals {
  storage_account_id = var.create_storage_resources ? azurerm_storage_account.this[0].id : data.azurerm_storage_account.existing[0].id
  storage_account_name = var.create_storage_resources ? azurerm_storage_account.this[0].name : data.azurerm_storage_account.existing[0].name
  storage_container_name = var.create_storage_resources ? azurerm_storage_container.this[0].name : data.azurerm_storage_container.existing[0].name
}

resource "azurerm_log_analytics_workspace" "this" {
  name                = var.log_analytics_workspace_name
  resource_group_name = data.azurerm_resource_group.this.name
  location            = local.location
  sku                 = "PerGB2018"
  retention_in_days   = var.log_analytics_retention_days
}

resource "azurerm_monitor_action_group" "this" {
  name                = var.monitor_action_group_name
  resource_group_name = data.azurerm_resource_group.this.name
  short_name          = substr(replace(var.monitor_action_group_name, "-", ""), 0, 12)

  dynamic "email_receiver" {
    for_each = trimspace(var.alert_email_receiver) == "" ? [] : [
      trimspace(var.alert_email_receiver)
    ]
    content {
      name          = "ops-email"
      email_address = email_receiver.value
    }
  }
}

resource "azurerm_monitor_diagnostic_setting" "storage" {
  count                      = var.enable_storage_diagnostics ? 1 : 0
  name                       = "diag-storage-openweather"
  target_resource_id         = local.storage_account_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id

  enabled_log {
    category_group = "allLogs"
  }

  metric {
    category = "AllMetrics"
  }
}

resource "azurerm_monitor_diagnostic_setting" "databricks" {
  count = (
    var.enable_databricks_diagnostics && local.databricks_workspace_id != "" ? 1 : 0
  )
  name                       = "diag-databricks-openweather"
  target_resource_id         = local.databricks_workspace_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id

  enabled_log {
    category_group = "allLogs"
  }

  metric {
    category = "AllMetrics"
  }
}

resource "azurerm_monitor_metric_alert" "storage_availability" {
  name                = "alert-storage-availability-openweather"
  resource_group_name = data.azurerm_resource_group.this.name
  scopes              = [local.storage_account_id]
  description         = "Storage availability below target for openweather pipeline."
  severity            = 2
  frequency           = "PT5M"
  window_size         = "PT5M"
  enabled             = true

  criteria {
    metric_namespace = "Microsoft.Storage/storageAccounts"
    metric_name      = "Availability"
    aggregation      = "Average"
    operator         = "LessThan"
    threshold        = 99
  }

  action {
    action_group_id = azurerm_monitor_action_group.this.id
  }
}

resource "azurerm_monitor_metric_alert" "storage_egress" {
  name                = "alert-storage-egress-openweather"
  resource_group_name = data.azurerm_resource_group.this.name
  scopes              = [local.storage_account_id]
  description         = "Unexpected storage egress for openweather pipeline."
  severity            = 3
  frequency           = "PT5M"
  window_size         = "PT5M"
  enabled             = true

  criteria {
    metric_namespace = "Microsoft.Storage/storageAccounts"
    metric_name      = "Egress"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = var.storage_egress_threshold_bytes
  }

  action {
    action_group_id = azurerm_monitor_action_group.this.id
  }
}
