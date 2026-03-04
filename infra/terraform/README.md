# Terraform - OpenWeather Enterprise Stack

This stack provisions observability and alerting foundations and can optionally manage storage resources.

## What it manages

1. Existing or new Storage Account + Container (toggle by `create_storage_resources`).
2. Log Analytics Workspace for centralized logs and metrics.
3. Action Group for alert notifications.
4. Diagnostic settings:
- Storage -> Log Analytics
- Databricks Workspace -> Log Analytics (when `databricks_workspace_id` is provided)
5. Metric alerts:
- Storage availability below target
- Storage egress above threshold

## Prerequisites

1. Terraform >= 1.6
2. Azure credentials configured in shell:
- `ARM_SUBSCRIPTION_ID`
- `ARM_TENANT_ID`
- `ARM_CLIENT_ID`
- `ARM_CLIENT_SECRET`

## Usage

```bash
cd infra/terraform
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

## Recommended first run

1. Copy `terraform.tfvars.example` to `terraform.tfvars`.
2. Keep `create_storage_resources = false` to avoid recreating existing storage.
3. Fill `databricks_workspace_id` to enable Databricks diagnostics.
4. Set a real email in `alert_email_receiver`.

## Notes

1. This stack is safe for existing environments when using data sources (`create_storage_resources=false`).
2. For production hardening, add:
- state backend (Azure Storage)
- resource locks
- tags policy
- CI plan/apply workflow with approvals
