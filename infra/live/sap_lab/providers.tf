terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }
}

provider "azurerm" {
  features {}
  storage_use_azuread = true
}

# Autentica no workspace Databricks via recurso ARM.
# Em dev e prod, usa o usuário pessoal autenticado no az CLI.
# Em um pipeline de CI/CD real, substituir por service principal:
#   client_id     = var.sp_client_id
#   client_secret = var.sp_client_secret
#   tenant_id     = var.tenant_id
provider "databricks" {
  azure_workspace_resource_id = "/subscriptions/${var.azure_subscription_id}/resourceGroups/${var.databricks_rg_name}/providers/Microsoft.Databricks/workspaces/${var.databricks_workspace_name}"
}
