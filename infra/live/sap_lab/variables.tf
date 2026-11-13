variable "azure_subscription_id" {
  type        = string
  description = "ID da subscription Azure onde o workspace Databricks está provisionado"
}

variable "databricks_rg_name" {
  type        = string
  description = "Resource group do workspace Databricks"
}

variable "databricks_workspace_name" {
  type        = string
  description = "Nome do workspace Databricks"
}
