variable "environment" {
  type        = string
  description = "Ambiente: 'dev' ou 'prod'"
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment deve ser 'dev' ou 'prod'."
  }
}
