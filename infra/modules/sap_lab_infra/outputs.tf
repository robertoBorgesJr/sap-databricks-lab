output "sap_lab_catalog_name" {
  description = "Nome do catálogo principal do lakehouse"
  value       = databricks_catalog.sap_lab.name
}

output "sap_bdc_sim_catalog_name" {
  description = "Nome do catálogo simulador do SAP BDC"
  value       = databricks_catalog.sap_bdc_sim.name
}

output "external_location_url" {
  description = "URL da External Location que cobre o container do lab"
  value       = databricks_external_location.sap_lab.url
}

output "sap_raw_volume_path" {
  description = "Caminho UC do volume de landing (usado pelo gerador e pelo Auto Loader)"
  value       = "/Volumes/${databricks_catalog.sap_lab.name}/landing/sap_raw"
}

output "checkpoints_volume_path" {
  description = "Caminho UC do volume de checkpoints (usado pelo Auto Loader)"
  value       = "/Volumes/${databricks_catalog.sap_lab.name}/ops/checkpoints"
}
