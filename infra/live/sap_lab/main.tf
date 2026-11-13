module "sap_lab_dev" {
  source      = "../../modules/sap_lab_infra"
  environment = "dev"
}

module "sap_lab_prod" {
  source      = "../../modules/sap_lab_infra"
  environment = "prod"
}

output "dev_sap_raw_volume_path" {
  value = module.sap_lab_dev.sap_raw_volume_path
}

output "prod_sap_raw_volume_path" {
  value = module.sap_lab_prod.sap_raw_volume_path
}
