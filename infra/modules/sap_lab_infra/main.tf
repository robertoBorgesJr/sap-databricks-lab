# Access Connector compartilhado com os demais domínios do workspace.
# Criado centralmente em rg-datamesh-governance-prod; não gerenciado aqui.
data "azurerm_databricks_access_connector" "uc_connector" {
  name                = "ext-access-connector-uc"
  resource_group_name = "rg-datamesh-governance-prod"
}

# Storage Account isolado para o lab (um por ambiente).
# Nomenclatura segue o padrão dos domínios do datamesh: st<nome><env>.
resource "azurerm_storage_account" "sap_lab" {
  name                     = "stsaplab${var.environment}"
  resource_group_name      = "rg-datamesh-governance-prod"
  location                 = "eastus2"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
}

# Container ADLS Gen2 único. Os dois catálogos usam sub-paths distintos
# dentro deste container (sap_lab/ e sap_bdc_sim/), evitando a criação
# de dois containers ou duas storage accounts para o mesmo ambiente.
resource "azurerm_storage_data_lake_gen2_filesystem" "sap_lab" {
  name               = "sap-lab-data"
  storage_account_id = azurerm_storage_account.sap_lab.id
}

# Permissão ao Access Connector para leitura/escrita no storage.
# Mesmo papel usado pelos domínios existentes: Storage Blob Data Contributor.
resource "azurerm_role_assignment" "sap_lab_contributor" {
  scope                = azurerm_storage_account.sap_lab.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_databricks_access_connector.uc_connector.identity[0].principal_id
}

locals {
  container_url = "abfss://${azurerm_storage_data_lake_gen2_filesystem.sap_lab.name}@${azurerm_storage_account.sap_lab.name}.dfs.core.windows.net"

  # Em prod, omite o sufixo de ambiente para seguir o padrão dos domínios existentes.
  # Em dev (e ambientes futuros), inclui o sufixo.
  external_loc_name = var.environment == "prod" ? "sap_lab_storage_location" : "sap_lab_${var.environment}_storage_location"
}

# Storage credential própria do lab, usando o access connector que já tem
# Storage Blob Data Contributor no storage account.
# Databricks serverless compute exige access connector (managed identity) —
# storage credentials de service principal não funcionam em serverless.
resource "databricks_storage_credential" "sap_lab" {
  name    = "sap_lab_cred_${var.environment}"
  comment = "Credential do SAP lab via access connector — suportada em serverless"

  azure_managed_identity {
    access_connector_id = data.azurerm_databricks_access_connector.uc_connector.id
  }

  force_destroy = true
  depends_on    = [azurerm_role_assignment.sap_lab_contributor]
}

# External Location cobre o container inteiro; os catálogos declaram
# storage_root com sub-paths internos a esta location.
resource "databricks_external_location" "sap_lab" {
  name            = local.external_loc_name
  url             = "${local.container_url}/"
  credential_name = databricks_storage_credential.sap_lab.name
  force_destroy   = true

  depends_on = [databricks_storage_credential.sap_lab]
}

# --- Catálogos ---------------------------------------------------------------

# Catálogo principal do lakehouse (bronze → silver → gold).
resource "databricks_catalog" "sap_lab" {
  name          = "sap_lab_${var.environment}"
  storage_root  = "${local.container_url}/sap_lab/"
  comment       = "Lakehouse SAP lab - ambiente ${var.environment}"
  force_destroy = true

  depends_on = [databricks_external_location.sap_lab]
}

# Catálogo que simula o provedor SAP BDC.
# Publica data products curados consumidos via Delta Sharing (Parte 4).
resource "databricks_catalog" "sap_bdc_sim" {
  name          = "sap_bdc_sim_${var.environment}"
  storage_root  = "${local.container_url}/sap_bdc_sim/"
  comment       = "Simulador do provedor SAP BDC - ambiente ${var.environment}"
  force_destroy = true

  depends_on = [databricks_external_location.sap_lab]
}

# --- Schemas de sap_lab_<env> ------------------------------------------------

resource "databricks_schema" "landing" {
  catalog_name = databricks_catalog.sap_lab.name
  name         = "landing"
  comment      = "Volume sap_raw com os arquivos do gerador: <TABELA>/batch=<N>/part-*.parquet"
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.sap_lab.name
  name         = "bronze"
  comment      = "Dado bruto append-only, tudo string, ingerido pelo Auto Loader"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.sap_lab.name
  name         = "silver"
  comment      = "Dado tipado, deduplicado, estado atual por chave"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.sap_lab.name
  name         = "gold"
  comment      = "Modelo dimensional: dimensões e fatos com nomes de negócio em inglês"
}

resource "databricks_schema" "ops" {
  catalog_name = databricks_catalog.sap_lab.name
  name         = "ops"
  comment      = "Checkpoints e schemaLocation do Auto Loader, tabelas de auditoria e qualidade"
}

# --- Schema de sap_bdc_sim_<env> ---------------------------------------------

resource "databricks_schema" "data_products" {
  catalog_name = databricks_catalog.sap_bdc_sim.name
  name         = "data_products"
  comment      = "Data products curados que simulam a publicação SAP BDC via Delta Sharing"
}

# --- Volumes -----------------------------------------------------------------

# Landing zone: destino dos arquivos gerados pelo simulador de CDC.
# Caminho resultante: /Volumes/sap_lab_<env>/landing/sap_raw/
resource "databricks_volume" "sap_raw" {
  catalog_name = databricks_catalog.sap_lab.name
  schema_name  = databricks_schema.landing.name
  name         = "sap_raw"
  volume_type  = "MANAGED"
  comment      = "Arquivos do gerador SAP: <TABELA>/batch=<N>/part-*.parquet"
}

# Checkpoints do Auto Loader e schemas inferidos por tabela.
# Caminho resultante: /Volumes/sap_lab_<env>/ops/checkpoints/
resource "databricks_volume" "checkpoints" {
  catalog_name = databricks_catalog.sap_lab.name
  schema_name  = databricks_schema.ops.name
  name         = "checkpoints"
  volume_type  = "MANAGED"
  comment      = "Checkpoints e schemaLocation do Auto Loader, um sub-diretório por tabela"
}

# --- Grants ------------------------------------------------------------------
# A Files API (CLI) tem bypass para workspace admins, mas o data plane
# (FUSE, Spark, Auto Loader) exige READ VOLUME explícito em todos os ambientes.
# Conceder a "account users" cobre conta user, conta admin e identidade dos jobs.

resource "databricks_grants" "catalog_sap_lab" {
  catalog = databricks_catalog.sap_lab.name
  grant {
    principal  = "account users"
    privileges = ["USE CATALOG"]
  }
}

resource "databricks_grants" "schema_landing" {
  schema = "${databricks_catalog.sap_lab.name}.${databricks_schema.landing.name}"
  grant {
    principal  = "account users"
    privileges = ["USE SCHEMA"]
  }
}

resource "databricks_grants" "schema_bronze" {
  schema = "${databricks_catalog.sap_lab.name}.${databricks_schema.bronze.name}"
  grant {
    principal  = "account users"
    privileges = ["USE SCHEMA", "CREATE TABLE", "MODIFY", "SELECT"]
  }
}

resource "databricks_grants" "schema_silver" {
  schema = "${databricks_catalog.sap_lab.name}.${databricks_schema.silver.name}"
  grant {
    principal  = "account users"
    privileges = ["USE SCHEMA", "CREATE TABLE", "MODIFY", "SELECT"]
  }
}

resource "databricks_grants" "schema_gold" {
  schema = "${databricks_catalog.sap_lab.name}.${databricks_schema.gold.name}"
  grant {
    principal  = "account users"
    privileges = ["USE SCHEMA", "CREATE TABLE", "MODIFY", "SELECT"]
  }
}

resource "databricks_grants" "schema_ops" {
  schema = "${databricks_catalog.sap_lab.name}.${databricks_schema.ops.name}"
  grant {
    principal  = "account users"
    privileges = ["USE SCHEMA"]
  }
}

resource "databricks_grants" "volume_sap_raw" {
  volume = "${databricks_catalog.sap_lab.name}.${databricks_schema.landing.name}.${databricks_volume.sap_raw.name}"
  grant {
    principal  = "account users"
    privileges = ["READ VOLUME", "WRITE VOLUME"]
  }
}

resource "databricks_grants" "volume_checkpoints" {
  volume = "${databricks_catalog.sap_lab.name}.${databricks_schema.ops.name}.${databricks_volume.checkpoints.name}"
  grant {
    principal  = "account users"
    privileges = ["READ VOLUME", "WRITE VOLUME"]
  }
}

resource "databricks_grants" "catalog_sap_bdc_sim" {
  catalog = databricks_catalog.sap_bdc_sim.name
  grant {
    principal  = "account users"
    privileges = ["USE CATALOG"]
  }
}

resource "databricks_grants" "schema_data_products" {
  schema = "${databricks_catalog.sap_bdc_sim.name}.${databricks_schema.data_products.name}"
  grant {
    principal  = "account users"
    privileges = ["USE SCHEMA", "CREATE TABLE", "MODIFY", "SELECT"]
  }
}
