# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## BDC Simulator — Data Products
# MAGIC
# MAGIC Popula o catálogo `sap_bdc_sim_<env>.data_products` com dados curados,
# MAGIC prontos para serem compartilhados via Delta Sharing.
# MAGIC
# MAGIC No cenário real, o SAP Business Data Cloud é o provedor: ele publica
# MAGIC data products com nomes de negócio, sem campos técnicos SAP, e os expõe
# MAGIC via Delta Sharing para o Databricks consumir. Aqui, este notebook faz
# MAGIC o papel do BDC — lê da gold do lakehouse e escreve no catálogo simulado.
# MAGIC
# MAGIC ### Data products publicados
# MAGIC | Tabela | Fonte | Chave |
# MAGIC |--------|-------|-------|
# MAGIC | `sales_order` | gold.fact_sales_order_item + dim_customer + dim_material | order_id + item_number |
# MAGIC | `customer_master` | gold.dim_customer | customer_id |
# MAGIC
# MAGIC ### O que NÃO aparece nos data products (versus a gold)
# MAGIC - `mandt`: campo técnico SAP, sem significado fora do sistema de origem.
# MAGIC - `_op`, `_seq`, `_gold_updated_at`: metadados de pipeline, não de negócio.
# MAGIC - `is_deleted`, `deleted_at`: controle CDC — o BDC entrega apenas o estado atual.

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catálogo do lakehouse (fonte)")
dbutils.widgets.text("bdc_catalog", "sap_bdc_sim_dev", "Catálogo BDC simulado (destino)")
catalog = dbutils.widgets.get("catalog").strip()
bdc_catalog = dbutils.widgets.get("bdc_catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
  CREATE SCHEMA IF NOT EXISTS {bdc_catalog}.data_products
  COMMENT 'Data products curados — equivalente ao que o SAP BDC publicaria via Delta Sharing.'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Data product: `sales_order`

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bdc_catalog}.data_products.sales_order (
  order_id        STRING        NOT NULL  COMMENT 'SAP VBELN — número da ordem de venda.',
  item_number     STRING        NOT NULL  COMMENT 'SAP POSNR — número do item da ordem.',
  order_date      DATE                    COMMENT 'SAP AUDAT — data de criação da ordem.',
  customer_id     STRING                  COMMENT 'SAP KUNNR — número do cliente.',
  customer_name   STRING                  COMMENT 'SAP NAME1 — razão social do cliente.',
  material_id     STRING                  COMMENT 'SAP MATNR — número do material.',
  material_desc   STRING                  COMMENT 'SAP MAKTX (SPRAS=P) — descrição do material em português.',
  quantity        DECIMAL(18,3)           COMMENT 'SAP MENGE — quantidade do item.',
  uom             STRING                  COMMENT 'SAP MEINS — unidade de medida.',
  net_value       DECIMAL(18,2)           COMMENT 'SAP NETWR — valor líquido na moeda original.',
  currency        STRING                  COMMENT 'SAP WAERK — moeda (BRL, USD, EUR).',
  net_value_brl   DECIMAL(18,2)           COMMENT 'Valor convertido para BRL via TCURR (KURST=M). NULL se taxa ausente.',
  sales_org       STRING                  COMMENT 'SAP VKORG — organização de vendas.',
  published_at    TIMESTAMP     NOT NULL  COMMENT 'Timestamp da última publicação neste data product.'
)
USING DELTA
COMMENT 'sales_order — Ordens de venda curadas para Delta Sharing. Fonte: {catalog}.gold.fact_sales_order_item + dim_customer + dim_material.'
""")

# COMMAND ----------

facts = spark.table(f"{catalog}.gold.fact_sales_order_item")

customers = (
    spark.table(f"{catalog}.gold.dim_customer")
    .select(F.col("customer_id").alias("dc_customer_id"), "customer_name")
)

materials = (
    spark.table(f"{catalog}.gold.dim_material")
    .select(F.col("material_id").alias("dm_material_id"), "material_description")
)

sales_dp = (
    facts
    .join(customers, facts.customer_id == customers.dc_customer_id, "left")
    .join(materials, facts.material_id == materials.dm_material_id, "left")
    .withColumn("order_date",
        F.when(F.col("order_date_id").isNotNull(),
            F.to_date(F.col("order_date_id").cast("string"), "yyyyMMdd")
        ).otherwise(F.lit(None).cast("date"))
    )
    .withColumn("published_at", F.current_timestamp())
    .select(
        F.col("vbeln").alias("order_id"),
        F.col("posnr").alias("item_number"),
        "order_date",
        "customer_id",
        "customer_name",
        "material_id",
        F.col("material_description").alias("material_desc"),
        "quantity",
        "uom",
        "net_value",
        "currency",
        "net_value_brl",
        F.col("vkorg").alias("sales_org"),
        "published_at",
    )
)

# COMMAND ----------

target_so = DeltaTable.forName(spark, f"{bdc_catalog}.data_products.sales_order")

(
    target_so.alias("tgt")
    .merge(
        sales_dp.alias("src"),
        "tgt.order_id = src.order_id AND tgt.item_number = src.item_number"
    )
    .whenMatchedUpdate(set={c: f"src.{c}" for c in sales_dp.columns})
    .whenNotMatchedInsert(values={c: f"src.{c}" for c in sales_dp.columns})
    .execute()
)

print(f"MERGE concluído → {bdc_catalog}.data_products.sales_order")
spark.table(f"{bdc_catalog}.data_products.sales_order").select(
    F.count("*").alias("total_items"),
    F.countDistinct("order_id").alias("distinct_orders"),
    F.sum("net_value_brl").alias("total_brl"),
).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Data product: `customer_master`

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bdc_catalog}.data_products.customer_master (
  customer_id      STRING    NOT NULL  COMMENT 'SAP KUNNR — número do cliente.',
  customer_name    STRING              COMMENT 'SAP NAME1 — razão social.',
  country          STRING              COMMENT 'SAP LAND1 — país (BR).',
  city             STRING              COMMENT 'SAP ORT01 — cidade.',
  postal_code      STRING              COMMENT 'SAP PSTLZ — CEP.',
  tax_id_cnpj      STRING              COMMENT 'SAP STCD1 — CNPJ (14 dígitos, fictício).',
  customer_status  STRING    NOT NULL  COMMENT 'ACTIVE | MARKED_FOR_DELETION.',
  published_at     TIMESTAMP NOT NULL  COMMENT 'Timestamp da última publicação neste data product.'
)
USING DELTA
COMMENT 'customer_master — Mestre de clientes curado para Delta Sharing. Fonte: {catalog}.gold.dim_customer.'
""")

# COMMAND ----------

customer_dp = (
    spark.table(f"{catalog}.gold.dim_customer")
    .withColumn("published_at", F.current_timestamp())
    .select(
        "customer_id",
        "customer_name",
        "country",
        "city",
        "postal_code",
        "tax_id_cnpj",
        "customer_status",
        "published_at",
    )
)

# COMMAND ----------

target_cm = DeltaTable.forName(spark, f"{bdc_catalog}.data_products.customer_master")

(
    target_cm.alias("tgt")
    .merge(customer_dp.alias("src"), "tgt.customer_id = src.customer_id")
    .whenMatchedUpdate(set={c: f"src.{c}" for c in customer_dp.columns})
    .whenNotMatchedInsert(values={c: f"src.{c}" for c in customer_dp.columns})
    .execute()
)

print(f"MERGE concluído → {bdc_catalog}.data_products.customer_master")
spark.table(f"{bdc_catalog}.data_products.customer_master").groupBy("customer_status").count().show()
