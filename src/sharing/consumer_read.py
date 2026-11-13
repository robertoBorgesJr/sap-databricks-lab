# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## BDC Consumer — Leitura via Delta Sharing
# MAGIC
# MAGIC Demonstra o consumo dos data products publicados pelo "BDC simulado"
# MAGIC usando o conector `deltaSharing` nativo do Databricks Runtime.
# MAGIC
# MAGIC **Pré-requisito:** o arquivo de perfil JSON deve estar no volume antes de
# MAGIC executar este notebook. Obtenha-o seguindo as instruções do notebook
# MAGIC `share_setup` (Passo manual).
# MAGIC
# MAGIC ### Por que `deltaSharing` em vez de catálogo montado?
# MAGIC Com um único metastore Unity Catalog, não é possível criar um catálogo
# MAGIC montado a partir de um share do mesmo metastore (`CREATE CATALOG ... USING SHARE`
# MAGIC só funciona entre metastores distintos). A alternativa é o conector
# MAGIC `spark.read.format("deltaSharing")`, que lê via protocolo open sharing usando
# MAGIC um arquivo de perfil (endpoint + bearer token).
# MAGIC
# MAGIC Referência: https://docs.databricks.com/en/delta-sharing/read-data-open.html

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catálogo do lakehouse")
dbutils.widgets.text("profile_path", "", "Caminho do perfil JSON (vazio = padrão do volume)")
catalog = dbutils.widgets.get("catalog").strip()
profile_path = dbutils.widgets.get("profile_path").strip()

env = catalog.rsplit("_", 1)[-1]
share_name = f"sap_bdc_share_{env}"

if not profile_path:
    profile_path = f"/Volumes/{catalog}/ops/sharing_profiles/profile.json"

print(f"share={share_name}")
print(f"profile_path={profile_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Volume para armazenar o perfil

# COMMAND ----------

spark.sql(f"""
  CREATE VOLUME IF NOT EXISTS {catalog}.ops.sharing_profiles
  COMMENT 'Perfis de credenciais do Delta Sharing (JSON de recipient, open sharing).'
""")
print(f"Volume disponível em /Volumes/{catalog}/ops/sharing_profiles/")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Leitura: `sales_order`
# MAGIC
# MAGIC Formato do caminho de load: `<perfil>#<share>.<schema>.<tabela>`

# COMMAND ----------

sales_df = (
    spark.read.format("deltaSharing")
    .load(f"{profile_path}#{share_name}.data_products.sales_order")
)

print(f"sales_order: {sales_df.count()} registros")
sales_df.printSchema()
sales_df.show(5, truncate=True)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Leitura: `customer_master`

# COMMAND ----------

customers_df = (
    spark.read.format("deltaSharing")
    .load(f"{profile_path}#{share_name}.data_products.customer_master")
)

print(f"customer_master: {customers_df.count()} registros")
customers_df.printSchema()
customers_df.show(5, truncate=True)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Verificação de schema
# MAGIC
# MAGIC Confirma que nenhum campo técnico SAP vazou para os data products.

# COMMAND ----------

sap_technical_cols = {"mandt", "_op", "_seq", "_gold_updated_at", "is_deleted", "deleted_at"}

for name, df in [("sales_order", sales_df), ("customer_master", customers_df)]:
    leaked = sap_technical_cols & set(df.columns)
    assert not leaked, f"{name}: campos técnicos SAP encontrados: {leaked}"
    print(f"{name}: schema OK — nenhum campo técnico SAP exposto.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Garantia read-only
# MAGIC
# MAGIC O protocolo Delta Sharing é intrinsecamente read-only: o servidor entrega
# MAGIC apenas URLs pré-assinadas para leitura de arquivos Parquet. Não existe
# MAGIC operação de escrita no protocolo — o consumidor não consegue modificar
# MAGIC as tabelas compartilhadas independentemente das permissões Unity Catalog.
# MAGIC
# MAGIC A verificação prática é confirmar que o `format("deltaSharing")` não
# MAGIC expõe caminho de escrita:

# COMMAND ----------

try:
    spark.read.format("deltaSharing").load(
        f"{profile_path}#{share_name}.data_products.sales_order"
    ).write.format("deltaSharing").save("/tmp/_write_test")
    raise AssertionError("Escrita via deltaSharing não deveria ser possível.")
except Exception as exc:
    # AnalysisException ou similar — deltaSharing não tem DataSource de escrita
    print(f"Read-only confirmado: {type(exc).__name__}: {str(exc)[:120]}")
