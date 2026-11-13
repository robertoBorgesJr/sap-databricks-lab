# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold — dim_customer (Dimensão de Cliente)
# MAGIC
# MAGIC Fonte: `silver.kna1`.
# MAGIC
# MAGIC ### Decisões de modelagem
# MAGIC - Clientes excluídos fisicamente (`is_deleted=true`) são **excluídos** da dim.
# MAGIC - Clientes marcados para exclusão logicamente (`is_logically_deleted=true`) são
# MAGIC   **incluídos** com `customer_status='MARKED_FOR_DELETION'` — histórico de pedidos
# MAGIC   pode referenciar este cliente.
# MAGIC - Nomes de coluna em inglês com comentário UC indicando campo SAP de origem.

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_customer (
  customer_id      STRING  NOT NULL  COMMENT 'SAP KUNNR — chave do cliente com zeros à esquerda (para JOIN com fatos).',
  customer_name    STRING            COMMENT 'SAP NAME1 — razão social.',
  customer_name2   STRING            COMMENT 'SAP NAME2 — complemento de nome.',
  country          STRING            COMMENT 'SAP LAND1 — país (BR).',
  city             STRING            COMMENT 'SAP ORT01 — cidade.',
  postal_code      STRING            COMMENT 'SAP PSTLZ — CEP.',
  street           STRING            COMMENT 'SAP STRAS — logradouro.',
  tax_id_cnpj      STRING            COMMENT 'SAP STCD1 — CNPJ (14 dígitos).',
  tax_id_cpf       STRING            COMMENT 'SAP STCD2 — CPF (11 dígitos).',
  phone            STRING            COMMENT 'SAP TELF1 — telefone.',
  language         STRING            COMMENT 'SAP SPRAS — idioma (P=português).',
  account_group    STRING            COMMENT 'SAP KTOKD — grupo de contas.',
  created_date     DATE              COMMENT 'SAP ERDAT — data de criação do cadastro.',
  customer_status  STRING  NOT NULL  COMMENT 'ACTIVE | MARKED_FOR_DELETION. Derivado de LOEVM.',
  _gold_updated_at TIMESTAMP NOT NULL COMMENT 'Última vez que esta linha foi atualizada na gold.'
)
USING DELTA
COMMENT 'dim_customer — Dimensão de clientes. Fonte: {catalog}.silver.kna1'
""")

# COMMAND ----------

dim_df = (
    spark.table(f"{catalog}.silver.kna1")
    .filter(F.col("is_deleted") == False)   # exclui registros fisicamente deletados
    .withColumn("customer_status",
        F.when(F.col("is_logically_deleted"), F.lit("MARKED_FOR_DELETION"))
        .otherwise(F.lit("ACTIVE"))
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .select(
        F.col("kunnr").alias("customer_id"),
        F.col("name1").alias("customer_name"),
        F.col("name2").alias("customer_name2"),
        F.col("land1").alias("country"),
        F.col("ort01").alias("city"),
        F.col("pstlz").alias("postal_code"),
        F.col("stras").alias("street"),
        F.col("stcd1").alias("tax_id_cnpj"),
        F.col("stcd2").alias("tax_id_cpf"),
        F.col("telf1").alias("phone"),
        F.col("spras").alias("language"),
        F.col("ktokd").alias("account_group"),
        F.col("erdat").alias("created_date"),
        "customer_status",
        "_gold_updated_at",
    )
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.gold.dim_customer")

(
    target.alias("tgt")
    .merge(dim_df.alias("src"), "tgt.customer_id = src.customer_id")
    .whenMatchedUpdate(set={c: f"src.{c}" for c in dim_df.columns})
    .whenNotMatchedInsert(values={c: f"src.{c}" for c in dim_df.columns})
    .execute()
)

print(f"MERGE concluído → {catalog}.gold.dim_customer")
spark.table(f"{catalog}.gold.dim_customer").groupBy("customer_status").count().show()
