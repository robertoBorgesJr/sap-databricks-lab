# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — VBAK (Cabeçalho de Ordem de Venda)
# MAGIC
# MAGIC ### Conceitos SAP cobertos
# MAGIC | Campo | Armadilha | Tratamento |
# MAGIC |-------|-----------|------------|
# MAGIC | ERDAT/AUDAT | DATS '00000000'; AUDAT pode preceder ERDAT | → DATE ou NULL |
# MAGIC | NETWR | vazio; vírgula decimal; '0.00' (ordem migrada) | → DECIMAL(18,2) ou NULL |
# MAGIC | WAERK | trailing space ('BRL ') | trim |
# MAGIC | BSTNK | vazio (sem pedido do cliente) | vazio → NULL |
# MAGIC | VBELN | pode ser alfanumérico (ORDxxxx de migração) | trim |

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.vbak (
  mandt              STRING        NOT NULL  COMMENT 'SAP MANDT',
  vbeln              STRING        NOT NULL  COMMENT 'SAP VBELN — número da ordem, 10 chars ALPHA.',
  erdat              DATE                    COMMENT 'SAP ERDAT — data de criação. DATS 00000000 → NULL.',
  ernam              STRING                  COMMENT 'SAP ERNAM — usuário criador.',
  audat              DATE                    COMMENT 'SAP AUDAT — data da ordem. Pode preceder ERDAT por erro de entrada.',
  vkorg              STRING                  COMMENT 'SAP VKORG — organização de vendas.',
  vtweg              STRING                  COMMENT 'SAP VTWEG — canal de distribuição.',
  spart              STRING                  COMMENT 'SAP SPART — divisão.',
  kunnr              STRING                  COMMENT 'SAP KUNNR — FK para silver.kna1.',
  netwr              DECIMAL(18,2)           COMMENT 'SAP NETWR — valor líquido do cabeçalho. Vazio ou vírgula → NULL.',
  waerk              STRING                  COMMENT 'SAP WAERK — moeda (BRL, USD, EUR). Trailing space → trim.',
  vkbur              STRING                  COMMENT 'SAP VKBUR — escritório de vendas.',
  bstnk              STRING                  COMMENT 'SAP BSTNK — número do pedido do cliente. Vazio → NULL.',
  is_deleted         BOOLEAN       NOT NULL  COMMENT 'Exclusão física via CDC.',
  deleted_at         TIMESTAMP               COMMENT 'Timestamp da exclusão física.',
  _silver_updated_at TIMESTAMP     NOT NULL  COMMENT 'Última atualização pelo MERGE.'
)
USING DELTA
COMMENT 'VBAK — Cabeçalho de ordem de venda. Fonte: {catalog}.bronze.vbak'
""")

# COMMAND ----------

raw = spark.table(f"{catalog}.bronze.vbak")
filtered = raw.filter(F.trim(F.col("mandt")) == "100")

key_cols = ["mandt", "vbeln"]
win = Window.partitionBy(*key_cols).orderBy(
    F.coalesce(F.col("_seq").cast("long"), F.lit(0)).desc()
)
deduped = (
    filtered
    .withColumn("_rank", F.row_number().over(win))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

# COMMAND ----------

def _parse_dats(c: str) -> F.Column:
    t = F.trim(F.col(c))
    return F.when(t.isNull() | (t == "") | (t == "00000000"), F.lit(None)).otherwise(
        F.to_date(t, "yyyyMMdd")
    )

def _empty_null(c: str) -> F.Column:
    t = F.trim(F.col(c))
    return F.when(t == "", F.lit(None)).otherwise(t)

def _parse_decimal(c: str, prec: int = 18, scale: int = 2) -> F.Column:
    t = F.trim(F.regexp_replace(F.col(c), ",", "."))
    return F.when(
        t.isNull() | (t == "") | ~t.rlike(r"^-?\d+(\.\d+)?$"),
        F.lit(None),
    ).otherwise(t.cast(f"DECIMAL({prec},{scale})"))

transformed = (
    deduped
    .withColumn("vbeln",  F.trim(F.col("vbeln")))
    .withColumn("ernam",  _empty_null("ernam"))
    .withColumn("vkorg",  _empty_null("vkorg"))
    .withColumn("vtweg",  _empty_null("vtweg"))
    .withColumn("spart",  _empty_null("spart"))
    .withColumn("kunnr",  F.trim(F.col("kunnr")))
    .withColumn("waerk",  _empty_null("waerk"))
    .withColumn("vkbur",  _empty_null("vkbur"))
    .withColumn("bstnk",  _empty_null("bstnk"))
    # Datas
    .withColumn("erdat",  _parse_dats("erdat"))
    .withColumn("audat",  _parse_dats("audat"))
    # Valor monetário: vírgula decimal e vazio ocorrem no gerador
    .withColumn("netwr",  _parse_decimal("netwr", 18, 2))
    # CDC
    .withColumn("_op_clean", F.coalesce(F.trim(F.col("_op")), F.lit("I")))
    .withColumn("is_deleted", F.col("_op_clean") == "D")
    .withColumn("deleted_at",
        F.when(F.col("_op_clean") == "D", F.to_timestamp(F.col("_extracted_at")))
        .otherwise(F.lit(None).cast("timestamp"))
    )
    .withColumn("_silver_updated_at", F.current_timestamp())
    .select(
        "mandt", "vbeln", "erdat", "ernam", "audat", "vkorg", "vtweg", "spart",
        "kunnr", "netwr", "waerk", "vkbur", "bstnk",
        "is_deleted", "deleted_at", "_silver_updated_at",
    )
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.silver.vbak")

(
    target.alias("tgt")
    .merge(transformed.alias("src"), "tgt.mandt = src.mandt AND tgt.vbeln = src.vbeln")
    .whenMatchedUpdate(
        condition="src.is_deleted = true",
        set={"is_deleted": "src.is_deleted", "deleted_at": "src.deleted_at",
             "_silver_updated_at": "src._silver_updated_at"}
    )
    .whenMatchedUpdate(
        condition="src.is_deleted = false",
        set={c: f"src.{c}" for c in transformed.columns}
    )
    .whenNotMatchedInsert(
        condition="src.is_deleted = false",
        values={c: f"src.{c}" for c in transformed.columns}
    )
    .execute()
)

print(f"MERGE concluído → {catalog}.silver.vbak")
spark.table(f"{catalog}.silver.vbak").select(
    F.count("*").alias("total"),
    F.countDistinct("waerk").alias("currencies"),
    F.sum(F.col("is_deleted").cast("int")).alias("deleted"),
).show()
