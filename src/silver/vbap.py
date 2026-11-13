# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — VBAP (Itens de Ordem de Venda)
# MAGIC
# MAGIC ### Conceitos SAP cobertos
# MAGIC | Campo | Armadilha | Tratamento |
# MAGIC |-------|-----------|------------|
# MAGIC | MATNR | trailing spaces 18 chars; pode vir sem zero-padding (legado) | trim |
# MAGIC | MENGE | pode ser negativa (erro de entrada); vírgula decimal | → DECIMAL(18,3) |
# MAGIC | NETWR | vazio; vírgula decimal; '0.00' | → DECIMAL(18,2) ou NULL |
# MAGIC | WAERK | trailing space ('BRL ') | trim |
# MAGIC | LGORT/ABGRU | vazio quando não preenchido | vazio → NULL |
# MAGIC | POSNR | pode ser '000000' por erro de entrada | mantido como veio |

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.vbap (
  mandt              STRING        NOT NULL  COMMENT 'SAP MANDT',
  vbeln              STRING        NOT NULL  COMMENT 'SAP VBELN — FK para silver.vbak.',
  posnr              STRING        NOT NULL  COMMENT 'SAP POSNR — posição do item, 6 chars (000010, 000020...).',
  matnr              STRING                  COMMENT 'SAP MATNR — FK para silver.mara. Trailing spaces → trim.',
  menge              DECIMAL(18,3)           COMMENT 'SAP MENGE — quantidade. Pode ser negativa (erro de entrada).',
  meins              STRING                  COMMENT 'SAP MEINS — unidade de medida. Trailing space → trim.',
  netwr              DECIMAL(18,2)           COMMENT 'SAP NETWR — valor líquido do item. Vazio → NULL.',
  waerk              STRING                  COMMENT 'SAP WAERK — moeda, igual ao cabeçalho VBAK. Trailing space → trim.',
  werks              STRING                  COMMENT 'SAP WERKS — centro (plant).',
  lgort              STRING                  COMMENT 'SAP LGORT — depósito. Vazio → NULL.',
  abgru              STRING                  COMMENT 'SAP ABGRU — motivo de rejeição. Vazio → NULL.',
  is_deleted         BOOLEAN       NOT NULL  COMMENT 'Exclusão física via CDC.',
  deleted_at         TIMESTAMP               COMMENT 'Timestamp da exclusão física.',
  _silver_updated_at TIMESTAMP     NOT NULL  COMMENT 'Última atualização pelo MERGE.'
)
USING DELTA
COMMENT 'VBAP — Itens de ordem de venda. Fonte: {catalog}.bronze.vbap'
""")

# COMMAND ----------

raw = spark.table(f"{catalog}.bronze.vbap")
filtered = raw.filter(F.trim(F.col("mandt")) == "100")

key_cols = ["mandt", "vbeln", "posnr"]
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
    .withColumn("matnr",  F.trim(F.col("matnr")))
    .withColumn("meins",  _empty_null("meins"))
    .withColumn("waerk",  _empty_null("waerk"))
    .withColumn("werks",  _empty_null("werks"))
    .withColumn("lgort",  _empty_null("lgort"))
    .withColumn("abgru",  _empty_null("abgru"))
    # Quantidade: pode ser negativa (erro de entrada) — preservar sem forçar ABS
    .withColumn("menge",  _parse_decimal("menge", 18, 3))
    # Valor do item
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
        "mandt", "vbeln", "posnr", "matnr", "menge", "meins", "netwr", "waerk",
        "werks", "lgort", "abgru",
        "is_deleted", "deleted_at", "_silver_updated_at",
    )
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.silver.vbap")

(
    target.alias("tgt")
    .merge(
        transformed.alias("src"),
        "tgt.mandt = src.mandt AND tgt.vbeln = src.vbeln AND tgt.posnr = src.posnr"
    )
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

print(f"MERGE concluído → {catalog}.silver.vbap")
spark.table(f"{catalog}.silver.vbap").select(
    F.count("*").alias("total"),
    F.sum(F.col("is_deleted").cast("int")).alias("deleted"),
    F.sum("netwr").alias("sum_netwr"),
).show()
