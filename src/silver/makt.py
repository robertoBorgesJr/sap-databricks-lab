# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — MAKT (Descrições de Material)
# MAGIC
# MAGIC Chave composta: MANDT + MATNR + SPRAS.
# MAGIC O gerador produz uma linha em português ('P') para cada material e ~30% também
# MAGIC em inglês ('E'). A gold `dim_material` filtra SPRAS='P'.
# MAGIC
# MAGIC ### Conceitos SAP cobertos
# MAGIC | Campo | Armadilha | Tratamento |
# MAGIC |-------|-----------|------------|
# MAGIC | MAKTX | trailing spaces 40 chars; vazio | trim; vazio → NULL |
# MAGIC | MAKTX | pode vir em maiúsculas (registros antigos) | mantido como veio |

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.makt (
  mandt              STRING    NOT NULL  COMMENT 'SAP MANDT',
  matnr              STRING    NOT NULL  COMMENT 'SAP MATNR — FK para silver.mara.',
  spras              STRING    NOT NULL  COMMENT 'SAP SPRAS — idioma: P=português, E=inglês.',
  maktx              STRING              COMMENT 'SAP MAKTX — descrição do material, 40 chars. Vazio → NULL.',
  is_deleted         BOOLEAN   NOT NULL  COMMENT 'Exclusão física via CDC.',
  deleted_at         TIMESTAMP           COMMENT 'Timestamp da exclusão física.',
  _silver_updated_at TIMESTAMP NOT NULL  COMMENT 'Última atualização pelo MERGE.'
)
USING DELTA
COMMENT 'MAKT — Descrições de material por idioma. Fonte: {catalog}.bronze.makt'
""")

# COMMAND ----------

raw = spark.table(f"{catalog}.bronze.makt")
filtered = raw.filter(F.trim(F.col("mandt")) == "100")

key_cols = ["mandt", "matnr", "spras"]
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

transformed = (
    deduped
    .withColumn("matnr", F.trim(F.col("matnr")))
    .withColumn("maktx", _empty_null("maktx"))
    .withColumn("_op_clean", F.coalesce(F.trim(F.col("_op")), F.lit("I")))
    .withColumn("is_deleted", F.col("_op_clean") == "D")
    .withColumn("deleted_at",
        F.when(F.col("_op_clean") == "D", F.to_timestamp(F.col("_extracted_at")))
        .otherwise(F.lit(None).cast("timestamp"))
    )
    .withColumn("_silver_updated_at", F.current_timestamp())
    .select("mandt", "matnr", "spras", "maktx", "is_deleted", "deleted_at", "_silver_updated_at")
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.silver.makt")

(
    target.alias("tgt")
    .merge(
        transformed.alias("src"),
        "tgt.mandt = src.mandt AND tgt.matnr = src.matnr AND tgt.spras = src.spras"
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

print(f"MERGE concluído → {catalog}.silver.makt")
spark.table(f"{catalog}.silver.makt").groupBy("spras").count().show()
