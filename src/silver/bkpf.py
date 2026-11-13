# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — BKPF (Cabeçalho de Documento Contábil)
# MAGIC
# MAGIC ### Conceitos SAP cobertos
# MAGIC | Campo | Armadilha | Tratamento |
# MAGIC |-------|-----------|------------|
# MAGIC | BLDAT | DATS '00000000' (data não preenchida na migração) | → NULL |
# MAGIC | BUDAT | data de lançamento — deve existir para docs postados | → DATE |
# MAGIC | BSTAT | ' ' (espaço) ≠ '' (não-lançado vs. parked) | trim; espaço → NULL |
# MAGIC | WAERS | trailing space ('BRL ') | trim |
# MAGIC | BKTXT | leading + trailing spaces (colado de sistema externo) | trim; vazio → NULL |
# MAGIC | BELNR | trailing space 10 chars (CHAR field) | trim |

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.bkpf (
  mandt              STRING    NOT NULL  COMMENT 'SAP MANDT',
  bukrs              STRING    NOT NULL  COMMENT 'SAP BUKRS — código da empresa (company code).',
  belnr              STRING    NOT NULL  COMMENT 'SAP BELNR — número do documento contábil, 10 chars.',
  gjahr              STRING    NOT NULL  COMMENT 'SAP GJAHR — exercício fiscal (ano 4 chars).',
  bldat              DATE                COMMENT 'SAP BLDAT — data do documento. DATS 00000000 → NULL.',
  budat              DATE                COMMENT 'SAP BUDAT — data de lançamento.',
  monat              STRING              COMMENT 'SAP MONAT — período contábil (mês 2 chars).',
  bktxt              STRING              COMMENT 'SAP BKTXT — texto do cabeçalho. Espaços → trim; vazio → NULL.',
  usnam              STRING              COMMENT 'SAP USNAM — usuário lançador.',
  bstat              STRING              COMMENT 'SAP BSTAT — status (vazio=lançado, V=estornado). Espaço → NULL.',
  waers              STRING              COMMENT 'SAP WAERS — moeda do documento. Trailing space → trim.',
  is_deleted         BOOLEAN   NOT NULL  COMMENT 'Exclusão física via CDC.',
  deleted_at         TIMESTAMP           COMMENT 'Timestamp da exclusão física.',
  _silver_updated_at TIMESTAMP NOT NULL  COMMENT 'Última atualização pelo MERGE.'
)
USING DELTA
COMMENT 'BKPF — Cabeçalho de documento contábil. Fonte: {catalog}.bronze.bkpf'
""")

# COMMAND ----------

raw = spark.table(f"{catalog}.bronze.bkpf")
filtered = raw.filter(F.trim(F.col("mandt")) == "100")

key_cols = ["mandt", "bukrs", "belnr", "gjahr"]
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

transformed = (
    deduped
    .withColumn("belnr",  F.trim(F.col("belnr")))
    .withColumn("bukrs",  F.trim(F.col("bukrs")))
    .withColumn("gjahr",  F.trim(F.col("gjahr")))
    .withColumn("monat",  _empty_null("monat"))
    .withColumn("usnam",  _empty_null("usnam"))
    .withColumn("waers",  _empty_null("waers"))
    # BKTXT: leading + trailing spaces de sistema externo
    .withColumn("bktxt",  _empty_null("bktxt"))
    # BSTAT: ' ' (espaço) deve virar NULL — não é o mesmo que '' (lançado)
    .withColumn("bstat",  _empty_null("bstat"))
    # Datas
    .withColumn("bldat",  _parse_dats("bldat"))
    .withColumn("budat",  _parse_dats("budat"))
    # CDC
    .withColumn("_op_clean", F.coalesce(F.trim(F.col("_op")), F.lit("I")))
    .withColumn("is_deleted", F.col("_op_clean") == "D")
    .withColumn("deleted_at",
        F.when(F.col("_op_clean") == "D", F.to_timestamp(F.col("_extracted_at")))
        .otherwise(F.lit(None).cast("timestamp"))
    )
    .withColumn("_silver_updated_at", F.current_timestamp())
    .select(
        "mandt", "bukrs", "belnr", "gjahr", "bldat", "budat", "monat",
        "bktxt", "usnam", "bstat", "waers",
        "is_deleted", "deleted_at", "_silver_updated_at",
    )
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.silver.bkpf")

(
    target.alias("tgt")
    .merge(
        transformed.alias("src"),
        "tgt.mandt = src.mandt AND tgt.bukrs = src.bukrs AND tgt.belnr = src.belnr AND tgt.gjahr = src.gjahr"
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

print(f"MERGE concluído → {catalog}.silver.bkpf")
spark.table(f"{catalog}.silver.bkpf").select(
    F.count("*").alias("total"),
    F.sum(F.col("is_deleted").cast("int")).alias("deleted"),
).show()
