# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — TCURR (Taxas de Câmbio)
# MAGIC
# MAGIC ### Conceito SAP central: GDATU invertido
# MAGIC SAP armazena a data de validade como `99999999 − YYYYMMDD` (subtração inteira).
# MAGIC Isso inverte a ordem cronológica: taxas mais recentes têm o menor valor de GDATU.
# MAGIC Ordenar por GDATU ASC = buscar da mais recente para a mais antiga.
# MAGIC
# MAGIC A silver mantém GDATU original (string) e adiciona `valid_from` (DATE real)
# MAGIC para uso em joins de negócio.
# MAGIC
# MAGIC `'99999999'` significa taxa sem data de fim (open-ended) → `valid_from = NULL`.
# MAGIC
# MAGIC ### Conceitos SAP cobertos
# MAGIC | Campo | Armadilha | Tratamento |
# MAGIC |-------|-----------|------------|
# MAGIC | GDATU | invertido; '99999999' = open-ended | 99999999 − int(GDATU) → DATE; '99999999' → NULL |
# MAGIC | UKURS | vírgula decimal; trailing spaces | trim, vírgula → ponto, → DECIMAL(18,5) |
# MAGIC | MANDT | só '100' nesta tabela no gerador | filtrar mesmo assim |

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.tcurr (
  mandt              STRING        NOT NULL  COMMENT 'SAP MANDT',
  kurst              STRING        NOT NULL  COMMENT 'SAP KURST — tipo de taxa (M=padrão, B=compra, G=venda).',
  fcurr              STRING        NOT NULL  COMMENT 'SAP FCURR — moeda de origem (USD, EUR).',
  tcurr              STRING        NOT NULL  COMMENT 'SAP TCURR — moeda de destino (BRL, EUR).',
  gdatu              STRING        NOT NULL  COMMENT 'SAP GDATU — data em formato INVERTIDO (99999999 - YYYYMMDD). Mantida para auditoria.',
  valid_from         DATE                    COMMENT 'Data real de início da taxa. Derivada de GDATU. NULL = open-ended (99999999).',
  ukurs              DECIMAL(18,5)           COMMENT 'SAP UKURS — taxa: 1 FCURR = UKURS TCURR.',
  ffact              INTEGER                 COMMENT 'SAP FFACT — fator moeda origem (normalmente 1).',
  tfact              INTEGER                 COMMENT 'SAP TFACT — fator moeda destino (normalmente 1).',
  is_deleted         BOOLEAN       NOT NULL  COMMENT 'Exclusão física via CDC.',
  deleted_at         TIMESTAMP               COMMENT 'Timestamp da exclusão física.',
  _silver_updated_at TIMESTAMP     NOT NULL  COMMENT 'Última atualização pelo MERGE.'
)
USING DELTA
COMMENT 'TCURR — Taxas de câmbio. Fonte: {catalog}.bronze.tcurr'
""")

# COMMAND ----------

raw = spark.table(f"{catalog}.bronze.tcurr")
filtered = raw.filter(F.trim(F.col("mandt")) == "100")

key_cols = ["mandt", "kurst", "fcurr", "tcurr", "gdatu"]
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

def _invert_gdatu(c: str) -> F.Column:
    """
    GDATU invertido → DATE real.
    '99999999' = open-ended → NULL.
    Lógica: real_int = 99999999 - int(gdatu)
            → formata como string 8 chars → to_date('yyyyMMdd')
    """
    t = F.trim(F.col(c))
    real_int = (F.lit(99999999) - t.cast("long")).cast("string")
    real_date_str = F.lpad(real_int, 8, "0")
    return F.when(
        t.isNull() | (t == "") | (t == "99999999") | (t == "00000000"),
        F.lit(None)
    ).otherwise(F.to_date(real_date_str, "yyyyMMdd"))

def _parse_decimal(c: str, prec: int = 18, scale: int = 5) -> F.Column:
    t = F.trim(F.regexp_replace(F.col(c), ",", "."))
    return F.when(
        t.isNull() | (t == "") | ~t.rlike(r"^-?\d+(\.\d+)?$"),
        F.lit(None),
    ).otherwise(t.cast(f"DECIMAL({prec},{scale})"))

transformed = (
    deduped
    # Mantém GDATU original para auditoria
    .withColumn("gdatu",      F.trim(F.col("gdatu")))
    # Deriva a data real
    .withColumn("valid_from", _invert_gdatu("gdatu"))
    # Taxa de câmbio: vírgula decimal e trailing spaces ocorrem em ~5% dos registros
    .withColumn("ukurs",      _parse_decimal("ukurs", 18, 5))
    .withColumn("ffact",      F.col("ffact").cast("INTEGER"))
    .withColumn("tfact",      F.col("tfact").cast("INTEGER"))
    # CDC
    .withColumn("_op_clean",  F.coalesce(F.trim(F.col("_op")), F.lit("I")))
    .withColumn("is_deleted", F.col("_op_clean") == "D")
    .withColumn("deleted_at",
        F.when(F.col("_op_clean") == "D", F.to_timestamp(F.col("_extracted_at")))
        .otherwise(F.lit(None).cast("timestamp"))
    )
    .withColumn("_silver_updated_at", F.current_timestamp())
    .select(
        "mandt", "kurst", "fcurr", "tcurr", "gdatu", "valid_from",
        "ukurs", "ffact", "tfact",
        "is_deleted", "deleted_at", "_silver_updated_at",
    )
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.silver.tcurr")

(
    target.alias("tgt")
    .merge(
        transformed.alias("src"),
        """tgt.mandt = src.mandt AND tgt.kurst = src.kurst
           AND tgt.fcurr = src.fcurr AND tgt.tcurr = src.tcurr
           AND tgt.gdatu = src.gdatu"""
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

print(f"MERGE concluído → {catalog}.silver.tcurr")
spark.table(f"{catalog}.silver.tcurr").select(
    F.count("*").alias("total"),
    F.countDistinct("fcurr", "tcurr").alias("currency_pairs"),
    F.sum(F.col("is_deleted").cast("int")).alias("deleted"),
).show()
