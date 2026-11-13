# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — MARA (Material Master / Dados Gerais do Material)
# MAGIC
# MAGIC ### Conceitos SAP cobertos
# MAGIC | Campo | Armadilha | Tratamento |
# MAGIC |-------|-----------|------------|
# MAGIC | ERSDA | DATS '00000000' | → NULL |
# MAGIC | LVORM | ' ' (espaço) ≠ '' | trim → flag bool |
# MAGIC | BRGEW/NTGEW | vírgula decimal; vazio; '0.000' | → DECIMAL(18,3) ou NULL |
# MAGIC | MEINS | trailing space ('ST ') | trim |
# MAGIC | MATNR | trailing spaces 18 chars; pode vir sem zero-padding (legado) | trim |
# MAGIC | MATKL | pode ser vazio | vazio → NULL |

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.mara (
  mandt                 STRING        NOT NULL  COMMENT 'SAP MANDT',
  matnr                 STRING        NOT NULL  COMMENT 'SAP MATNR — 18 chars. Numérico zero-padded ou alfanumérico.',
  ersda                 DATE                    COMMENT 'SAP ERSDA — data de criação. DATS 00000000 → NULL.',
  ernam                 STRING                  COMMENT 'SAP ERNAM — usuário criador.',
  mtart                 STRING                  COMMENT 'SAP MTART — tipo de material (FERT, ROH, HALB, HAWA).',
  mbrsh                 STRING                  COMMENT 'SAP MBRSH — setor industrial.',
  matkl                 STRING                  COMMENT 'SAP MATKL — grupo de materiais. Vazio → NULL.',
  meins                 STRING                  COMMENT 'SAP MEINS — unidade de medida base. Trailing space → trim.',
  brgew                 DECIMAL(18,3)            COMMENT 'SAP BRGEW — peso bruto. Vírgula decimal → period; vazio → NULL.',
  gewei                 STRING                  COMMENT 'SAP GEWEI — unidade de peso (KG, G, LB).',
  ntgew                 DECIMAL(18,3)            COMMENT 'SAP NTGEW — peso líquido. Mesmo tratamento de BRGEW.',
  lvorm                 STRING                  COMMENT 'SAP LVORM — flag de exclusão original.',
  is_logically_deleted  BOOLEAN       NOT NULL  COMMENT 'Derivado de LVORM.',
  is_deleted            BOOLEAN       NOT NULL  COMMENT 'Exclusão física via CDC.',
  deleted_at            TIMESTAMP               COMMENT 'Timestamp da exclusão física.',
  _silver_updated_at    TIMESTAMP     NOT NULL  COMMENT 'Última atualização pelo MERGE.'
)
USING DELTA
COMMENT 'MARA — Dados gerais do material. Fonte: {catalog}.bronze.mara'
""")

# COMMAND ----------

raw = spark.table(f"{catalog}.bronze.mara")
filtered = raw.filter(F.trim(F.col("mandt")) == "100")

key_cols = ["mandt", "matnr"]
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

def _parse_decimal(c: str, prec: int = 18, scale: int = 3) -> F.Column:
    t = F.trim(F.regexp_replace(F.col(c), ",", "."))
    return F.when(
        t.isNull() | (t == "") | ~t.rlike(r"^-?\d+(\.\d+)?$"),
        F.lit(None),
    ).otherwise(t.cast(f"DECIMAL({prec},{scale})"))

transformed = (
    deduped
    .withColumn("matnr",  F.trim(F.col("matnr")))
    .withColumn("ernam",  _empty_null("ernam"))
    .withColumn("mtart",  _empty_null("mtart"))
    .withColumn("mbrsh",  _empty_null("mbrsh"))
    .withColumn("matkl",  _empty_null("matkl"))
    .withColumn("meins",  _empty_null("meins"))
    .withColumn("gewei",  _empty_null("gewei"))
    # Data de criação
    .withColumn("ersda",  _parse_dats("ersda"))
    # Pesos: vírgula decimal, vazio ou '0.000' são todos válidos
    .withColumn("brgew",  _parse_decimal("brgew", 18, 3))
    .withColumn("ntgew",  _parse_decimal("ntgew", 18, 3))
    # Flag LVORM: mesma armadilha do espaço que LOEVM em KNA1
    .withColumn("lvorm",  F.when(F.trim(F.col("lvorm")) == "", F.lit("")).otherwise(F.trim(F.col("lvorm"))))
    .withColumn("is_logically_deleted", F.trim(F.col("lvorm")) == "X")
    # CDC
    .withColumn("_op_clean", F.coalesce(F.trim(F.col("_op")), F.lit("I")))
    .withColumn("is_deleted", F.col("_op_clean") == "D")
    .withColumn("deleted_at",
        F.when(F.col("_op_clean") == "D", F.to_timestamp(F.col("_extracted_at")))
        .otherwise(F.lit(None).cast("timestamp"))
    )
    .withColumn("_silver_updated_at", F.current_timestamp())
    .select(
        "mandt", "matnr", "ersda", "ernam", "mtart", "mbrsh", "matkl",
        "meins", "brgew", "gewei", "ntgew", "lvorm",
        "is_logically_deleted", "is_deleted", "deleted_at", "_silver_updated_at",
    )
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.silver.mara")

(
    target.alias("tgt")
    .merge(transformed.alias("src"), "tgt.mandt = src.mandt AND tgt.matnr = src.matnr")
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

print(f"MERGE concluído → {catalog}.silver.mara")
spark.table(f"{catalog}.silver.mara").select(
    F.count("*").alias("total"),
    F.sum(F.col("is_deleted").cast("int")).alias("deleted"),
).show()
