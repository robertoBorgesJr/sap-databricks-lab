# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — BSEG (Linhas de Documento Contábil)
# MAGIC
# MAGIC ### Conceito central: DMBTR + SHKZG → amount
# MAGIC Em SAP, `DMBTR` é **sempre positivo**. O sinal do lançamento vem de `SHKZG`:
# MAGIC - `'S'` (Soll = **débito**)  → valor positivo
# MAGIC - `'H'` (Haben = **crédito**) → valor negativo
# MAGIC
# MAGIC A silver deriva `amount = DMBTR * sinal(SHKZG)`. Invariante contábil:
# MAGIC `SUM(amount) = 0` por documento (débitos = créditos).
# MAGIC
# MAGIC ### Conceitos SAP cobertos
# MAGIC | Campo | Armadilha | Tratamento |
# MAGIC |-------|-----------|------------|
# MAGIC | DMBTR | vírgula decimal (extração legada) | trim, vírgula → ponto → DECIMAL |
# MAGIC | SHKZG | trailing space ('S ', 'H ') | trim antes de comparar |
# MAGIC | KOSTL | trailing spaces 10 chars; vazio | trim; vazio → NULL |
# MAGIC | MWSKZ | ' ' (espaço) em vez de '' | trim; espaço → NULL |
# MAGIC | HKONT | pode vir sem zero-padding (legado 6 dígitos) | mantido como veio |

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.bseg (
  mandt              STRING        NOT NULL  COMMENT 'SAP MANDT',
  bukrs              STRING        NOT NULL  COMMENT 'SAP BUKRS — empresa.',
  belnr              STRING        NOT NULL  COMMENT 'SAP BELNR — documento contábil.',
  gjahr              STRING        NOT NULL  COMMENT 'SAP GJAHR — exercício fiscal.',
  buzei              STRING        NOT NULL  COMMENT 'SAP BUZEI — linha do item, 3 chars (001, 002...).',
  shkzg              STRING                  COMMENT 'SAP SHKZG — S=débito (Soll), H=crédito (Haben). Trailing space → trim.',
  dmbtr              DECIMAL(18,2)           COMMENT 'SAP DMBTR — valor sempre positivo. Vírgula decimal → ponto.',
  amount             DECIMAL(18,2)           COMMENT 'Derivado: dmbtr com sinal de shkzg. S=positivo, H=negativo.',
  hkont              STRING                  COMMENT 'SAP HKONT — conta contábil 10 chars. Pode vir sem zero-padding.',
  kostl              STRING                  COMMENT 'SAP KOSTL — centro de custo. Vazio → NULL.',
  mwskz              STRING                  COMMENT 'SAP MWSKZ — código de imposto. Espaço → NULL.',
  is_deleted         BOOLEAN       NOT NULL  COMMENT 'Exclusão física via CDC.',
  deleted_at         TIMESTAMP               COMMENT 'Timestamp da exclusão física.',
  _silver_updated_at TIMESTAMP     NOT NULL  COMMENT 'Última atualização pelo MERGE.'
)
USING DELTA
COMMENT 'BSEG — Linhas de documento contábil. Invariante: SUM(amount)=0 por documento. Fonte: {catalog}.bronze.bseg'
""")

# COMMAND ----------

raw = spark.table(f"{catalog}.bronze.bseg")
filtered = raw.filter(F.trim(F.col("mandt")) == "100")

key_cols = ["mandt", "bukrs", "belnr", "gjahr", "buzei"]
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

def _signed_amount(dmbtr_col: str, shkzg_col: str) -> F.Column:
    """
    BSEG: DMBTR sempre positivo + sinal de SHKZG.
    'H' (Haben = crédito) → negativo; 'S' (Soll = débito) → positivo.
    Armadilha: SHKZG pode vir com trailing space ('S ') — trim antes de comparar.
    """
    amount = _parse_decimal(dmbtr_col, 18, 2)
    return F.when(F.trim(F.col(shkzg_col)) == "H", -amount).otherwise(amount)

transformed = (
    deduped
    .withColumn("bukrs",  F.trim(F.col("bukrs")))
    .withColumn("belnr",  F.trim(F.col("belnr")))
    .withColumn("hkont",  _empty_null("hkont"))
    .withColumn("kostl",  _empty_null("kostl"))
    # MWSKZ: ' ' (espaço) deve virar NULL — mesma armadilha de BSTAT
    .withColumn("mwskz",  _empty_null("mwskz"))
    # SHKZG normalizado (trim); mantido como string original após trim
    .withColumn("shkzg",  F.trim(F.col("shkzg")))
    # DMBTR sempre positivo
    .withColumn("dmbtr",  _parse_decimal("dmbtr", 18, 2))
    # amount = DMBTR com sinal de SHKZG — coluna principal para análise financeira
    .withColumn("amount", _signed_amount("dmbtr", "shkzg"))
    # CDC
    .withColumn("_op_clean", F.coalesce(F.trim(F.col("_op")), F.lit("I")))
    .withColumn("is_deleted", F.col("_op_clean") == "D")
    .withColumn("deleted_at",
        F.when(F.col("_op_clean") == "D", F.to_timestamp(F.col("_extracted_at")))
        .otherwise(F.lit(None).cast("timestamp"))
    )
    .withColumn("_silver_updated_at", F.current_timestamp())
    .select(
        "mandt", "bukrs", "belnr", "gjahr", "buzei",
        "shkzg", "dmbtr", "amount", "hkont", "kostl", "mwskz",
        "is_deleted", "deleted_at", "_silver_updated_at",
    )
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.silver.bseg")

(
    target.alias("tgt")
    .merge(
        transformed.alias("src"),
        """tgt.mandt = src.mandt AND tgt.bukrs = src.bukrs
           AND tgt.belnr = src.belnr AND tgt.gjahr = src.gjahr
           AND tgt.buzei = src.buzei"""
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

print(f"MERGE concluído → {catalog}.silver.bseg")

# Verificação do invariante contábil: SUM(amount) = 0 por documento
balance_check = (
    spark.table(f"{catalog}.silver.bseg")
    .filter(F.col("is_deleted") == False)
    .groupBy("mandt", "bukrs", "belnr", "gjahr")
    .agg(F.sum("amount").alias("balance"))
    .filter(F.abs(F.col("balance")) > F.lit(0.01))
)
unbalanced = balance_check.count()
print(f"Documentos fora do balanço débito=crédito: {unbalanced}  (esperado: 0)")
spark.table(f"{catalog}.silver.bseg").select(
    F.count("*").alias("total_lines"),
    F.sum(F.col("is_deleted").cast("int")).alias("deleted"),
).show()
