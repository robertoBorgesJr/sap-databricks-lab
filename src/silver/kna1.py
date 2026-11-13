# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — KNA1 (Customer Master / Dados Gerais do Cliente)
# MAGIC
# MAGIC Lê de `bronze.kna1`, aplica todas as conversões SAP e faz MERGE idempotente
# MAGIC em `silver.kna1`.
# MAGIC
# MAGIC ### Conceitos SAP cobertos
# MAGIC | Campo | Armadilha | Tratamento |
# MAGIC |-------|-----------|------------|
# MAGIC | ERDAT | DATS '00000000' | → NULL |
# MAGIC | LOEVM | ' ' (espaço) ≠ '' | trim → flag bool |
# MAGIC | NAME1 | trailing spaces 35 chars | trim; vazio → NULL |
# MAGIC | KUNNR | pode ser alfanumérico (migração) | mantido como veio, apenas trim |
# MAGIC | MANDT | '100' e '200' no extrato | filtrar para '100' |
# MAGIC
# MAGIC ### Padrão de deduplicação
# MAGIC Bronze é append-only. Registros da carga inicial chegam com `_seq = NULL`
# MAGIC (são anteriores a qualquer CDC). Os lotes CDC sobrescrevem com `_seq` crescente.
# MAGIC Deduplicação: `ROW_NUMBER() OVER (PARTITION BY mandt, kunnr ORDER BY _seq_int DESC) = 1`.

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------
# MAGIC %md ### 1. DDL — cria a tabela silver na primeira execução

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.kna1 (
  mandt                 STRING    NOT NULL  COMMENT 'SAP MANDT — mandante (filtrado para 100)',
  kunnr                 STRING    NOT NULL  COMMENT 'SAP KUNNR — número do cliente, 10 chars ALPHA',
  land1                 STRING              COMMENT 'SAP LAND1 — país (BR). Pode ser NULL por erro de migração.',
  name1                 STRING              COMMENT 'SAP NAME1 — razão social, 35 chars CHAR. Trailing spaces removidos.',
  name2                 STRING              COMMENT 'SAP NAME2 — complemento de nome. Vazio → NULL.',
  ort01                 STRING              COMMENT 'SAP ORT01 — cidade.',
  pstlz                 STRING              COMMENT 'SAP PSTLZ — CEP.',
  stras                 STRING              COMMENT 'SAP STRAS — logradouro.',
  stcd1                 STRING              COMMENT 'SAP STCD1 — CNPJ (14 dígitos).',
  stcd2                 STRING              COMMENT 'SAP STCD2 — CPF. Vazio → NULL.',
  loevm                 STRING              COMMENT 'SAP LOEVM — flag de exclusão original (X ou vazio).',
  erdat                 DATE                COMMENT 'SAP ERDAT — data de criação. DATS 00000000 → NULL.',
  ernam                 STRING              COMMENT 'SAP ERNAM — usuário criador.',
  telf1                 STRING              COMMENT 'SAP TELF1 — telefone. Vazio → NULL.',
  spras                 STRING              COMMENT 'SAP SPRAS — idioma (P=português).',
  ktokd                 STRING              COMMENT 'SAP KTOKD — grupo de contas.',
  is_logically_deleted  BOOLEAN   NOT NULL  COMMENT 'Derivado de LOEVM. True = cliente marcado para exclusão no SAP.',
  is_deleted            BOOLEAN   NOT NULL  COMMENT 'True = excluído fisicamente via CDC (_op=D).',
  deleted_at            TIMESTAMP           COMMENT 'Timestamp da exclusão física. NULL quando is_deleted=false.',
  _silver_updated_at    TIMESTAMP NOT NULL  COMMENT 'Última vez que esta linha foi processada pelo MERGE.'
)
USING DELTA
COMMENT 'KNA1 — Dados gerais do cliente. Fonte: {catalog}.bronze.kna1'
""")

# COMMAND ----------
# MAGIC %md ### 2. Leitura, filtro e deduplicação

raw = spark.table(f"{catalog}.bronze.kna1")

# CDC columns (_op, _seq, _extracted_at) are absent when only the initial full-load
# batch has been ingested (Auto Loader adds them via addNewColumns when CDC files arrive).
# Add them as NULL strings so the transform works identically for both paths:
# initial load → _op=NULL → coalesced to "I" (insert); _seq=NULL → 0 (baseline order).
_raw_cols_lower = {c.lower() for c in raw.columns}
for _cdc_col in ("_op", "_seq", "_extracted_at"):
    if _cdc_col not in _raw_cols_lower:
        raw = raw.withColumn(_cdc_col, F.lit(None).cast("string"))

# Filtrar mandante principal. MANDT='200' existe no extrato bruto para simular
# ambiente multi-tenant, mas silver só contém o mandante de produção.
filtered = raw.filter(F.trim(F.col("mandt")) == "100")

# Deduplicação por chave.
# Carga inicial: _seq = NULL → int 0 (baseline, antes de qualquer CDC).
# CDC posterior: _seq = "5", "12"... → sobrescreve a carga inicial.
key_cols = ["mandt", "kunnr"]
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
# MAGIC %md ### 3. Conversões SAP (funções nativas Spark)

def _parse_dats(c: str) -> F.Column:
    """DATS: trim; '00000000' e branco → NULL; demais → DATE."""
    t = F.trim(F.col(c))
    return F.when(t.isNull() | (t == "") | (t == "00000000"), F.lit(None)).otherwise(
        F.to_date(t, "yyyyMMdd")
    )

def _empty_null(c: str) -> F.Column:
    """Blank/whitespace → NULL; demais → trim."""
    t = F.trim(F.col(c))
    return F.when(t == "", F.lit(None)).otherwise(t)

transformed = (
    deduped
    # CHAR fields: trailing spaces removidos
    .withColumn("kunnr",  F.trim(F.col("kunnr")))
    .withColumn("land1",  _empty_null("land1"))
    .withColumn("name1",  _empty_null("name1"))
    .withColumn("name2",  _empty_null("name2"))
    .withColumn("ort01",  _empty_null("ort01"))
    .withColumn("pstlz",  _empty_null("pstlz"))
    .withColumn("stras",  _empty_null("stras"))
    .withColumn("stcd1",  _empty_null("stcd1"))
    .withColumn("stcd2",  _empty_null("stcd2"))
    .withColumn("telf1",  _empty_null("telf1"))
    .withColumn("ernam",  _empty_null("ernam"))
    # Data de criação: DATS → DATE
    .withColumn("erdat",  _parse_dats("erdat"))
    # Flag LOEVM: normalizar espaço → vazio; derivar booleano.
    # Armadilha: ' ' (espaço único) deve ser False, não True.
    .withColumn("loevm",  F.when(F.trim(F.col("loevm")) == "", F.lit("")).otherwise(F.trim(F.col("loevm"))))
    .withColumn("is_logically_deleted", F.trim(F.col("loevm")) == "X")
    # Colunas CDC: _op = NULL para carga inicial → tratar como "I"
    .withColumn("_op_clean", F.coalesce(F.trim(F.col("_op")), F.lit("I")))
    .withColumn("is_deleted",
        F.col("_op_clean") == "D"
    )
    .withColumn("deleted_at",
        F.when(F.col("_op_clean") == "D",
            F.to_timestamp(F.col("_extracted_at"))
        ).otherwise(F.lit(None).cast("timestamp"))
    )
    .withColumn("_silver_updated_at", F.current_timestamp())
    .select(
        "mandt", "kunnr", "land1", "name1", "name2", "ort01", "pstlz", "stras",
        "stcd1", "stcd2", "loevm", "erdat", "ernam", "telf1", "spras", "ktokd",
        "is_logically_deleted", "is_deleted", "deleted_at", "_silver_updated_at",
    )
)

# COMMAND ----------
# MAGIC %md ### 4. MERGE idempotente em silver.kna1

target = DeltaTable.forName(spark, f"{catalog}.silver.kna1")

(
    target.alias("tgt")
    .merge(
        transformed.alias("src"),
        "tgt.mandt = src.mandt AND tgt.kunnr = src.kunnr"
    )
    # Exclusão física via CDC (_op='D'): soft-delete, preserva histórico
    .whenMatchedUpdate(
        condition="src.is_deleted = true",
        set={
            "is_deleted":         "src.is_deleted",
            "deleted_at":         "src.deleted_at",
            "_silver_updated_at": "src._silver_updated_at",
        }
    )
    # Upsert: insert ou update de negócio
    .whenMatchedUpdate(
        condition="src.is_deleted = false",
        set={c: f"src.{c}" for c in transformed.columns}
    )
    # Novo registro (apenas para registros não-deletados)
    .whenNotMatchedInsert(
        condition="src.is_deleted = false",
        values={c: f"src.{c}" for c in transformed.columns}
    )
    .execute()
)

print(f"MERGE concluído → {catalog}.silver.kna1")
spark.table(f"{catalog}.silver.kna1").select(
    F.count("*").alias("total"),
    F.sum(F.col("is_deleted").cast("int")).alias("deleted"),
    F.sum(F.col("is_logically_deleted").cast("int")).alias("logically_deleted"),
).show()
