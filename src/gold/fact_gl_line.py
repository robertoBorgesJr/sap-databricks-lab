# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold — fact_gl_line (Fato: Linha Contábil)
# MAGIC
# MAGIC **Grão:** 1 linha por `(mandt, bukrs, belnr, gjahr, buzei)` — cada linha de
# MAGIC cada documento contábil.
# MAGIC
# MAGIC ### Fontes
# MAGIC | Tabela | Papel |
# MAGIC |--------|-------|
# MAGIC | `silver.bseg` | linhas com valor assinado (`amount`) |
# MAGIC | `silver.bkpf` | cabeçalho (datas, moeda, status) |
# MAGIC
# MAGIC ### Coluna `amount`
# MAGIC A silver.bseg já aplicou o sinal de SHKZG: S=débito (+), H=crédito (−).
# MAGIC Este fato apenas projeta esse valor sem nenhuma transformação adicional.
# MAGIC Invariante: `SUM(amount) = 0` por documento.

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_gl_line (
  mandt            STRING        NOT NULL  COMMENT 'SAP MANDT.',
  bukrs            STRING        NOT NULL  COMMENT 'SAP BUKRS — empresa.',
  belnr            STRING        NOT NULL  COMMENT 'SAP BELNR — documento contábil.',
  gjahr            STRING        NOT NULL  COMMENT 'SAP GJAHR — exercício fiscal.',
  buzei            STRING        NOT NULL  COMMENT 'SAP BUZEI — linha do item.',
  document_date    DATE                    COMMENT 'SAP BLDAT — data do documento.',
  posting_date     DATE                    COMMENT 'SAP BUDAT — data de lançamento.',
  fiscal_period    STRING                  COMMENT 'SAP MONAT — período fiscal (mês).',
  doc_currency     STRING                  COMMENT 'SAP WAERS — moeda do documento.',
  doc_text         STRING                  COMMENT 'SAP BKTXT — texto do cabeçalho.',
  debit_credit     STRING                  COMMENT 'SAP SHKZG — S=débito, H=crédito.',
  amount           DECIMAL(18,2)           COMMENT 'Valor com sinal: S=positivo, H=negativo. Derivado de DMBTR+SHKZG em silver.bseg.',
  gl_account       STRING                  COMMENT 'SAP HKONT — conta contábil.',
  cost_center      STRING                  COMMENT 'SAP KOSTL — centro de custo.',
  tax_code         STRING                  COMMENT 'SAP MWSKZ — código de imposto.',
  posting_date_id  INTEGER                 COMMENT 'SAP BUDAT como YYYYMMDD — FK para dim_date.',
  _gold_updated_at TIMESTAMP     NOT NULL  COMMENT 'Última atualização na gold.'
)
USING DELTA
COMMENT 'fact_gl_line — Linhas contábeis com valor assinado. Invariante: SUM(amount)=0 por doc. Fonte: silver.bseg + bkpf'
""")

# COMMAND ----------

bseg = (
    spark.table(f"{catalog}.silver.bseg")
    .filter(F.col("is_deleted") == False)
    .select("mandt", "bukrs", "belnr", "gjahr", "buzei",
            "shkzg", "amount", "hkont", "kostl", "mwskz")
)

bkpf = (
    spark.table(f"{catalog}.silver.bkpf")
    .filter(F.col("is_deleted") == False)
    .select("mandt", "bukrs", "belnr", "gjahr",
            "bldat", "budat", "monat", "bktxt", "waers")
)

fact_df = (
    bseg.join(bkpf, ["mandt", "bukrs", "belnr", "gjahr"])
    .withColumn("posting_date_id",
        F.when(F.col("budat").isNotNull(),
            F.date_format(F.col("budat"), "yyyyMMdd").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .select(
        "mandt", "bukrs", "belnr", "gjahr", "buzei",
        F.col("bldat").alias("document_date"),
        F.col("budat").alias("posting_date"),
        F.col("monat").alias("fiscal_period"),
        F.col("waers").alias("doc_currency"),
        F.col("bktxt").alias("doc_text"),
        F.col("shkzg").alias("debit_credit"),
        "amount",
        F.col("hkont").alias("gl_account"),
        F.col("kostl").alias("cost_center"),
        F.col("mwskz").alias("tax_code"),
        "posting_date_id",
        "_gold_updated_at",
    )
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.gold.fact_gl_line")

(
    target.alias("tgt")
    .merge(
        fact_df.alias("src"),
        """tgt.mandt = src.mandt AND tgt.bukrs = src.bukrs
           AND tgt.belnr = src.belnr AND tgt.gjahr = src.gjahr
           AND tgt.buzei = src.buzei"""
    )
    .whenMatchedUpdate(set={c: f"src.{c}" for c in fact_df.columns})
    .whenNotMatchedInsert(values={c: f"src.{c}" for c in fact_df.columns})
    .execute()
)

print(f"MERGE concluído → {catalog}.gold.fact_gl_line")

# Verificação do invariante contábil na gold
balance_check = (
    spark.table(f"{catalog}.gold.fact_gl_line")
    .groupBy("mandt", "bukrs", "belnr", "gjahr")
    .agg(F.sum("amount").alias("balance"))
    .filter(F.abs(F.col("balance")) > F.lit(0.01))
)
print(f"Documentos fora do balanço na gold: {balance_check.count()}  (esperado: 0)")
spark.table(f"{catalog}.gold.fact_gl_line").select(
    F.count("*").alias("total_lines"),
    F.sum("amount").alias("net_amount"),
).show()
