# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold — dim_date (Dimensão de Data)
# MAGIC
# MAGIC Gera uma spine de datas de 2015-01-01 a 2030-12-31 usando `spark.range` +
# MAGIC aritmética de datas — sem nenhuma fonte SAP.
# MAGIC
# MAGIC ### Por que gerar a dimensão de data?
# MAGIC Dimensões de data são sempre geradas, nunca extraídas. O SAP não exporta
# MAGIC um calendário; a granularidade (dia, semana, mês, trimestre) é definida
# MAGIC pela necessidade analítica, não pela fonte.
# MAGIC
# MAGIC ### Idempotência
# MAGIC Usa MERGE com `WHEN NOT MATCHED THEN INSERT`: datas existentes nunca são
# MAGIC alteradas (são fatos imutáveis). Re-executar o job é seguro.

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_date (
  date_id          INTEGER   NOT NULL  COMMENT 'Chave inteira YYYYMMDD — usada nos fatos como FK.',
  date_value       DATE      NOT NULL  COMMENT 'Data como DATE.',
  year             INTEGER   NOT NULL  COMMENT 'Ano (2015–2030).',
  month            INTEGER   NOT NULL  COMMENT 'Mês (1–12).',
  day              INTEGER   NOT NULL  COMMENT 'Dia do mês (1–31).',
  quarter          INTEGER   NOT NULL  COMMENT 'Trimestre (1–4).',
  day_of_week      INTEGER   NOT NULL  COMMENT 'Dia da semana Spark: 1=domingo, 7=sábado.',
  day_name         STRING    NOT NULL  COMMENT 'Nome do dia em inglês (Monday, Tuesday...).',
  month_name       STRING    NOT NULL  COMMENT 'Nome do mês em inglês (January...).',
  is_weekday       BOOLEAN   NOT NULL  COMMENT 'True para segunda a sexta.'
)
USING DELTA
COMMENT 'Dimensão de data gerada localmente. Cobre 2015-01-01 a 2030-12-31.'
""")

# COMMAND ----------

start = date(2015, 1, 1)
end   = date(2030, 12, 31)
n_days = (end - start).days + 1

dates_df = (
    spark.range(n_days)
    .withColumn("date_value",  F.date_add(F.lit(start), F.col("id").cast("int")))
    .withColumn("date_id",     F.date_format(F.col("date_value"), "yyyyMMdd").cast("int"))
    .withColumn("year",        F.year(F.col("date_value")))
    .withColumn("month",       F.month(F.col("date_value")))
    .withColumn("day",         F.dayofmonth(F.col("date_value")))
    .withColumn("quarter",     F.quarter(F.col("date_value")))
    # Spark: 1=Sunday, 2=Monday..., 7=Saturday
    .withColumn("day_of_week", F.dayofweek(F.col("date_value")))
    .withColumn("day_name",    F.date_format(F.col("date_value"), "EEEE"))
    .withColumn("month_name",  F.date_format(F.col("date_value"), "MMMM"))
    # Weekday: Spark day_of_week 2=Mon..6=Fri
    .withColumn("is_weekday",  F.col("day_of_week").between(2, 6))
    .drop("id")
    .select(
        "date_id", "date_value", "year", "month", "day", "quarter",
        "day_of_week", "day_name", "month_name", "is_weekday"
    )
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.gold.dim_date")

# Apenas insere datas novas; nunca altera datas existentes (imutáveis)
(
    target.alias("tgt")
    .merge(dates_df.alias("src"), "tgt.date_id = src.date_id")
    .whenNotMatchedInsert(values={c: f"src.{c}" for c in dates_df.columns})
    .execute()
)

print(f"dim_date atualizada → {catalog}.gold.dim_date")
spark.table(f"{catalog}.gold.dim_date").select(
    F.count("*").alias("total_dates"),
    F.min("date_value").alias("first"),
    F.max("date_value").alias("last"),
).show()
