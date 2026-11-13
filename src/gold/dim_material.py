# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold — dim_material (Dimensão de Material)
# MAGIC
# MAGIC Fonte: `silver.mara` JOIN `silver.makt` (SPRAS='P' — português).
# MAGIC
# MAGIC ### Decisões de modelagem
# MAGIC - Materiais excluídos fisicamente (`is_deleted=true`) são excluídos da dim.
# MAGIC - Materiais marcados logicamente (`is_logically_deleted=true`) incluídos com
# MAGIC   `material_status='MARKED_FOR_DELETION'`.
# MAGIC - Descrição em português (`SPRAS='P'`) da MAKT. Se não existir, `material_description=NULL`.
# MAGIC - Nomes de coluna em inglês com comentário UC indicando campo SAP de origem.

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_material (
  material_id           STRING  NOT NULL  COMMENT 'SAP MATNR — chave do material (formato original com zeros).',
  material_description  STRING            COMMENT 'SAP MAKTX (SPRAS=P) — descrição em português.',
  material_type         STRING            COMMENT 'SAP MTART — tipo (FERT, ROH, HALB, HAWA).',
  industry              STRING            COMMENT 'SAP MBRSH — setor industrial.',
  material_group        STRING            COMMENT 'SAP MATKL — grupo de materiais.',
  base_uom              STRING            COMMENT 'SAP MEINS — unidade de medida base.',
  gross_weight          DECIMAL(18,3)     COMMENT 'SAP BRGEW — peso bruto.',
  net_weight            DECIMAL(18,3)     COMMENT 'SAP NTGEW — peso líquido.',
  weight_unit           STRING            COMMENT 'SAP GEWEI — unidade de peso.',
  material_status       STRING  NOT NULL  COMMENT 'ACTIVE | MARKED_FOR_DELETION. Derivado de LVORM.',
  _gold_updated_at      TIMESTAMP NOT NULL COMMENT 'Última atualização na gold.'
)
USING DELTA
COMMENT 'dim_material — Dimensão de materiais com descrição em português. Fonte: {catalog}.silver.mara + makt'
""")

# COMMAND ----------

mara = (
    spark.table(f"{catalog}.silver.mara")
    .filter(F.col("is_deleted") == False)
)

# Descrição em português (SPRAS='P') da MAKT
makt_pt = (
    spark.table(f"{catalog}.silver.makt")
    .filter((F.col("spras") == "P") & (F.col("is_deleted") == False))
    .select("matnr", F.col("maktx").alias("material_description"))
)

dim_df = (
    mara
    .join(makt_pt, "matnr", "left")
    .withColumn("material_status",
        F.when(F.col("is_logically_deleted"), F.lit("MARKED_FOR_DELETION"))
        .otherwise(F.lit("ACTIVE"))
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .select(
        F.col("matnr").alias("material_id"),
        "material_description",
        F.col("mtart").alias("material_type"),
        F.col("mbrsh").alias("industry"),
        F.col("matkl").alias("material_group"),
        F.col("meins").alias("base_uom"),
        F.col("brgew").alias("gross_weight"),
        F.col("ntgew").alias("net_weight"),
        F.col("gewei").alias("weight_unit"),
        "material_status",
        "_gold_updated_at",
    )
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.gold.dim_material")

(
    target.alias("tgt")
    .merge(dim_df.alias("src"), "tgt.material_id = src.material_id")
    .whenMatchedUpdate(set={c: f"src.{c}" for c in dim_df.columns})
    .whenNotMatchedInsert(values={c: f"src.{c}" for c in dim_df.columns})
    .execute()
)

print(f"MERGE concluído → {catalog}.gold.dim_material")
spark.table(f"{catalog}.gold.dim_material").select(
    F.count("*").alias("total"),
    F.sum((F.col("material_description").isNull()).cast("int")).alias("without_description"),
).show()
spark.table(f"{catalog}.gold.dim_material").groupBy("material_status").count().show()
