# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold — fact_sales_order_item (Fato: Item de Ordem de Venda)
# MAGIC
# MAGIC **Grão:** 1 linha por `(mandt, vbeln, posnr)` — cada item de cada ordem de venda.
# MAGIC
# MAGIC ### Fontes
# MAGIC | Tabela | Papel |
# MAGIC |--------|-------|
# MAGIC | `silver.vbap` | itens (quantidade, valor, moeda) |
# MAGIC | `silver.vbak` | cabeçalho (data, cliente, org) |
# MAGIC | `silver.tcurr` | taxa de câmbio → conversão para BRL |
# MAGIC
# MAGIC ### Conversão para BRL via TCURR
# MAGIC Para ordens em USD ou EUR, busca a taxa `KURST='M'` mais recente com
# MAGIC `valid_from <= audat` (data da ordem). Lógica "as-of": a taxa vigente na
# MAGIC data do pedido.
# MAGIC - `waerk='BRL'` → `net_value_brl = net_value` (sem conversão).
# MAGIC - Taxa não encontrada → `net_value_brl = NULL` (dado ausente, sem estimativa).
# MAGIC
# MAGIC Esta é a prática recomendada em data warehouses: nunca inventar uma taxa.

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catalog (Unity Catalog)")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_sales_order_item (
  mandt            STRING        NOT NULL  COMMENT 'SAP MANDT.',
  vbeln            STRING        NOT NULL  COMMENT 'SAP VBELN — ordem de venda.',
  posnr            STRING        NOT NULL  COMMENT 'SAP POSNR — item da ordem.',
  customer_id      STRING                  COMMENT 'SAP KUNNR — FK para dim_customer.',
  material_id      STRING                  COMMENT 'SAP MATNR — FK para dim_material.',
  order_date_id    INTEGER                 COMMENT 'SAP AUDAT como YYYYMMDD — FK para dim_date.',
  vkorg            STRING                  COMMENT 'SAP VKORG — organização de vendas.',
  vtweg            STRING                  COMMENT 'SAP VTWEG — canal de distribuição.',
  spart            STRING                  COMMENT 'SAP SPART — divisão.',
  vkbur            STRING                  COMMENT 'SAP VKBUR — escritório de vendas.',
  quantity         DECIMAL(18,3)           COMMENT 'SAP MENGE — quantidade do item.',
  uom              STRING                  COMMENT 'SAP MEINS — unidade de medida.',
  net_value        DECIMAL(18,2)           COMMENT 'SAP NETWR — valor líquido do item na moeda original.',
  currency         STRING                  COMMENT 'SAP WAERK — moeda original (BRL, USD, EUR).',
  net_value_brl    DECIMAL(18,2)           COMMENT 'Valor convertido para BRL via TCURR (KURST=M). NULL se taxa não encontrada.',
  exchange_rate    DECIMAL(18,5)           COMMENT 'SAP UKURS — taxa usada na conversão. NULL para ordens BRL.',
  rate_date        DATE                    COMMENT 'valid_from da taxa usada. NULL para ordens BRL.',
  _gold_updated_at TIMESTAMP     NOT NULL  COMMENT 'Última atualização na gold.'
)
USING DELTA
COMMENT 'fact_sales_order_item — Itens de ordens de venda com valor convertido para BRL. Fonte: silver.vbap + vbak + tcurr'
""")

# COMMAND ----------

# TCURR: taxas padrão (KURST='M') para conversão em BRL
# Broadcast porque são poucos registros (~1080 linhas)
tcurr = F.broadcast(
    spark.table(f"{catalog}.silver.tcurr")
    .filter(
        (F.col("kurst") == "M") &
        (F.col("tcurr") == "BRL") &
        (F.col("is_deleted") == False) &
        F.col("valid_from").isNotNull()
    )
    .select(
        F.col("fcurr").alias("t_fcurr"),
        F.col("valid_from").alias("t_valid_from"),
        F.col("ukurs").alias("t_ukurs"),
    )
)

# Itens com data da ordem do cabeçalho
items = (
    spark.table(f"{catalog}.silver.vbap")
    .filter(F.col("is_deleted") == False)
    .select("mandt", "vbeln", "posnr", "matnr", "menge", "meins", "netwr", "waerk")
)

headers = (
    spark.table(f"{catalog}.silver.vbak")
    .filter(F.col("is_deleted") == False)
    .select("mandt", "vbeln", "kunnr", "audat", "vkorg", "vtweg", "spart", "vkbur")
)

items_full = items.join(headers, ["mandt", "vbeln"])

# COMMAND ----------

# Join "as-of": para cada item, busca taxas onde fcurr=waerk e valid_from <= audat.
# Depois mantém a mais recente (maior valid_from) por item.
rate_window = Window.partitionBy("mandt", "vbeln", "posnr").orderBy(
    F.col("t_valid_from").desc_nulls_last()
)

fact_df = (
    items_full.alias("i")
    .join(
        tcurr,
        (F.col("i.waerk") == F.col("t_fcurr")) &
        (F.col("t_valid_from") <= F.col("i.audat")),
        "left"
    )
    .withColumn("_rate_rank", F.row_number().over(rate_window))
    .filter(F.col("_rate_rank") == 1)
    .drop("_rate_rank", "t_fcurr")
    # Conversão BRL:
    # waerk=BRL → net_value_brl = netwr (sem conversão)
    # outra moeda + taxa encontrada → netwr * ukurs
    # outra moeda + taxa não encontrada → NULL (nunca inventar uma taxa)
    .withColumn("net_value_brl",
        F.when(F.col("waerk") == "BRL", F.col("netwr"))
        .when(F.col("t_ukurs").isNotNull(),
            (F.col("netwr") * F.col("t_ukurs")).cast("DECIMAL(18,2)")
        )
        .otherwise(F.lit(None).cast("DECIMAL(18,2)"))
    )
    .withColumn("order_date_id",
        F.when(F.col("audat").isNotNull(),
            F.date_format(F.col("audat"), "yyyyMMdd").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .select(
        "mandt", "vbeln", "posnr",
        F.col("kunnr").alias("customer_id"),
        F.col("matnr").alias("material_id"),
        "order_date_id",
        "vkorg", "vtweg", "spart", "vkbur",
        F.col("menge").alias("quantity"),
        F.col("meins").alias("uom"),
        F.col("netwr").alias("net_value"),
        F.col("waerk").alias("currency"),
        "net_value_brl",
        F.col("t_ukurs").alias("exchange_rate"),
        F.col("t_valid_from").alias("rate_date"),
        "_gold_updated_at",
    )
)

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.gold.fact_sales_order_item")

(
    target.alias("tgt")
    .merge(
        fact_df.alias("src"),
        "tgt.mandt = src.mandt AND tgt.vbeln = src.vbeln AND tgt.posnr = src.posnr"
    )
    .whenMatchedUpdate(set={c: f"src.{c}" for c in fact_df.columns})
    .whenNotMatchedInsert(values={c: f"src.{c}" for c in fact_df.columns})
    .execute()
)

print(f"MERGE concluído → {catalog}.gold.fact_sales_order_item")
spark.table(f"{catalog}.gold.fact_sales_order_item").select(
    F.count("*").alias("total_items"),
    F.sum("net_value").alias("total_net_value"),
    F.sum("net_value_brl").alias("total_net_value_brl"),
    F.sum((F.col("net_value_brl").isNull() & (F.col("currency") != "BRL")).cast("int")).alias("items_missing_rate"),
).show()
