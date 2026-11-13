# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold — Enriquecimento com Dado Compartilhado (BDC → Lakehouse)
# MAGIC
# MAGIC Demonstra o padrão central de consumo do Delta Sharing: dado mestre vindo
# MAGIC do "BDC simulado" (zero-copy, read-only) combinado com tabelas transacionais
# MAGIC próprias do lakehouse.
# MAGIC
# MAGIC ### Lógica
# MAGIC | Dado | Origem | Acesso |
# MAGIC |------|--------|--------|
# MAGIC | `customer_master` | BDC share | deltaSharing (read-only, zero-copy) |
# MAGIC | `fact_sales_order_item` | própria gold | Unity Catalog (read-write) |
# MAGIC
# MAGIC O join por `customer_id` enriquece cada item de ordem com os atributos
# MAGIC de cliente entregues pelo BDC. No cenário real, seria o mestre oficial
# MAGIC vindo do SAP — sem necessidade de extrair, copiar ou manter uma réplica.
# MAGIC
# MAGIC ### Resultado
# MAGIC O DataFrame `enriched` não é gravado em uma tabela Delta permanente: o
# MAGIC objetivo deste notebook é demonstrar a leitura combinada. Em produção,
# MAGIC este padrão poderia alimentar um relatório, um modelo ML ou uma view gold.

# COMMAND ----------

dbutils.widgets.text("catalog", "sap_lab_dev", "Catálogo do lakehouse")
dbutils.widgets.text("profile_path", "", "Caminho do perfil JSON (vazio = padrão do volume)")
catalog = dbutils.widgets.get("catalog").strip()
profile_path = dbutils.widgets.get("profile_path").strip()

env = catalog.rsplit("_", 1)[-1]
share_name = f"sap_bdc_share_{env}"

if not profile_path:
    profile_path = f"/Volumes/{catalog}/ops/sharing_profiles/profile.json"

print(f"share={share_name}")
print(f"profile_path={profile_path}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Ler `customer_master` do share (zero-copy)

# COMMAND ----------

bdc_customers = (
    spark.read.format("deltaSharing")
    .load(f"{profile_path}#{share_name}.data_products.customer_master")
    .select(
        "customer_id",
        "customer_name",
        "country",
        "city",
        "tax_id_cnpj",
        "customer_status",
    )
)

print(f"customer_master (do share): {bdc_customers.count()} clientes")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Ler `fact_sales_order_item` do próprio lakehouse

# COMMAND ----------

own_sales = spark.table(f"{catalog}.gold.fact_sales_order_item")
print(f"fact_sales_order_item (própria gold): {own_sales.count()} itens")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Join: ordens enriquecidas com dado do cliente do BDC

# COMMAND ----------

enriched = (
    own_sales
    .join(bdc_customers, "customer_id", "left")
    .withColumn("_enriched_at", F.current_timestamp())
    .select(
        "vbeln",
        "posnr",
        "order_date_id",
        "customer_id",
        "customer_name",       # vem do BDC share
        "country",             # vem do BDC share
        "city",                # vem do BDC share
        "material_id",
        "quantity",
        "uom",
        "net_value",
        "currency",
        "net_value_brl",
        "sales_org",
        "_enriched_at",
    )
)

print(f"enriched: {enriched.count()} linhas")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Análise: ordens por país e moeda

# COMMAND ----------

enriched.groupBy("country", "currency").agg(
    F.count("*").alias("order_items"),
    F.countDistinct("vbeln").alias("orders"),
    F.round(F.sum("net_value_brl"), 2).alias("total_brl"),
).orderBy(F.desc("total_brl")).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Reconciliação: clientes sem match no share

# COMMAND ----------

null_customer = enriched.filter(F.col("customer_name").isNull()).count()
total = enriched.count()
pct = 100.0 * null_customer / max(total, 1)
print(f"Itens sem match no share: {null_customer}/{total} ({pct:.1f}%)")
# Esperado: 0% — todos os customer_id da gold devem existir no customer_master do share.

# COMMAND ----------
# MAGIC %md
# MAGIC ### Resumo do padrão demonstrado
# MAGIC
# MAGIC | Aspecto | Valor |
# MAGIC |---------|-------|
# MAGIC | Cópia de dado? | Não — `bdc_customers` lido on-the-fly via signed URLs |
# MAGIC | Latência de atualização | Depende de quando `data_products` foi re-executado (pull-based) |
# MAGIC | Governança | Permissão granular por tabela no SHARE; consumer não acessa catálogo BDC |
# MAGIC | Escalabilidade | O share expõe o Delta Log completo — Consumer lê apenas o que precisa |
