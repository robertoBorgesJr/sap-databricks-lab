# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## BDC Simulator — Share & Recipient Setup
# MAGIC
# MAGIC Configura a infraestrutura de Delta Sharing no lado provedor:
# MAGIC cria o SHARE com os data products e o RECIPIENT para o consumidor.
# MAGIC
# MAGIC **Caminho adotado:** open sharing (single-metastore). O recipient recebe
# MAGIC um link de ativação e baixa um arquivo JSON de perfil com endpoint + bearer
# MAGIC token. Este é o caminho a usar quando provedor e consumidor estão no mesmo
# MAGIC metastore Unity Catalog, ou quando o consumidor usa o conector `deltaSharing`
# MAGIC diretamente (sem catálogo montado).
# MAGIC
# MAGIC **Pré-requisito:** o notebook `data_products` deve ter sido executado antes —
# MAGIC o SHARE só pode incluir tabelas já existentes.
# MAGIC
# MAGIC ### Objetos criados
# MAGIC | Objeto | Nome |
# MAGIC |--------|------|
# MAGIC | SHARE | `sap_bdc_share_<env>` |
# MAGIC | RECIPIENT | `lakehouse_consumer_<env>` |
# MAGIC
# MAGIC ### Passo manual após execução
# MAGIC 1. Copie o `activation_link` exibido ao final deste notebook.
# MAGIC 2. Baixe o perfil via curl ou browser:
# MAGIC    ```bash
# MAGIC    curl -s "<activation_link>" > profile.json
# MAGIC    ```
# MAGIC 3. Faça upload para o volume do lakehouse:
# MAGIC    ```bash
# MAGIC    databricks fs cp profile.json \
# MAGIC      /Volumes/sap_lab_<env>/ops/sharing_profiles/profile.json
# MAGIC    ```
# MAGIC    Ou: **Data > Volumes > sap_lab_<env>.ops.sharing_profiles > Upload**.
# MAGIC 4. Execute os notebooks `consumer_read` e `gold_enriched`.
# MAGIC
# MAGIC > O link de ativação é de uso único. Se precisar regenerar, recrie o recipient
# MAGIC > (`DROP RECIPIENT ... ; CREATE RECIPIENT ...`).

# COMMAND ----------

dbutils.widgets.text("bdc_catalog", "sap_bdc_sim_dev", "Catálogo BDC simulado")
bdc_catalog = dbutils.widgets.get("bdc_catalog").strip()

# Deriva env do nome do catálogo: "sap_bdc_sim_dev" → "dev"
env = bdc_catalog.rsplit("_", 1)[-1]
share_name = f"sap_bdc_share_{env}"
recipient_name = f"lakehouse_consumer_{env}"

print(f"env={env}  share={share_name}  recipient={recipient_name}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Criar SHARE

# COMMAND ----------

spark.sql(f"""
  CREATE SHARE IF NOT EXISTS {share_name}
  COMMENT 'Simulação do SAP BDC — data products curados publicados via Delta Sharing.'
""")
print(f"SHARE {share_name}: OK")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Adicionar tabelas ao SHARE (idempotente)

# COMMAND ----------

def add_table_to_share(share: str, full_table: str) -> None:
    """Adiciona tabela ao share; ignora se já estiver presente."""
    try:
        spark.sql(f"ALTER SHARE {share} ADD TABLE {full_table}")
        print(f"  + {full_table} adicionada ao share.")
    except Exception as exc:
        msg = str(exc).lower()
        if "already" in msg or "already added" in msg:
            print(f"  ~ {full_table} já estava no share (ignorado).")
        else:
            raise

add_table_to_share(share_name, f"{bdc_catalog}.data_products.sales_order")
add_table_to_share(share_name, f"{bdc_catalog}.data_products.customer_master")

# COMMAND ----------

print(f"\nConteúdo atual do SHARE {share_name}:")
spark.sql(f"SHOW ALL IN SHARE {share_name}").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Criar RECIPIENT (open sharing)

# COMMAND ----------

spark.sql(f"""
  CREATE RECIPIENT IF NOT EXISTS {recipient_name}
  COMMENT 'Consumidor da simulação BDC — open sharing, single-metastore.'
""")
print(f"RECIPIENT {recipient_name}: OK")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Conceder acesso ao recipient

# COMMAND ----------

spark.sql(f"GRANT SELECT ON SHARE {share_name} TO RECIPIENT {recipient_name}")
print(f"SELECT em {share_name} concedido a {recipient_name}.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Exibir link de ativação
# MAGIC
# MAGIC Copie o valor de `activation_link` abaixo e siga as instruções no cabeçalho
# MAGIC deste notebook para baixar o perfil JSON e armazená-lo no volume.

# COMMAND ----------

desc = spark.sql(f"DESCRIBE RECIPIENT {recipient_name}")
display(desc)

# COMMAND ----------
# MAGIC %md
# MAGIC > **Lembrete:** o link de ativação expira após o primeiro uso. Se o perfil
# MAGIC > já foi baixado anteriormente, o campo `tokens_activated` aparecerá como `true`
# MAGIC > e o link não estará mais disponível. Neste caso, não é necessário baixar novamente —
# MAGIC > use o perfil já armazenado no volume.
