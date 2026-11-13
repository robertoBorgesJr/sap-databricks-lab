# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze — Auto Loader Ingest (availableNow)
# MAGIC
# MAGIC Notebook parametrizado: lê **todos** os arquivos Parquet da landing zone SAP
# MAGIC e faz append em `{catalog}.bronze.{table_name}`, preservando todas as colunas
# MAGIC como **string** sem nenhuma interpretação de tipo.
# MAGIC
# MAGIC ### Por que tudo STRING na bronze?
# MAGIC A camada bronze replica fielmente o que saiu do SAP — incluindo zeros à esquerda,
# MAGIC flags 'X'/vazio, datas '00000000' e separadores de vírgula. Converter tipos aqui
# MAGIC destruiria evidência dos dados originais. As conversões SAP são responsabilidade
# MAGIC exclusiva da **silver**.
# MAGIC
# MAGIC ### Parâmetros (injetados pelo bundle job)
# MAGIC | Widget | Exemplo | Descrição |
# MAGIC |--------|---------|-----------|
# MAGIC | `catalog` | `sap_lab_dev` | Catálogo Unity Catalog do target |
# MAGIC | `table_name` | `KNA1` | Nome da tabela SAP em maiúsculas |
# MAGIC
# MAGIC ### Layout de volumes (Unity Catalog)
# MAGIC ```
# MAGIC /Volumes/{catalog}/landing/sap_raw/{TABLE}/          ← arquivos da carga inicial
# MAGIC /Volumes/{catalog}/landing/sap_raw/{TABLE}/batch=N/  ← lotes CDC
# MAGIC /Volumes/{catalog}/ops/checkpoints/{TABLE}/          ← checkpoint do stream
# MAGIC /Volumes/{catalog}/ops/checkpoints/{TABLE}_schema/   ← schema do Auto Loader
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog",    "sap_lab_dev", "Catalog (Unity Catalog)")
dbutils.widgets.text("table_name", "KNA1",        "SAP Table Name (uppercase)")

catalog    = dbutils.widgets.get("catalog").strip()
table_name = dbutils.widgets.get("table_name").strip().upper()

# COMMAND ----------

landing_path    = f"/Volumes/{catalog}/landing/sap_raw/{table_name}/"
schema_location = f"/Volumes/{catalog}/ops/checkpoints/{table_name}_schema"
checkpoint_path = f"/Volumes/{catalog}/ops/checkpoints/{table_name}"
target_table    = f"{catalog}.bronze.{table_name.lower()}"

print(f"Source    : {landing_path}")
print(f"Schema    : {schema_location}")
print(f"Checkpoint: {checkpoint_path}")
print(f"Target    : {target_table}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Verificação prévia: landing não-vazia
# MAGIC
# MAGIC Auto Loader com `inferColumnTypes=false` ainda precisa ler ao menos um arquivo
# MAGIC Parquet para conhecer os nomes das colunas na primeira execução (antes de o schema
# MAGIC ser persistido no `schemaLocation`). Diretório vazio = `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE`.
# MAGIC
# MAGIC A saída antecipada evita falha da task quando os arquivos ainda não foram enviados.

# COMMAND ----------

try:
    # spark.read uses the same access path as Auto Loader (cloud storage reader),
    # which works with Unity Catalog external-storage-backed volumes in serverless.
    # dbutils.fs.ls and os.listdir rely on FUSE/cloud-files connector and may fail
    # in serverless even when storage access is correctly configured.
    # Also check len(columns) > 0: if the directory exists but has no parquet
    # files, Spark may return an empty DataFrame (no exception, 0 columns).
    # That would set _has_files=True and start a stream with no source data,
    # creating an empty Delta table and corrupting the checkpoint state.
    _probe = spark.read.format("parquet").load(landing_path)
    _has_files = len(_probe.columns) > 0
except Exception as _e:
    _msg = str(_e).lower()
    if any(k in _msg for k in ("path does not exist", "no such file", "empty", "cf_empty_dir")):
        _has_files = False
    else:
        raise RuntimeError(
            f"Cannot access landing path {landing_path} — "
            f"check READ VOLUME grant on {catalog}.landing.sap_raw.\n{_e}"
        ) from _e

if not _has_files:
    msg = f"SKIPPED: landing path is empty — {landing_path}. Upload files before running."
    print(msg)
    dbutils.notebook.exit(msg)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        # schemaLocation: obrigatório para Auto Loader em Unity Catalog Volumes.
        # Armazena o schema inferido entre execuções.
        .option("cloudFiles.schemaLocation", schema_location)
        # Nunca inferir tipos: todos os campos chegam como string, exatamente
        # como no extrato SAP bruto.
        .option("cloudFiles.inferColumnTypes", "false")
        # addNewColumns: quando os arquivos CDC adicionam _op/_seq/_extracted_at
        # (colunas ausentes na carga inicial), o Auto Loader expande o schema.
        # Linhas da carga inicial terão NULL nessas colunas — tratado pela silver.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(landing_path)
        # _metadata é uma coluna struct especial do Auto Loader.
        # Extraímos file_path antes do write para auditoria de linhagem.
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
)

# availableNow=True: processa todos os arquivos novos e encerra.
# Equivalente a um job batch incremental — sem cluster sempre ativo.
(
    df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
        .awaitTermination()
)

print(f"Ingestão concluída → {target_table}")
