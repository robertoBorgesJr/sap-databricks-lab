# sap-databricks-lab

Projeto de estudo que simula a integração de dados SAP com um lakehouse no Azure Databricks, sem acesso a um ambiente SAP real. Cobre quatro partes:

1. **Gerador de dados sintéticos** com "cara de SAP"
2. **Simulador de CDC** (arquivos de delta caindo em uma landing zone)
3. **Pipeline Medallion** (bronze → silver → gold)
4. **Simulação do caminho SAP BDC** com Delta Sharing

> Objetivo principal: preparação para entrevistas técnicas. Cada decisão arquitetural deve ser justificável.

---

## Visão geral da arquitetura

```
Gerador Python
(KNA1, MARA, MAKT,         Landing Zone           Pipeline Medallion
 VBAK, VBAP, BKPF,  --->  (ADLS Volume)   --->   Bronze (Auto Loader)
 BSEG, TCURR)              <TABELA>/               Silver (MERGE)
                            batch=<N>/              Gold (dimensões + fatos)
                            part-*.parquet
                                                    sap_bdc_sim_<env>
                                               ---> Delta Sharing
                                                    (data products)
```

**Infraestrutura:** Azure Databricks (pay-as-you-go) com Unity Catalog. Deploy via Databricks Asset Bundles. Infraestrutura provisionada com Terraform.

---

## Pré-requisitos

| Ferramenta | Versão mínima |
|---|---|
| Python | 3.11+ |
| pip / uv | — |
| Databricks CLI | com perfil `DEFAULT` configurado |
| Terraform | para provisionamento inicial |

```bash
pip install -r requirements.txt
```

Dependências: `faker>=24.0.0`, `pyarrow>=15.0.0`, `pytest>=8.0.0`.

Nunca coloque credenciais no repositório. Use perfis do Databricks CLI.

---

## Estrutura do repositório

```
generator/            # gerador sintético e simulador de CDC (Python puro)
  tables/             # uma definição por tabela SAP
  cdc.py              # geração de lotes de delta
  upload.py           # envio dos arquivos para o volume da landing
src/
  bronze/             # ingestão com Auto Loader
  silver/             # MERGE, tipagem e conversões SAP
  gold/               # modelo dimensional
  sharing/            # simulação BDC / Delta Sharing
contracts/            # contrato de dados por tabela (YAML)
tests/                # pytest
resources/            # jobs e pipelines do bundle
infra/
  modules/sap_lab_infra/  # módulo Terraform reutilizável
  live/sap_lab/           # configuração live (dev + prod)
docs/
  bdc-vs-extracao.md  # comparativo dos dois caminhos
databricks.yml        # Databricks Asset Bundle
```

---

## Ambientes (Unity Catalog)

Dois ambientes isolados por catálogo. O código nunca hardcoda o nome do catálogo — ele vem da variável `catalog` do bundle.

| Objeto | dev | prod |
|---|---|---|
| Catálogo lakehouse | `sap_lab_dev` | `sap_lab_prod` |
| Catálogo simulador BDC | `sap_bdc_sim_dev` | `sap_bdc_sim_prod` |
| Share | `sap_bdc_share_dev` | `sap_bdc_share_prod` |
| Recipient | `lakehouse_consumer_dev` | `lakehouse_consumer_prod` |

Schemas de `sap_lab_<env>`:

| Schema | Conteúdo |
|---|---|
| `landing` | Volume `sap_raw` — destino dos arquivos do gerador |
| `bronze` | Dado bruto, append-only, tudo string |
| `silver` | Dado tipado, deduplicado, estado atual por chave |
| `gold` | Modelo dimensional |
| `ops` | Checkpoints e schemas do Auto Loader, auditoria |

---

## Parte 1 — Gerador de dados sintéticos

Roda em Python puro (sem Spark, sem Docker). Volumes pequenos por padrão, determinístico via `--seed`.

### Tabelas geradas

| Tabela | Conteúdo | Chave |
|---|---|---|
| `KNA1` | Clientes | MANDT, KUNNR |
| `MARA` | Materiais | MANDT, MATNR |
| `MAKT` | Descrições de material | MANDT, MATNR, SPRAS |
| `VBAK` | Cabeçalho de ordem de venda | MANDT, VBELN |
| `VBAP` | Itens de ordem de venda | MANDT, VBELN, POSNR |
| `BKPF` | Cabeçalho de documento contábil | MANDT, BUKRS, BELNR, GJAHR |
| `BSEG` | Linhas de documento contábil | MANDT, BUKRS, BELNR, GJAHR, BUZEI |
| `TCURR` | Taxas de câmbio | MANDT, KURST, FCURR, TCURR, GDATU |

### Armadilhas SAP simuladas

Toda coluna sai do gerador como **string**, como em um extrato bruto real:

- **MANDT** — mandante `100` (principal) + registros em `200` para exercitar filtros
- **DATS** — string `YYYYMMDD`; data vazia = `00000000`, não nulo
- **TIMS** — string `HHMMSS`
- **Conversão ALPHA** — `KUNNR`/`VBELN` com 10 posições, `MATNR` com 18, `POSNR` com 6
- **Flags** — `X` para verdadeiro, string vazia para falso (`LOEVM`, `LVORM`)
- **Valores monetários** — sempre acompanhados da moeda (`NETWR` + `WAERK`); em `BSEG` o valor é positivo e o sinal vem de `SHKZG` (`S`/`H`)
- **TCURR** — `GDATU` em formato invertido (`99999999 - YYYYMMDD`)
- **Strings vazias** no lugar de nulos; espaços à direita em campos CHAR
- **Contexto Brasil** — `LAND1='BR'`, `STCD1` (CNPJ) e `STCD2` (CPF) fictícios, moeda principal `BRL`, parte das ordens em `USD`/`EUR`
- **Integridade referencial** — toda `VBAP` tem `VBAK`; débitos e créditos de cada `BKPF` fecham em zero

### Uso

```bash
# Carga inicial (seed 42, padrões: 200 clientes, 150 materiais, 300 ordens, 100 docs)
python -m generator full --seed 42 --out ./out

# Parâmetros opcionais
python -m generator full --seed 42 --out ./out \
  --n-customers 200 --n-materials 150 --n-orders 300 --n-docs 100 \
  --format parquet   # ou csv

# Upload para o volume da landing (requer Databricks CLI configurado)
python -m generator upload --catalog sap_lab_dev --out ./out
```

---

## Parte 2 — Simulador de CDC

Imita o que um extrator ODP entregaria: carga inicial (via `full`) e lotes de delta.

Cada registro de delta carrega três colunas técnicas (simplificação do laboratório, inspirada nos campos ODP):

| Coluna | Descrição |
|---|---|
| `_op` | `I` (insert), `U` (update), `D` (delete) |
| `_seq` | Sequência crescente e única por registro |
| `_extracted_at` | Timestamp da extração |

Arquivos gerados: `<TABELA>/batch=<N>/part-*.parquet`

### Cenários cobertos nos lotes

- Mesma chave alterada mais de uma vez no mesmo lote
- Registro inserido e excluído no mesmo lote
- Lote entregue fora de ordem
- Arquivo duplicado (reentrega com `--duplicate-file`)
- Exclusão física (`_op=D`) e exclusão lógica (flag `LOEVM`/`LVORM`)

### Uso

```bash
# Lote 1 com mix padrão (10% insert, 20% update, 5% delete)
python -m generator delta --batch 1 --seed 42 --out ./out

# Mix customizado e reentrega de arquivo
python -m generator delta --batch 2 --mix 15:30:5 --duplicate-file --out ./out
```

---

## Parte 3 — Pipeline Medallion

Deploy pelo bundle: `databricks bundle deploy` (target `dev` por padrão).

### Bronze — Auto Loader

- Lê Parquet da landing zone com `cloudFiles`
- Append-only, tudo string, sem transformação de negócio
- `schemaLocation` e checkpoint por tabela em `/Volumes/sap_lab_<env>/ops/checkpoints/`
- Colunas de auditoria: `_metadata.file_path` e `_ingested_at`
- Trigger `availableNow` (batch incremental)

### Silver — MERGE + conversões SAP

Conversões aplicadas em [`src/silver/transforms.py`](src/silver/transforms.py):

| Tipo SAP | Conversão |
|---|---|
| `YYYYMMDD` | → `DATE`; `00000000` → `NULL` |
| Zeros à esquerda (ALPHA) | Removidos em colunas de negócio; chave original preservada |
| Flags `X`/vazio | → `BOOLEAN` |
| String vazia | → `NULL`; `trim` em CHAR |
| Valores monetários | → `DECIMAL(18,2)`; `BSEG` aplica sinal de `SHKZG` |
| `GDATU` invertido | → `DATE` |

Regras do MERGE:
- Filtro por `MANDT = '100'`
- Deduplicação por chave (maior `_seq`) antes do MERGE
- Idempotente: reprocessar o mesmo lote não altera o resultado
- Exclusões: soft delete com `is_deleted` / `deleted_at`

Ordem de execução (respeitando dependências de chave estrangeira):

```
KNA1, MARA, TCURR  →  MAKT (dep. MARA)  →  VBAK (dep. KNA1)
                   →  VBAP (dep. VBAK + MAKT)
BKPF              →  BSEG (dep. BKPF)
```

### Gold — Modelo dimensional

| Tabela | Conteúdo |
|---|---|
| `dim_customer` | Dimensão cliente (de KNA1) |
| `dim_material` | Dimensão material com descrição PT-BR (MARA + MAKT) |
| `dim_date` | Dimensão data gerada |
| `fact_sales_order_item` | Grão de item de ordem; valor original + convertido para BRL via TCURR |
| `fact_gl_line` | Grão de linha contábil com valor assinado |

Nomes de colunas em inglês (`customer_id`, `net_value`) com comentário Unity Catalog indicando o campo SAP de origem.

### Contratos e testes

Cada tabela tem um contrato em [`contracts/<tabela>.yml`](contracts/) com schema, chave e regras de qualidade.

Testes mínimos por tabela silver:

- Unicidade da chave
- Idempotência do MERGE
- Tratamento de `00000000`
- Exclusões físicas e lógicas
- Lote fora de ordem
- Reconciliação: contagem e soma de `NETWR` batem com o estado final do gerador

```bash
pytest
```

---

## Parte 4 — Simulação SAP BDC + Delta Sharing

Simula o caminho onde o SAP Business Data Cloud é o provedor e publica data products via Delta Sharing — zero-copy, sem pipeline de CDC.

O catálogo `sap_bdc_sim_<env>` faz o papel do SAP BDC:

| Componente | Arquivo |
|---|---|
| Publicação dos data products | [`src/sharing/data_products.py`](src/sharing/data_products.py) |
| Configuração do SHARE e RECIPIENT | [`src/sharing/share_setup.py`](src/sharing/share_setup.py) |
| Leitura via `deltaSharing` | [`src/sharing/consumer_read.py`](src/sharing/consumer_read.py) |
| Join com gold própria | [`src/sharing/gold_enriched.py`](src/sharing/gold_enriched.py) |

Data products publicados: `sales_order`, `customer` — nomes de negócio, sem armadilhas SAP.

Referência oficial: [docs.databricks.com/aws/en/delta-sharing/sap-bdc](https://docs.databricks.com/aws/en/delta-sharing/sap-bdc)

Para a comparação completa entre os dois caminhos, veja [`docs/bdc-vs-extracao.md`](docs/bdc-vs-extracao.md).

---

## Deploy

```bash
# Autenticar (usa perfil DEFAULT do Databricks CLI)
databricks auth login

# Validar o bundle
databricks bundle validate

# Deploy em dev (padrão)
databricks bundle deploy

# Deploy em prod (somente pelo bundle, nunca manualmente)
databricks bundle deploy --target prod

# Executar jobs manualmente (dev)
databricks bundle run bronze_ingest
databricks bundle run silver_transform
databricks bundle run gold_build
databricks bundle run sharing_setup
```

---

## Infraestrutura (Terraform)

Provisionado em [`infra/`](infra/):

- Storage Account ADLS Gen2 (`stsaplab<env>`) — um por ambiente
- External Location + Storage Credential (Access Connector com managed identity)
- Catálogos `sap_lab_<env>` e `sap_bdc_sim_<env>` com schemas e volumes
- Grants para `account users`

```bash
cd infra/live/sap_lab
terraform init
terraform plan
terraform apply
```

> O Access Connector (`ext-access-connector-uc`) é compartilhado com outros domínios do workspace e não é gerenciado aqui.

---

## Regras do projeto

- Nunca fixar o nome do catálogo no código — sempre `${var.catalog}` ou `${var.bdc_catalog}`
- Nunca usar `inferSchema` na bronze
- Nunca usar `overwrite` na silver/gold como atalho para o MERGE
- Nunca subir clusters grandes — usar serverless ou o menor nó disponível
- `prod` só recebe deploy pelo bundle
- Dados 100% fictícios (Faker `pt_BR`)
