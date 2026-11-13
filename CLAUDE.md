# CLAUDE.md — sap-databricks-lab (projeto simulado SAP → Databricks)

## Objetivo

Projeto de estudo que simula a integração de dados SAP com um lakehouse no Azure Databricks, sem acesso a um ambiente SAP real. Tem quatro partes:

1. Gerador de dados sintéticos com "cara de SAP"
2. Simulação de CDC (arquivos de delta caindo em uma landing zone)
3. Pipeline Medallion (bronze → silver → gold)
4. Simulação do caminho SAP BDC com Delta Sharing

O dono do projeto é engenheiro de dados sênior em SQL, PySpark, Delta Lake e Unity Catalog, e está aprendendo o domínio SAP. O projeto serve de preparação para entrevistas, então ele precisa entender e defender cada decisão.

## Como trabalhar neste repositório

- Trabalhe em incrementos pequenos: uma tabela ou uma regra por mudança. Não gere o projeto inteiro de uma vez.
- Antes de implementar, apresente um plano curto e espere aprovação. Se o OpenSpec estiver instalado (`openspec/`), cada mudança começa como uma proposta lá.
- Ao terminar, explique em poucas linhas o que foi feito, por que, e qual conceito SAP ou Databricks está envolvido.
- Para APIs e recursos Databricks, confira a documentação oficial (docs.databricks.com ou learn.microsoft.com/azure/databricks) antes de usar e cite a URL. Se não conseguir confirmar algo, diga isso explicitamente em vez de adivinhar.
- Use as skills oficiais da Databricks quando disponíveis (`databricks aitools install`).
- Não declare uma tarefa concluída sem rodar os testes e mostrar o resultado.
- Idioma: explicações e documentação em português; código, nomes de colunas técnicas e commits em inglês.

## Ambiente

- Máquina local com 8 GB de RAM: o gerador roda em Python puro (sem Spark local, sem Docker). Volumes de dados pequenos por padrão (milhares de linhas, não milhões).
- Databricks: workspace com Unity Catalog. **[Azure Databricks pay-as-you-go]**
- Deploy com Databricks Asset Bundles (`databricks.yml`), nomeados "Declarative Automation Bundles" na documentação mais recente.
- Python 3.11+, `uv` ou `pip` com `requirements.txt`. Nunca coloque credenciais no repositório; use perfis do Databricks CLI.

## Estrutura do repositório

```
generator/            # gerador sintético e simulador de CDC (Python puro)
  tables/             # uma definição por tabela SAP
  cdc.py              # geração de lotes de delta
src/
  bronze/             # ingestão com Auto Loader
  silver/             # MERGE, tipagem e limpeza SAP
  gold/               # modelo dimensional
  sharing/            # simulação BDC / Delta Sharing
contracts/            # contrato de dados por tabela (YAML)
tests/                # pytest
resources/            # jobs e pipelines do bundle
openspec/             # propostas de mudança (opcional)
databricks.yml
```

## Unity Catalog

Dois ambientes, `dev` e `prod`, com a mesma estrutura. O ambiente é sempre o sufixo do catálogo.

| Objeto | dev | prod |
|---|---|---|
| Catálogo do lakehouse | `sap_lab_dev` | `sap_lab_prod` |
| Catálogo "provedor" BDC simulado | `sap_bdc_sim_dev` | `sap_bdc_sim_prod` |
| Catálogo montado a partir do share | `sap_bdc_shared_dev` | `sap_bdc_shared_prod` |
| Share | `sap_bdc_share_dev` | `sap_bdc_share_prod` |
| Recipient | `lakehouse_consumer_dev` | `lakehouse_consumer_prod` |

Schemas de `sap_lab_<env>`:

| Schema | Conteúdo |
|---|---|
| `landing` | Volume `sap_raw` com os arquivos do gerador: `/Volumes/sap_lab_<env>/landing/sap_raw/<TABELA>/` |
| `bronze` | Dado bruto, append-only, tudo string |
| `silver` | Dado tipado, deduplicado, estado atual por chave |
| `gold` | Modelo dimensional |
| `ops` | Checkpoints e schemas do Auto Loader (volume `checkpoints`), tabelas de auditoria e resultados de qualidade |

Schema de `sap_bdc_sim_<env>`: `data_products`.

Regras:

- Nunca fixar o nome do catálogo no código. Ele vem da variável `catalog` do bundle, definida por target (`dev`, `prod`) no `databricks.yml`.
- Schemas têm o mesmo nome nos dois ambientes; só o catálogo muda.
- `prod` só recebe deploy pelo bundle, nunca por execução manual de notebook.

Tabelas bronze e silver mantêm o nome SAP em minúsculas (`vbak`, `kna1`). Tabelas gold usam nomes de negócio em inglês (`fact_sales_order_item`).

## Parte 1 — Gerador de dados sintéticos

### Tabelas

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

### Armadilhas SAP que o gerador deve reproduzir

Estas são o ponto central do laboratório. Toda coluna sai do gerador como **string**, como em um extrato bruto.

- **MANDT**: todo registro tem mandante. Gerar `100` (principal) e alguns registros em `200` para exercitar o filtro.
- **Datas (DATS)**: string `YYYYMMDD`. Data vazia é `00000000`, não nulo.
- **Horas (TIMS)**: string `HHMMSS`.
- **Zeros à esquerda (conversão ALPHA)**: `KUNNR` e `VBELN` com 10 posições, `MATNR` com 18, `POSNR` com 6 (`000010`, `000020`...). Valores alfanuméricos não recebem zeros.
- **Flags**: `X` para verdadeiro, string vazia para falso. Ex.: `LOEVM` (cliente marcado para exclusão), `LVORM` (material).
- **Valores monetários**: sempre acompanhados da moeda (`NETWR` + `WAERK`). Em `BSEG`, o valor é sempre positivo e o sinal vem de `SHKZG` (`S` débito, `H` crédito).
- **TCURR**: `GDATU` em formato invertido (`99999999 - YYYYMMDD`).
- **Strings vazias no lugar de nulos**, e espaços à direita em campos CHAR.
- **Brasil**: `LAND1 = 'BR'`, `STCD1` (CNPJ) e `STCD2` (CPF) com valores fictícios, moeda principal `BRL`, com parte das ordens em `USD` e `EUR`.

### Regras do gerador

- Determinístico: mesma `--seed`, mesma saída.
- Integridade referencial: toda `VBAP` tem `VBAK`; todo `KUNNR` e `MATNR` existem nos mestres; débitos e créditos de cada documento `BKPF` fecham em zero.
- Dados 100% fictícios (Faker `pt_BR`). Nunca usar dados reais.
- Saída em Parquet, com opção CSV.
- CLI: `python -m generator full --seed 42 --out ./out` para a carga inicial.

## Parte 2 — Simulação de CDC

Imita o que um extrator ODP entregaria: uma carga inicial e depois lotes de delta.

- `python -m generator delta --batch N` gera o lote N com inserções, alterações e exclusões sobre o estado anterior.
- Cada registro de delta carrega três colunas técnicas:
  - `_op`: `I`, `U` ou `D`
  - `_seq`: sequência crescente e única por registro
  - `_extracted_at`: timestamp da extração
- Estas colunas são uma simplificação própria do laboratório, inspirada nos campos de delta do ODP. Ao documentar, deixar claro que os nomes reais dependem da ferramenta de extração.
- Cenários obrigatórios nos lotes:
  - Mesma chave alterada mais de uma vez no mesmo lote
  - Registro inserido e excluído no mesmo lote
  - Lote entregue fora de ordem
  - Arquivo duplicado (reentrega)
  - Exclusão física (`_op = D`) e exclusão lógica (flag `LOEVM`/`LVORM`)
- Arquivos: `<TABELA>/batch=<N>/part-*.parquet`, enviados para o volume da landing.

## Parte 3 — Pipeline Medallion

### Bronze

- Auto Loader (`cloudFiles`) lendo o volume da landing, com `schemaLocation` e checkpoint por tabela.
- Append-only, tudo como string, sem transformação de negócio.
- Colunas de auditoria: arquivo de origem (`_metadata.file_path`) e `_ingested_at`.

### Silver

- Tabelas referenciadas sempre como `{catalog}.<schema>.<tabela>`.
- Filtrar pelo mandante configurado (`MANDT = '100'`).
- Deduplicar por chave, mantendo o maior `_seq`, antes do MERGE.
- `MERGE INTO` idempotente: reprocessar o mesmo lote não altera o resultado.
- Exclusões: aplicar `_op = D` como soft delete (`is_deleted`, `deleted_at`), preservando o histórico.
- Conversões SAP:
  - `YYYYMMDD` → `DATE`; `00000000` → `NULL`
  - Remover zeros à esquerda em colunas de negócio derivadas, mantendo a chave original
  - `X`/vazio → `BOOLEAN`
  - String vazia → `NULL`; `trim` em CHAR
  - Valores → `DECIMAL(18,2)`; em `BSEG`, aplicar o sinal de `SHKZG`
  - `GDATU` invertido → `DATE`
- Uma alternativa a avaliar depois do MERGE manual são os fluxos de CDC do Lakeflow Declarative Pipelines. Confirmar a API atual na documentação antes de usar.

### Gold

- `dim_customer`, `dim_material` (com descrição em português da `MAKT`), `dim_date`
- `fact_sales_order_item`: grão de item de ordem, com valor original e valor convertido para BRL via `TCURR`
- `fact_gl_line`: grão de linha contábil, com valor assinado
- Nomes de colunas de negócio em inglês (`customer_id`, `net_value`), com comentário no Unity Catalog indicando o campo SAP de origem.

### Contratos e testes

- Cada tabela tem um contrato em `contracts/<tabela>.yml`: schema, chave, regras de qualidade. O contrato é o critério de aceite da mudança.
- Lógica de transformação em funções puras e testáveis, separadas da leitura e escrita.
- Testes mínimos por tabela silver: unicidade da chave, idempotência do MERGE, tratamento de `00000000`, exclusões, lote fora de ordem.
- Teste de reconciliação: contagem e soma de `NETWR` da silver batem com o estado final esperado do gerador.

## Parte 4 — Simulação do caminho BDC (Delta Sharing)

No cenário real, o SAP Business Data Cloud é o provedor e publica data products que o Databricks consome via Delta Sharing, sem cópia. Aqui, o catálogo `sap_bdc_sim_<env>` faz o papel do SAP.

- Publicar em `sap_bdc_sim_<env>.data_products` dois ou três "data products" já curados (ex.: `sales_order`, `customer`), com nomes de negócio, como o BDC entregaria. São diferentes das tabelas brutas da Parte 1.
- Criar um `SHARE` com essas tabelas e um `RECIPIENT`.
- Lado consumidor, conforme o ambiente disponível:
  - **Dois metastores**: compartilhamento Databricks-to-Databricks e catálogo montado a partir do share.
  - **Um metastore só**: recipient de open sharing (token) e leitura com o conector `delta-sharing`.
- Confirmar na documentação oficial qual opção o ambiente suporta antes de implementar.
- Praticar: permissões (`USE PROVIDER`, `SELECT` no catálogo montado), leitura somente-leitura, e combinar o dado compartilhado com tabelas próprias na gold.
- Documentar em `docs/bdc-vs-extracao.md` a comparação entre os dois caminhos: zero-copy via share contra extração/CDC com bronze própria.
- Referência: https://docs.databricks.com/aws/en/delta-sharing/sap-bdc

## Não fazer

- Não inventar nomes de campos ou tabelas SAP. Na dúvida, perguntar.
- Não usar `inferSchema` para "corrigir" tipos na bronze.
- Não usar `overwrite` na silver ou gold como atalho para o MERGE.
- Não subir clusters grandes: usar serverless ou o menor nó disponível.
- Não adicionar ferramentas novas (dbt, Airflow, Kafka) sem pedir.
