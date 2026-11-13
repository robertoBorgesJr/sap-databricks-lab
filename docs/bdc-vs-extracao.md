# BDC vs Extração/CDC — dois caminhos para consumir dados SAP no Databricks

## Contexto

Existem dois modelos para levar dados SAP para um lakehouse Databricks:

1. **Extração/CDC** — extrair os dados do SAP, armazená-los em uma landing zone
   e processá-los pelo pipeline Medallion (bronze → silver → gold). É o que as
   Partes 1–3 deste laboratório simulam.

2. **SAP BDC + Delta Sharing** — o SAP Business Data Cloud atua como provedor:
   publica data products curados via Delta Sharing e o Databricks consome
   diretamente, sem cópia. É o que a Parte 4 simula.

---

## Comparativo

| Aspecto | Extração/CDC (Partes 1–3) | BDC + Delta Sharing (Parte 4) |
|---------|--------------------------|-------------------------------|
| **Cópia de dados** | Sim — dado replicado em bronze e silver | Não — zero-copy via signed URLs |
| **Latência** | Depende do intervalo de extração (batch ou near-real-time) | Depende de quando o BDC atualiza o data product (pull-based) |
| **Transformação** | Feita pelo engenheiro de dados (silver/gold) | Feita pelo BDC antes da publicação; consumidor recebe dado já curado |
| **Esquema** | Bruto SAP (ALPHA, DATS, flags X/vazio) → conversão na silver | Já tipado e com nomes de negócio; sem armadilhas SAP |
| **Campos técnicos SAP** | Presentes na bronze e silver (MANDT, _op, _seq, etc.) | Ausentes — o data product expõe apenas atributos de negócio |
| **Histórico completo** | Sim — bronze é append-only; silver mantém deleted_at | Não — o share reflete o estado atual; histórico fica no SAP/BDC |
| **Governança** | Unity Catalog do lakehouse (você controla) | Permissão granular no SHARE; consumer não acessa catálogo BDC |
| **Rastreabilidade** | Linhagem full: arquivo → bronze → silver → gold | Linhagem parcial: você sabe de onde veio o share, não o que aconteceu antes |
| **Custo de infraestrutura** | Cluster para ingestão + armazenamento de todas as camadas | Sem cluster de ingestão; armazenamento apenas para o dado próprio |
| **Dependência de ferramentas SAP** | Alta — precisa de extrator (ODP, SLT, BODS, etc.) | Baixa — só precisa de licença BDC e conectividade de rede |
| **Customização** | Total — você decide o que extrair, como transformar e quando | Limitada — você consome o que o BDC publica; filtros e joins são do seu lado |
| **Casos de uso ideais** | Analytics profundo, histórico, qualidade avançada, dados não cobertos pelo BDC | Enriquecimento rápido com mestre SAP, integração sem pipeline de CDC |

---

## Quando usar cada abordagem

### Use Extração/CDC quando:
- Precisar de histórico completo de mudanças (tipo 2 SCD, auditoria, replay).
- O dado necessário não está disponível como data product no BDC.
- Precisar de transformações muito específicas que o BDC não entrega.
- O volume de dados justifica a cópia local para performance de query.

### Use BDC + Delta Sharing quando:
- O objetivo é enriquecer transações próprias com mestre SAP (cliente, material, fornecedor).
- Latência de algumas horas é aceitável.
- Quer reduzir a complexidade de pipeline — sem extrator, sem bronze de CDC.
- O dado já é publicado pelo BDC com a qualidade necessária.

### Combinação (padrão recomendado em produção):
- **Bronze/silver próprios** para tabelas transacionais (VBAK, VBAP, BKPF, BSEG) onde
  histórico e latência importam.
- **Delta Sharing** para tabelas mestre (KNA1, MARA, LFA1) onde o BDC entrega o estado
  atual e você não precisa do histórico de mudanças.

---

## Implementação neste laboratório

| Componente | Arquivo |
|------------|---------|
| Publicação dos data products | [src/sharing/data_products.py](../src/sharing/data_products.py) |
| Configuração do SHARE e RECIPIENT | [src/sharing/share_setup.py](../src/sharing/share_setup.py) |
| Leitura via deltaSharing | [src/sharing/consumer_read.py](../src/sharing/consumer_read.py) |
| Join com gold própria | [src/sharing/gold_enriched.py](../src/sharing/gold_enriched.py) |
| Job do bundle | [resources/sharing_job.yml](../resources/sharing_job.yml) |

---

## Referências

- Delta Sharing + SAP BDC: https://docs.databricks.com/aws/en/delta-sharing/sap-bdc
- Open sharing (single metastore): https://docs.databricks.com/en/delta-sharing/read-data-open.html
- Protocolo Delta Sharing (open source): https://github.com/delta-io/delta-sharing
