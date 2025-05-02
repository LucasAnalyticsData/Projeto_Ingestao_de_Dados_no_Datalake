# 🧠 Guia Explicativo: Boas Práticas na Camada Bronze

Este documento reúne as boas práticas aplicadas na **Camada Bronze** de um pipeline de dados moderno, com foco em **escalabilidade**, **performance**, **controle de schema** e **economia de recursos em nuvem**.

---

## 🔷 Camada Bronze (Ingestão de Dados Brutos)

### ✅ Função principal:
- **Captura** dados brutos de múltiplas fontes (arquivos CSV, APIs, sistemas legados).
- **Preserva a fidelidade dos dados**, sem agregações ou transformações analíticas.
- Serve como **ponto de rastreabilidade e auditoria** dentro do Data Lakehouse.

---

## 🛠️ Técnicas e Melhorias Implementadas:

| Técnica                                | Justificativa Técnica                                                                 |
|----------------------------------------|----------------------------------------------------------------------------------------|
| 🔁 **Auto Loader com `trigger(once)`** | Ingestão incremental, com leitura eficiente e econômica de arquivos novos             |
| 🧹 **`dropDuplicates()` + `na.drop()`**| Elimina duplicidades e registros nulos, melhorando a qualidade desde a origem         |
| 📅 **Particionamento por data**        | Organiza os dados no disco para melhorar performance de leitura                       |
| 🧬 **Delta Lake com schema evolution** | Permite evolução segura do schema com histórico e tolerância a mudanças estruturais   |
| 📌 **Checkpointing**                   | Garante recuperação e consistência mesmo em falhas ou reprocessamentos                |

---

## 1️⃣ Leitura Eficiente com Auto Loader

### ❌ Antes: (forma tradicional)
```python
spark.read.format("csv").load("caminho/dados")
```

🔍 **Problemas desta abordagem:**
- Releitura de todos os arquivos existentes a cada execução.
- Baixa escalabilidade com crescimento do volume de dados.
- Ausência de gerenciamento de metadados ou versionamento.

---

### ✅ Solução Moderna: Structured Streaming + Auto Loader

```python
df_bronze = (
    spark.readStream
        .format("cloudFiles")  # Auto Loader da Databricks otimizado para arquivos em nuvem
        .option("cloudFiles.format", "csv")  # Define o formato de entrada
        .option("cloudFiles.schemaLocation", schema_path)  # Caminho para armazenar e versionar o schema
        .option("cloudFiles.inferColumnTypes", "true")  # Detecta automaticamente tipos como Integer, Date, etc.
        .load(source_path)  # Caminho onde os arquivos são depositados (armazenamento externo/nuvem)
)
```

### 🔍 Explicação técnica linha por linha:
- `readStream`: ativa o modo de leitura contínua (Structured Streaming).
- `"cloudFiles"`: usa Auto Loader, que monitora novas adições no diretório de forma eficiente.
- `"csv"`: permite lidar com arquivos legados e fontes comuns de ingestão inicial.
- `schemaLocation`: grava a estrutura inferida para manter consistência entre execuções.
- `inferColumnTypes`: evita que tudo seja lido como `String`, gerando schemas ricos.
- `load(source_path)`: inicia a leitura incremental baseada em novos arquivos.

---

## 2️⃣ Escrita em Delta com Particionamento e Trigger Controlado

```python
df_final.writeStream     .format("delta")  # Escrita no formato Delta Lake, com ACID e versionamento nativo
    .partitionBy("data_carga")  # Cria subpastas por data, otimizando futuras consultas
    .outputMode("append")  # Adiciona novos dados sem sobrescrever os existentes
    .option("checkpointLocation", checkpoint_path)  # Armazena estado para recuperação e controle de duplicações
    .trigger(once=True)  # Executa como um batch controlado (ideal para orquestrações com tempo e custo fixos)
    .start(bronze_path)  # Caminho de saída no Data Lake (camada Bronze)
```

### 💡 Justificativas:
- `format("delta")`: permite escrita com confiabilidade transacional (ACID) e suporte a `schema evolution`.
- `partitionBy("data_carga")`: reduz o custo de leitura e escrita com pushdown predicates e menor I/O.
- `outputMode("append")`: evita reprocessamentos e mantém o histórico dos dados ingeridos.
- `trigger(once=True)`: ideal para pipelines por lote controlado, com menor custo e sem necessidade de cluster ativo contínuo.
- `checkpointLocation`: essencial para tolerância a falhas e controle de duplicidade nos dados ingeridos.

---

## 3️⃣ Controle de Schema + Qualidade Inicial

```python
df_limpado = df.dropDuplicates().na.drop()
```

### ✅ Por que aplicar logo na Bronze?
- **`dropDuplicates()`**: evita registros repetidos que inflacionam os dados e distorcem métricas futuras.
- **`na.drop()`**: remove registros com campos nulos, especialmente em chaves críticas (como IDs e datas).
- Promove **qualidade mínima** mesmo em dados brutos, prevenindo propagação de sujeira até Silver/Gold.

---

## ✅ Tabela Resumo

| Estratégia                           | Valor para o Pipeline                                                              |
|-------------------------------------|------------------------------------------------------------------------------------|
| Auto Loader (`cloudFiles`)          | Leitura incremental de novos arquivos com alta performance                        |
| Trigger Once                        | Economia de recursos e controle sobre execuções                                   |
| Delta Lake                          | Segurança transacional, versionamento e auditoria                                 |
| PartitionBy (`data_carga`)          | Otimização de leitura, compressão e pushdown                                      |
| Checkpoint                          | Resiliência a falhas, controle de estado entre execuções                          |
| Schema Evolution (`mergeSchema`)    | Tolerância a alterações sem quebras estruturais                                   |
| Qualidade (`dropDuplicates`, `na`)  | Previne retrabalho e sujeira nas próximas camadas                                 |

---

## 🧠 Conclusão Técnica

A Camada Bronze, quando bem projetada, não é apenas um “repositório bruto”, mas uma **base sólida e rastreável** para todo o pipeline analítico. A aplicação dessas boas práticas permite que você:

- **Ganhe performance desde a ingestão**, evitando gargalos futuros.
- **Reduza custo de cluster**, usando `trigger(once)` e partições eficientes.
- **Aumente a confiabilidade** com Delta Lake e checkpoints.
- **Simplifique manutenção**, mesmo com evolução de schema e crescimento de volume.

---

> 🔍 *"Não existe dado bom na camada Silver sem uma Bronze bem feita." — Engenharia de Dados moderna*

