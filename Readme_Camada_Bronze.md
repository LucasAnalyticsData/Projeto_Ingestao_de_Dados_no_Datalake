# 🚀 Projeto: Ingestão de Dados no Data Lakehouse

## 📌 Visão Geral

Este projeto tem como objetivo a construção de um **Data Lakehouse** utilizando o **Databricks** e tecnologias associadas, como **Apache Spark**, **Delta Lake** e **Parquet**. O foco principal está na ingestão, processamento e estruturação de dados de um ambiente de e-commerce, garantindo **alta performance**, **escalabilidade** e **governança dos dados**.

---

## 🎯 Motivação e Problema a Ser Resolvido

Empresas de e-commerce lidam com grandes volumes de dados provenientes de múltiplas fontes, como transações, cadastros de clientes e dados de produtos.

Entretanto, armazenar e processar esses dados de forma eficiente representa um desafio, especialmente ao se buscar:

✅ Escalabilidade para lidar com altos volumes de dados  
✅ Qualidade e confiabilidade na análise e tomada de decisão  
✅ Flexibilidade para consultas analíticas e aprendizado de máquina  
✅ Redução de custos operacionais em relação a arquiteturas tradicionais de Data Warehouses  

---

## 💡 Solução Proposta

Para resolver esses desafios, implementamos uma arquitetura **Lakehouse**, unificando:

- A escalabilidade e flexibilidade dos **Data Lakes**
- Com a governança e estruturação de **Data Warehouses**

### 🏗️ Arquitetura Medallion

Adotamos a **Arquitetura Medallion (Medalhão)**, estruturada em três camadas:

🔸 **Bronze** – Armazena dados brutos sem transformação, garantindo um histórico completo  
🔸 **Silver** – Processa e enriquece os dados, assegurando qualidade e padronização  
🔸 **Gold** – Contém dados refinados e agregados, otimizados para análises e relatórios  

Além disso, aplicamos o **modelo Star Schema** na camada Gold, organizando os dados em **tabelas fato** e **dimensão**, facilitando consultas analíticas eficientes.

---

## 🪙 Camada Bronze

A **Camada Bronze** é responsável por armazenar os dados exatamente como foram recebidos, **sem qualquer transformação**. Essa abordagem assegura:

- Rastreabilidade total
- Possibilidade de reprocessamento futuro
- Preservação da integridade e fidelidade da fonte original

---

## 🔧 Tecnologias Utilizadas

- **Apache Spark**  
- **Delta Lake**  
- **Databricks**  
- **Parquet**  
- **Azure Data Lake Storage (Gen2)**  
- **Star Schema Modeling**

---




# 🧠 Guia Explicativo: Boas Práticas na Camada Bronze

Este documento reúne as boas práticas aplicadas na **Camada Bronze** do pipeline de dados, com foco em performance, economia de recursos e confiabilidade.

---

## 🔷 Camada Bronze (Ingestão)

### ✅ Responsável por:
- Capturar dados brutos de múltiplas fontes (CSV, APIs, arquivos externos).
- Armazenar em Delta Lake mantendo fidelidade com a fonte.

### ✅ Melhorias aplicadas:

- 📥 **Auto Loader com `trigger(once)`** para leitura eficiente e econômica.
- 🧹 Pré-tratamento básico: `dropDuplicates`, `na.drop()`.
- 📁 Particionamento por data (`partitionBy("data_carga")`).
- 💾 Salvamento em formato Delta Lake com schema evolution.

---

## 1. ✅ Leitura eficiente com Auto Loader

### ❌ Antes (forma tradicional):
```python
spark.read.format("csv").load("caminho/dados")
```
Essa abordagem:
- Lê todos os arquivos de uma vez, toda vez;
- Não escala bem para grandes volumes;
- Não detecta automaticamente novos arquivos (sem reprocessar os antigos).

### ✅ Agora (com Auto Loader):
```python
spark.readStream \
     .format("cloudFiles") \
     .option("cloudFiles.format", "csv") \
     .load(BRONZE_PATH)
```

### 📌 Vantagens:
- Detecta automaticamente arquivos novos (sem reler os antigos);
- Ideal para pipelines contínuos ou agendados;
- Melhor uso de recursos do cluster;
- Suporta grandes volumes com performance.

---

## 2. ✅ Particionamento correto dos dados

### Exemplo:
```python
.write.partitionBy("data_carga")
```

### 🧠 O que é `partitionBy`?
É uma forma de **organizar fisicamente os arquivos no Data Lake**. Por exemplo:
```
bronze/clientes/data_carga=2025-04-14/
bronze/clientes/data_carga=2025-04-15/
```

### 📌 Vantagens:
- Spark lê apenas o necessário (ex: apenas um mês);
- Reduz tempo de leitura e custo computacional;
- Evita leitura desnecessária (ótimo em grandes volumes).

---

## 3. ✅ Trigger otimizada para controle de recursos

### Exemplo:
```python
.writeStream \
     .trigger(once=True)
```

### 🧠 Por que isso é importante?
Spark Structured Streaming normalmente fica monitorando o tempo todo. Isso **consome o cluster**, mesmo sem novos arquivos.

### 📌 Com `.trigger(once=True)`:
- Executa apenas uma vez e finaliza o job;
- Ideal para **pipelines batch automatizados**;
- Evita consumo desnecessário de recursos.

---

## 4. ✅ Uso de checkpoint e schema evolution

### Exemplo:
```python
.option("checkpointLocation", checkpoint_path) \
.option("mergeSchema", "true")
```

### 🧠 Para que serve?
- `checkpointLocation`: salva o **estado atual** da leitura contínua;
- `mergeSchema`: permite aceitar **novas colunas** sem quebrar o pipeline.

### 📌 Vantagens:
- Garante **tolerância a falhas**;
- Permite **evolução segura** do schema;
- Pipeline mais resiliente em produção.

---

## 5. ✅ Limpeza e tratamento antecipado

### Exemplo:
```python
df = df.dropDuplicates().na.drop()
```

### 🧠 Explicação:
- `dropDuplicates()`: remove registros duplicados;
- `na.drop()`: remove linhas com valores nulos (null).

### 📌 Por que fazer isso na Bronze?
- Reduz complexidade nas camadas Silver e Gold;
- Evita erros em joins e métricas erradas;
- Garante que os dados cheguem limpos para análise.

---

## ✅ Resumo Visual

| Técnica                       | Benefício principal                         |
|-------------------------------|----------------------------------------------|
| Auto Loader                   | Leitura escalável e incremental              |
| partitionBy                   | Leitura seletiva, performance e economia     |
| trigger(once=True)            | Controle de execução e uso do cluster        |
| checkpoint + mergeSchema      | Tolerância a falhas e schema flexível        |
| dropDuplicates + na.drop()    | Dados limpos desde a origem                  |

---

> _"Engenharia de dados começa na Bronze: quanto melhor a base, mais poderosa será a entrega."_

