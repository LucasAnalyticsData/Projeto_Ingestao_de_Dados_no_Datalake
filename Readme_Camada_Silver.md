# 🪙 Camada Silver – Enriquecimento e Otimização de Vendas

A **Camada Silver** é responsável por transformar os dados brutos da Bronze em uma estrutura limpa, confiável e pronta para análise. Aqui, aplicamos técnicas avançadas de engenharia de dados para gerar uma tabela desnormalizada de vendas (modelo **One Big Table**) com foco em performance e facilidade de consumo analítico.

> "Na Silver, os dados deixam de ser apenas rastreáveis e se tornam analiticamente poderosos."

---

## 📁 Localizações
| Camada | Path |
|--------|------|
| Bronze | `abfss://bronze@dlsprojetofixo.dfs.core.windows.net` |
| Silver | `abfss://silver@dlsprojetofixo.dfs.core.windows.net` |

---

## 🔍 Fontes Utilizadas
- `vendas_lucas_exploded`: Informações detalhadas de vendas, item a item.
- `clientes_lucas`: Dados cadastrais dos clientes.

---

## ⚙️ Pipeline Detalhado e Comentado

### 1. ✅ Leitura das Tabelas Bronze com Delta Lake
```python
vendas = spark.read.format("delta").load("path_para_bronze/vendas_lucas_exploded")
clientes = spark.read.format("delta").load("path_para_bronze/clientes_lucas")
```
📌 *Uso do formato Delta garante performance, leitura otimizada e suporte a schema evolution.*

---

### 2. ✅ Conversão de tipos e limpeza inicial
```python
from pyspark.sql.functions import col, to_date

vendas = vendas.withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd"))
clientes = clientes.drop("Date_Time_Load")
```
📌 *Conversão de OrderDate melhora análises temporais. A coluna `Date_Time_Load` é irrelevante para análise e foi removida.*

---

### 3. ✅ Evitar Duplicação e limpeza de nulos
```python
vendas = vendas.dropDuplicates().na.drop()
clientes = clientes.dropDuplicates().na.drop()
```
📌 *Remove ruídos como duplicatas e linhas com dados ausentes, elevando a qualidade desde já.*

---

### 4. ✅ Enriquecimento via Broadcast Join
```python
from pyspark.sql.functions import broadcast

silver = vendas.join(broadcast(clientes), on="CustomerID", how="inner")
```
📌 *Broadcast Join evita shuffle e acelera o processamento em joins assimétricos.*

---

### 5. ✅ Inclusão de coluna de auditoria
```python
from pyspark.sql.functions import current_timestamp

silver = silver.withColumn("last_updated", current_timestamp())
```
📌 *Permite rastreabilidade da carga, essencial em ambientes produtivos e auditoria.*

---

### 6. ✅ Ordenação das colunas para consistência
```python
colunas_ordenadas = [
    "OrderID", "CustomerID", "OrderDate", "TotalAmount", "Quantity", "Price",
    "CustomerName", "Country", "Email", "Phone", "last_updated"
]
silver = silver.select(colunas_ordenadas)
```
📌 *Padroniza a estrutura para facilitar leitura e uso por dashboards e analistas.*

---

### 7. ✅ Reparticionamento e otimização por OrderDate
```python
silver = silver.repartition("OrderDate")
silver.cache()
```
📌 *Reparticiona logicamente os dados, melhorando escrita e leitura. Uso de `cache()` evita reprocessamentos em etapas seguintes.*

---

### 8. ✅ Escrita incremental com merge (upsert)
```python
from delta.tables import DeltaTable

tabela_silver = "gold.fato_vendas"
tabela_path = "abfss://silver@dlsprojetofixo.dfs.core.windows.net/tabela_prata_desnormalizadas"

if DeltaTable.isDeltaTable(spark, tabela_path):
    delta_tabela = DeltaTable.forPath(spark, tabela_path)
    (
        delta_tabela.alias("target")
        .merge(
            silver.alias("source"),
            "target.OrderID = source.OrderID AND target.CustomerID = source.CustomerID"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver.write.format("delta").mode("overwrite").partitionBy("OrderDate").save(tabela_path)
```
📌 *Uso de `merge` permite ingestão incremental sem sobrescrever os dados, garantindo performance e consistência.*

---

### 9. ✅ Registro no Metastore para consumo via SQL
```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tabela_silver}
    USING DELTA
    LOCATION '{tabela_path}'
""")
```
📌 *Facilita uso via notebooks SQL, BI tools e democratiza o acesso aos dados confiáveis.*

---

### 10. ✅ Otimização e limpeza da tabela
```python
spark.sql(f"OPTIMIZE {tabela_silver} ZORDER BY (OrderDate, OrderID)")
spark.sql(f"VACUUM {tabela_silver} RETAIN 168 HOURS")
```
📌 *OPTIMIZE reorganiza fisicamente os arquivos para leitura por colunas filtradas. VACUUM limpa arquivos obsoletos, reduzindo custo de armazenamento.*

---

## ✅ Técnicas Avançadas Utilizadas
| Técnica | Benefício |
|--------|-----------|
| DeltaTable.merge() | Atualização incremental robusta, sem sobrescrita total |
| broadcast join | Acelera joins entre grandes e pequenas tabelas |
| dropDuplicates() + na.drop() | Dados limpos desde a Silver, evitando falhas em análises |
| cache() | Reaproveitamento em etapas intensas sem reprocessamento |
| OPTIMIZE ZORDER | Performance superior em consultas analíticas |
| VACUUM | Redução de custos e organização do Delta Lake |
| Metastore SQL | Permite acesso padronizado por notebooks e BI |

---

## 🧠 Conclusão
Essa arquitetura Silver mostra um pipeline robusto, escalável e alinhado com as melhores práticas de engenharia de dados no Databricks. Ao seguir essa abordagem, garantimos consistência, performance e rastreabilidade ponta-a-ponta para as análises futuras.

> "Silver transforma dados brutos em valor analítico com elegância e eficiência."

---




