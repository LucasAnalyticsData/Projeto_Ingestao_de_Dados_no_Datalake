# 🪙 Camada Silver - Enriquecimento e Otimização de Vendas

## 📌 Objetivo

Este pipeline tem como finalidade transformar os dados brutos processados da **Camada Bronze** em uma **tabela desnormalizada, otimizada e confiável** (Esse modelo One Big Table (OBT) facilita análises exploratórias, reduz a necessidade de joins complexos e melhora a performance de consultas analíticas) na **Camada Silver**, pronta para análise e consumo por dashboards, relatórios e modelo analítico. Ele aplica práticas avançadas de engenharia de dados com o **Delta Lake** no ambiente **Databricks**.

---

## 📁 Estrutura de Pastas

| Camada     | Localização |
|------------|-------------|
| Bronze     | `abfss://bronze@dlsprojetofixo.dfs.core.windows.net` |
| Silver     | `abfss://silver@dlsprojetofixo.dfs.core.windows.net` |

---

## 🔍 Fontes Utilizadas

- `vendas_lucas_exploded` — Tabela com informações detalhadas de vendas (por item).
- `clientes_lucas` — Tabela de dados cadastrais dos clientes.

---

## 🛠️ Principais Transformações

- **Conversão de tipos:** `OrderDate` convertida para `Date`
- **Remoção de colunas ambíguas:** `Date_Time_Load` removido de clientes
- **Limpeza de dados:** remoção de duplicatas e nulos (`dropDuplicates()` + `na.drop()`)
- **Desnormalização:** join entre vendas e clientes via chave `CustomerID`
- **Padronização:** ordenação explícita das colunas
- **Inclusão de auditoria:** campo `last_updated` com timestamp da carga
- **Reparticionamento por data:** otimização por `OrderDate`

---

## 🧠 Técnicas Avançadas Utilizadas

| Técnica                    | Benefício                                                                 |
|---------------------------|--------------------------------------------------------------------------|
| 🔄 `DeltaTable.merge()`    | Garantia de atualização incremental e controle de duplicidade           |
| 📦 `broadcast join`       | Acelera joins assimétricos (cliente x vendas) evitando shuffle           |
| 🧹 `dropDuplicates()` + `na.drop()` | Eleva a qualidade do dado removendo inconsistências              |
| 🧊 `cache()`               | Melhora performance em pipelines com múltiplas etapas                    |
| 🧱 `OPTIMIZE ZORDER`       | Melhora leitura por colunas com filtragem frequente (`OrderDate`, `OrderID`) |
| 🧼 `VACUUM`                | Reduz custo de armazenamento com limpeza de arquivos obsoletos           |
| 📚 Registro no metastore  | Permite acesso à tabela Silver via SQL e notebooks                       |

---

## 🧪 Validação Final

Ao final da execução, é feita a contagem total de registros na tabela para fins de auditoria:

```python
✅ Total de registros na Tabela Silver: XXXX

