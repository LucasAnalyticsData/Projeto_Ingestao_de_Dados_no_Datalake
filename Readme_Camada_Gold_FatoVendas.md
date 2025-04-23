%md
# 🚀 Projeto: Construção da Tabela Gold - Fato Vendas

## 📌 Visão Geral

Este módulo representa a construção da **tabela de fato `fato_vendas`** na camada **Gold** do projeto de Data Lakehouse. A tabela é derivada da camada Silver e consolidada com foco em **eficiência analítica**, **integridade dos dados**, **otimização de performance** e **auditoria completa**.

---

## 🎯 Objetivo

Criar uma tabela de fatos robusta que consolide informações de vendas, garantindo:

- 🧾 Unicidade por combinação de `OrderID` + `ItemID`
- 💰 Registro de métricas de vendas como `TotalAmount`, `Quantity` e `Price`
- 🕒 Auditoria temporal com `Created_at` e versionamento Delta
- 🔄 Atualizações seguras com **MERGE Delta**
- ⚡ Performance otimizada com **Z-ORDER** e **particionamento eficiente**

---

## 📐 Detalhes Técnicos

- **Fonte**: Camada Silver (`tabela_prata_desnormalizadas`)
- **Destino**: Camada Gold (`gold.fato_vendas`)
- **Particionamento**: `Status` (categoria do pedido)
- **Chave de Negócio**: Composição de `OrderID` e `ItemID`
- **Colunas da Tabela Fato Vendas**:
  - `OrderID` (identificador do pedido)
  - `ItemID` (identificador do item)
  - `CustomerID` (identificador do cliente)
  - `OrderDate` (data do pedido)
  - `Status` (status do pedido)
  - `Quantity` (quantidade)
  - `Price` (preço unitário)
  - `TotalAmount` (valor total)
  - `Created_at` (timestamp de inserção na camada Gold)

---

## ⚙️ Etapas do Pipeline

1. **Criação do Banco de Dados `gold`**, se não existir.
2. **Leitura da Tabela Silver** contendo dados desnormalizados.
3. **Cache do DataFrame** para otimizar múltiplas operações sequenciais.
4. **Criação da Coluna `Created_at`** para auditoria e rastreabilidade.
5. **Geração da Coluna `hash_value`** para identificação de registros únicos.
6. **Remoção de Duplicatas** usando o hash como referência de unicidade.
7. **Uso de Window Function** para manter apenas o registro mais recente de cada combinação `OrderID` + `ItemID`.
8. **Auditoria Inicial**: contagem de registros únicos preparados para a Gold.
9. **Aplicação do MERGE Delta**: inserção e atualização eficiente baseada nas chaves de negócio.
10. **Auditoria Final**: contagem pós-processamento.
11. **Otimização com Z-ORDER** por `OrderID` para aceleração de leitura.
12. **Limpeza com VACUUM** para liberação de espaço de arquivos antigos.
13. **Registro no Catálogo Hive/Unity Catalog** para permitir consultas SQL e governança.

---

## ✅ Benefícios Técnicos Aplicados

| Técnica                      | Finalidade                                                                 |
|-----------------------------|----------------------------------------------------------------------------|
| `MERGE` Delta               | Atualizações incrementais seguras e sem duplicidade                        |
| `sha2(hash)`                | Identificação rápida e leve de duplicatas completas                        |
| `dropDuplicates` + `Window` | Remoção de linhas redundantes com base nas chaves e timestamp              |
| `cache()`                   | Otimização de múltiplas operações no mesmo DataFrame                       |
| `Z-ORDER`                   | Melhoria significativa no tempo de resposta para filtros por `OrderID`     |
| `VACUUM`                    | Redução de uso de armazenamento e remoção de arquivos obsoletos            |
| `current_timestamp()`       | Registro confiável de quando os dados foram inseridos                      |
| Registro no catálogo        | Acesso via consultas SQL, dashboards BI e governança centralizada          |

---

## 📊 Métricas Utilizáveis

A tabela permite geração de KPIs e análises como:

- 🛍️ Total de vendas por pedido ou cliente
- 📈 Evolução temporal de faturamento
- 📦 Itens mais vendidos
- 🧾 Ticket médio por cliente
- 🧭 Performance de vendas por status (`Status`) ou data (`OrderDate`)

---

## 🧱 Modelo Estrela (Star Schema)

Esta tabela representa a **Fato Vendas** de um **modelo dimensional**, conectada a dimensões como:

- `dim_cliente` → via `CustomerID`
- `dim_tempo` → via `OrderDate`
- `dim_produto` (futura) → via `ItemID`

Permite análises OLAP em dashboards e modelos de Machine Learning supervisionado com base em comportamento de compra.

---
