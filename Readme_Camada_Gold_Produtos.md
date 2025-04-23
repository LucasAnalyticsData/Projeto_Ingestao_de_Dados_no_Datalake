%md
# 🚀 Projeto: Construção da Tabela Gold - Dimensão Produto

## 📌 Visão Geral

Este módulo representa a construção da **tabela de dimensão dim_produto** na camada **Gold** do projeto de Data Lakehouse. A tabela é derivada da Silver e foi desenhada para armazenar as informações relevantes dos produtos vendidos, com **altos padrões de qualidade de dados**, **otimização de leitura** e **governança**.

---

## 🎯 Objetivo

Criar uma tabela dimensional que represente os produtos, garantindo:

- 📦 Dados únicos e limpos de cada produto (ItemID)
- ⚙️ Estrutura otimizada para consultas analíticas
- 📈 Rastreabilidade com controle de criação (Created_at)
- 🔄 Atualizações eficientes via **MERGE Delta**
- 🚀 Alto desempenho com **Z-ORDER** e **reparticionamento estratégico**

---

## 📐 Detalhes Técnicos

- **Fonte**: Camada Silver (tabela_prata_desnormalizadas)
- **Destino**: Camada Gold (gold.dim_produto)
- **Particionamento**: ItemID (100 partições)
- **Chave de Negócio**: ItemID
- **Colunas da Dimensão Produto**:
  - ItemID (identificador do produto)
  - ProductName (nome do produto)
  - Created_at (timestamp de inserção na tabela Gold)

---

## ⚙️ Etapas do Pipeline

1. **Leitura da Tabela Silver** com controle de partições.
2. **Seleção das Colunas Relevantes**: apenas ItemID e ProductName.
3. **Limpeza de Dados**: remoção de duplicatas e valores nulos.
4. **Adição de Auditoria Temporal** com a coluna Created_at.
5. **Cache dos Dados** para melhorar performance nas operações subsequentes.
6. **Auditoria Inicial**: contagem dos produtos únicos existentes.
7. **Aplicação do MERGE Delta**: inserção e atualização baseada em ItemID.
8. **Auditoria Final**: nova contagem dos produtos únicos pós-processamento.
9. **Otimização com Z-ORDER** por ItemID.
10. **Limpeza com VACUUM** para liberação de espaço.
11. **Registro no Catálogo Hive/Unity Catalog** para consultas SQL.

---

## ✅ Benefícios Técnicos Aplicados

| Técnica                      | Finalidade                                                                |
|----------------------------  | --------------------------------------------------------------------------|
| MERGE Delta                | Evita duplicidade e garante atualizações incrementais                     |
| Reparticionamento            | Aumenta performance de escrita/leitura por chave de negócio (ItemID)    |
| dropDuplicates + na.drop | Garante consistência e integridade dos dados                              |
| Z-ORDER                    | Melhora tempo de resposta nas consultas filtradas                         |
| VACUUM                     | Reduz uso de armazenamento eliminando arquivos obsoletos                  |
| Registro no catálogo         | Permite uso via SQL e dashboards com governança centralizada              |

---

## 🧱 Modelo Estrela (Star Schema)

Esta tabela é utilizada como uma **Dimensão** em um **Esquema Estrela**, onde será relacionada à Tabela Fato de Vendas através da chave ItemID, permitindo análises como:

- Total de vendas por produto
- Produtos mais vendidos por período
- Comparações de performance entre categorias

---
