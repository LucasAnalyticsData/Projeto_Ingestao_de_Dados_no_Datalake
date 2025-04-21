# 🚀 Projeto: Construção da Tabela Gold - Dimensão Cliente

## 📌 Visão Geral

Este projeto tem como objetivo a construção e otimização de uma **tabela Gold de Dimensão Cliente** em um **Data Lakehouse** utilizando o **Databricks**, **Apache Spark**, e **Delta Lake**. O foco principal é garantir a **qualidade dos dados**, **performance nas consultas** e **eficiência no armazenamento** na camada Gold do Data Lake, para suportar análises e decisões em tempo real.

---

## 🎯 Motivação e Problema a Ser Resolvido

Empresas que lidam com grandes volumes de dados de clientes enfrentam desafios na organização, processamento e análise desses dados. Problemas típicos incluem:

✅ **Dados desestruturados** e **duplicados**  
✅ Necessidade de garantir **qualidade e integridade dos dados**  
✅ **Otimização de performance** para consultas analíticas em grande escala  
✅ **Redução de custos operacionais** por meio de uma arquitetura eficiente

Este projeto visa resolver esses desafios, criando uma tabela de dimensão cliente que seja **alta performance**, **robusta** e **preparada para análises de negócios**.

---

## 💡 Solução Proposta

A solução proposta envolve a construção da tabela **dim_cliente** na camada **Gold**, aplicando as seguintes técnicas:

- **Limpeza e deduplicação dos dados**: Remoção de registros inválidos e valores nulos.
- **Reparticionamento estratégico**: Organizar os dados para melhorar o desempenho de leitura, especialmente para consultas frequentes.
- **Cache**: Melhorar a performance de transformações repetidas.
- **Upsert (MERGE)**: Manter os dados atualizados, permitindo a inserção e atualização de registros.
- **Z-ORDER**: Organizar os dados para otimizar consultas frequentes por `CustomerID` e `Country`.
- **VACUUM**: Reduzir o custo de armazenamento, removendo arquivos obsoletos.

A arquitetura segue os princípios da **Arquitetura Medallion** (Medalhão), com dados organizados em camadas **Bronze**, **Silver** e **Gold**.

### 🏗️ Arquitetura Medallion

- **Bronze**: Armazena os dados brutos, sem transformação, garantindo um histórico completo.
- **Silver**: Processa, limpa e enriquece os dados, garantindo qualidade e consistência.
- **Gold**: Contém dados refinados e otimizados para análises de negócios e relatórios.

Na camada Gold, aplicamos o **modelo Star Schema**, utilizando tabelas de **dimensões** e **fatos** para consultas analíticas eficientes.

---

## 🔧 Tecnologias Utilizadas

- **Apache Spark**  
- **Delta Lake**  
- **Databricks**  
- **Parquet**  
- **Azure Data Lake Storage (Gen2)**  
- **SQL (MERGE, Z-ORDER, VACUUM)**  
- **Engenharia de Dados Avançada**

---
