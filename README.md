# Desafio Raízen 
O teste consiste em desenvolver uma pipeline de orquestração de dados ETL que extraia relatórios consolidados sobre vendas de combustíveis do site da ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis), transforme os dados fazendo uma limpeza e estruturação dos campos, exporte os arquivos em um formato de big data e valide a integridade dos dados brutos (raw data) com os dados tratados.

As tabelas que deverão ser extraídas do site da ANP são:
- Vendas de combustíveis derivados de petróleo por UF e por produto;
- Vendas de diesel por UF e por tipo;

Os dados deverão ser armazenados nos seguintes formatos:

| Coluna       | Tipo        |
| ------------ | ----------- |
| `year_month` | `date`      |
| `uf`         | `string`    |
| `product`    | `string`    |
| `unit`       | `string`    |
| `volume`     | `double`    |
| `created_at` | `timestamp` |

link do repositório contendo o teste da Raízen: https://github.com/raizen-analytics/data-engineering-test

Nesto projeto foi utilizado as seguintes ferramentas: vscode, google colab, apache airflow e docker

# Resolução Desafio Raízen (Etapas)

Criar a lógica em Python (extração e tratamento) utilizando bibliotecas como requests,pandas, os e subprocess.
Teste e validação do código criado no Google Colab
Criado arquitetura de medalhão para armazenamento dos dados trabalhados (raw_data, silver e gold)
Instalação e configuraçao do Apache Airflow
Criação da DAG para orquestração.











