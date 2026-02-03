# Desafio Técnico – Teste Prático para Engenharia de Dados  


## Etapa 2 – Análise de Dados com Spark SQL

---

## Visão Geral – Análise dos Dados

Esta etapa do desafio tem como objetivo demonstrar o uso de **Spark SQL** para análises analíticas sobre o dataset `clientes_sinteticos.csv`.

A implementação foi realizada utilizando **PySpark com Spark SQL**
As análises solicitadas foram:

- Identificar os **5 clientes que mais sofreram atualizações** na base
- Calcular a **média de idade dos clientes**

---


## Arquitetura Utilizada

- Leitura do arquivo CSV com **schema explícito**
- Criação de **Temporary View** no Spark
- Execução das análises exclusivamente via **SQL**
- Execução distribuída em cluster Spark Standalone (Docker)

---

## Detalhes da Implementação

A análise foi implementada em um **script Python único**, conforme solicitado no desafio.

### Principais Componentes

- **Logger**
  - Padronização de logs
  - Facilita rastreabilidade da execução

- **SparkSession**
  - Criada explicitamente para o job analítico
  - Execução via `spark-submit`

- **Schema explícito**
  - Evita inferência incorreta de tipos
  - Garante consistência de leitura

- **Spark SQL**
  - Uso de `createOrReplaceTempView`
  - Consultas SQL claras e legíveis

---

## Fluxo de Processamento

1. Criação da SparkSession
2. Leitura do arquivo `clientes_sinteticos.csv` com schema explícito
3. Criação de view temporária `clientes`
4. Execução das análises utilizando Spark SQL
5. Exibição dos resultados no console
6. Finalização da SparkSession

---

## Análises Realizadas

#### Top 5 clientes com mais atualizações
Consulta SQL utilizada:

```sql
SELECT
    cod_cliente,
    nm_cliente,
    COUNT(*) AS qtd_atualizacoes
FROM clientes
GROUP BY cod_cliente, nm_cliente
ORDER BY qtd_atualizacoes DESC
LIMIT 5;
```

```TEXT
Resultado obtido:
cod_cliente	nm_cliente	qtd_atualizacoes
177	Karen Hart	1
858	Steven Smith	1
407	Mr. Craig Martinez DDS	1
366	Diana Hatfield	1
414	Clayton Thomas	1
```

#### Média de idade dos clientes
Consulta SQL utilizada:
```sql
SELECT
    AVG(
        YEAR(current_date()) -
        YEAR(TO_DATE(dt_nascimento_cliente, 'yyyy-MM-dd'))
    ) AS media_idade_clientes
FROM clientes
WHERE dt_nascimento_cliente IS NOT NULL;
```

```TEXT
Resultado obtido:
Média de idade dos clientes: 51.142 anos
```

### Tecnologias Utilizadas
- Python
- PySpark
- Spark SQL
- Docker
- Docker Compose


### Como Executar a Análise
### Pré-requisitos
- Docker instalado
- Docker Compose instalado

###Passo a Passo de Execução
- Subir o ambiente Spark:

```
docker-compose up -d
```
- Acessar o container do Jupyter:

```
docker exec -it jupyter-notebook bash
```

- Executar o job analítico:

```
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /mnt/analisedados/analise.py
```

- Após a execução, encerrar o ambiente:
```
docker-compose down
```

Considerações Técnicas
A execução via spark-submit simula um ambiente real de produção e manipulação dos dados utilizando views temporárias.


### Melhorias Futuras
- Leitura direta da camada Silver no S3
- Persistência dos resultados analíticos em view temporaria.
- Parametrização do caminho de entrada via argumentos
