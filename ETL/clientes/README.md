# Desafio Técnico – Teste Prático para Engenharia de Dados


## Visão Geral - Tópico ETL

Este projeto implementa um processo de **ETL utilizando PySpark**, responsável por:

- Ler o arquivo `clientes_sinteticos.csv`
- Processar os dados seguindo o modelo **Bronze → Silver**
- Persistir os dados em **buckets S3**, com **particionamento por data de processamento(anomesdia)**
- Garantir que os dados estejam acessíveis via **AWS Glue Data Catalog**

---

## Arquitetura de Dados

### Camada Bronze
- Mantém os dados **o mais próximo possível do dado bruto e sem tratamentos**.
- Schema definido explicitamente (string-first)
- Escrita incremental (`append`)
- Particionamento por data de processamento (`anomesdia`)

### Camada Silver
- Camada **com dados tratados**, pronto para consumo
- Aplica regras de negócio e qualidade
- Deduplicação por cliente
- Tipagem de dados
- Escrita por sobrescrita (`overwrite`)

---

## Detalhes da Implementação

A implementação foi estruturada em um **único script Python**, conforme solicitado no desafio, utilizando separação lógica por classes para facilitar leitura, manutenção e entendimento do fluxo.

### Principais Componentes

- **Config**
  - Centraliza paths de entrada e saída
  - Controla ambiente (`dev` / `prod`)
  - Garante uso correto do esquema `s3a://`

- **Connection**
  - Responsável pela criação e gerenciamento da `SparkSession`
  - Centraliza a lógica de escrita no S3
  - Controla o modo de escrita (`append` / `overwrite`)

- **Transformation**
  - Leitura do CSV com schema explícito
  - Transformações da camada Bronze
  - Transformações da camada Silver (regra de negócio)


---

## Fluxo de Processamento

1. Leitura do arquivo CSV com schema explícito
2. Escrita dos dados na **camada Bronze**
3. Aplicação de regras de negócio e qualidade
4. Deduplicação dos registros
5. Escrita dos dados na **camada Silver**

---

## Estrutura de Particionamento

### Partição Lógica (Glue Data Catalog)

A partição lógica é definida no **AWS Glue Data Catalog**, permitindo que ferramentas como Athena realizem consultas eficientes.

Conforme o enunciado do desafio, considera-se que as tabelas já existem no Glue.

A atualização das partições lógicas deve ser realizada via:
- **AWS Glue Crawler**
- ou comando SQL:
```sql
MSCK REPAIR TABLE nome_da_tabela;
```

A partição física é criada diretamente no storage utilizando:
.partitionBy("anomesdia")


## Tecnologias Utilizadas
- Python
- PySpark
- Docker
- Docker Compose
- AWS S3
- AWS Glue Data Catalog


## Como Executar o Projeto
### Pré-requisitos
- Docker instalado
- Docker Compose instalado
- Credenciais AWS configuradas localmente
- Usuário AWS precisa possuir permissões adequadas no S3, incluindo:
```
s3:PutObject
s3:GetObject
s3:ListBucket
```


### Passo a Passo de Execução

Subir o ambiente Spark com Docker:
```
docker-compose up -d
```

Acessar o container ou notebook configurado no ambiente
```
docker exec -it jupyter-notebook bash
```

Executar o job Spark:
```
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.extraPythonPath=/mnt/etl \
  --conf spark.executor.extraPythonPath=/mnt/etl \
  /mnt/etl/clientes/main.py
```

Após a execução, encerrar o ambiente:

```
docker-compose down
```

### Desafios Tecnicos 
Uso do S3 e s3a://
A escrita dos dados no S3 utiliza o esquema s3a://, que faz uso do S3AFileSystem do Hadoop. Foi necessário seguir dessa forma para não enviar arquivos poluídos para o bucket.


## Melhorias Futuras
### Controle global de paralelismo
- .config("spark.sql.shuffle.partitions", "4")

### Tratamento de dados sensíveis em logs
- .config("spark.redaction.regex", "(?i)secret|password|token")
- .config("spark.sql.redaction.string.regex", "(?i)secret|password|token")

