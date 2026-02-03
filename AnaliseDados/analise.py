import sys
import logging
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import count, current_date, year, avg, to_date
from pyspark.sql.types import StructType, StructField, StringType
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['INPUT_PATH'])
input_path = args['INPUT_PATH']


ENV = "stag"


### Arquivo log.py
def set_logger(name=__name__):
    logger = logging.getLogger('com.itau.desafiotecnico.analise_clientes')
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s: %(message)s', datefmt='%y/%m/%d %H:%M:%S')

    ch.setFormatter(formatter)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        logger.addHandler(ch)
    return logger

logger = set_logger("analise_clientes")

def create_spark_session() -> SparkSession:
    spark_ss = SparkSession.builder.appName("analise_clientes").getOrCreate()
    return spark_ss

def read_csv(spark: SparkSession) -> DataFrame:
    """
        Leitura do arquivo CSV e permanencia em Dataframe Spark.

        Args:
            spark: sessão Spark ativa.

        Returns:
            DataFrame: DataFrame Spark contendo os dados lidos do CSV.

        Raises:
            não se aplica.  
    """
    logger.info("[DEBUG] - Realizando leitura de input")
    df = (
        spark.read
        .schema(get_bronze_schema())
        .option("header", "true")
        #.csv("file:///mnt/notebooks/clientes_sinteticos.csv") # Utilizado somente para testes locais
        .csv(input_path)
    )
    logger.info("[DEBUG] - leitura finalizada")
    return df

def get_bronze_schema() -> StructType:
    """
        Definindo schema dos dados Raw que serão da camada bronze.
        Inicialmente todos os campos serão string.

        Args:
            não se aplica.

        Returns:
            StructType: Schema definido para os dados da camada bronze.

        Raises:
            não se aplica.  
    """
    return StructType([
        StructField("cod_cliente", StringType(), True),
        StructField("nm_cliente", StringType(), True),
        StructField("nm_pais_cliente", StringType(), True),
        StructField("nm_cidade_cliente", StringType(), True),
        StructField("nm_rua_cliente", StringType(), True),
        StructField("num_casa_cliente", StringType(), True),
        StructField("telefone_cliente", StringType(), True),
        StructField("dt_nascimento_cliente", StringType(), True),
        StructField("dt_atualizacao", StringType(), True),
        StructField("tp_pessoa", StringType(), True),
        StructField("vl_renda", StringType(), True),
    ])

def main():
    spark = create_spark_session()

    df = read_csv(spark)

    # Caso tenhamos multiplos tabelas, poderia ser usado como abaixo;
    # Melhoria futura
    #tables = []
    #for table in tables:
    #spark.read.format('bigquery') \
    #    .option('project','xxxxx') \
    #    .option('table',f'{table}') \
    #    .load().createOrReplaceTempView(f'{table}')

    # Cria view temporária para uso com Spark SQL
    df.createOrReplaceTempView("clientes")

    logger.info("=== TOP 5 CLIENTES COM MAIS ATUALIZAÇÕES ===")
    spark.sql("""
        SELECT
            cod_cliente,
            nm_cliente,
            COUNT(*) AS qtd_atualizacoes
        FROM clientes
        GROUP BY cod_cliente, nm_cliente
        ORDER BY qtd_atualizacoes DESC
        LIMIT 5
    """).show(truncate=False)
   
    logger.info("=== MÉDIA DE IDADE DOS CLIENTES ===")
    spark.sql("""
        SELECT
            AVG(
                YEAR(current_date()) - YEAR(
                    TO_DATE(dt_nascimento_cliente, 'yyyy-MM-dd')
                )
            ) AS media_idade_clientes
        FROM clientes
        WHERE dt_nascimento_cliente IS NOT NULL
    """).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
