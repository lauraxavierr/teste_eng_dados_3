import os
import sys
import json
import logging
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import to_date, col, upper, row_number, to_timestamp, current_date, date_format, when
from pyspark.sql.types import StructType, StructField, StringType, DecimalType


### Arquivo log.py
def set_logger(name=__name__):
    logger = logging.getLogger('com.itau.desafiotecnico.etl_clientes')
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s: %(message)s', datefmt='%y/%m/%d %H:%M:%S')

    ch.setFormatter(formatter)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        logger.addHandler(ch)
    return logger


def main():
    try:
        logger.info("[DEBUG] - Iniciando processo ETL")
        spark = conn.spark
        df = transformation.read_csv(spark)
        df_bronze = transformation.transform_bronze(df)
        conn.write_data_in_s3(df_bronze, config.bronze_bucket_s3, operation="append")

        df_silver = transformation.transform_silver(df_bronze)
        conn.write_data_in_s3(df_silver, config.silver_bucket_s3, operation="overwrite") # Camada Silver é a camada de consumo final, então sobrescrevemos os dados a cada execução

        logger.info("[SUCCESS] - Processo de ETL concluído")

    except Exception as error:
        logger.exception("[ERROR] - ETL process failed")
        raise error

    finally:
        if spark in locals():
            logger.info("[DEBUG] - Stopping SparkSession")
            spark.stop()


sys.path.append("/workspace")
logger = set_logger("ETL_clientes")
ENV = os.getenv("ENV", "dev")

## Arquivo libs.util.config.py
class Config():
    def __init__(self, env: str = None) -> None:
        self.logger = set_logger(__name__)
        self.env = ENV
        ############# IMPORTANTE
        #### Necessário incluir como 's3a' pois utiliza S3AFileSystem do Hadoop; Caso contrario, cria e mantem arquivos poluídos no S3
        #############
        self.input_path = "s3://bucket-input-prod/clientes_sinteticos.csv" if self.env == "prod" else "file:///mnt/notebooks/clientes_sinteticos.csv"
        self.bronze_bucket_s3 = "s3a://bucket-bronze-prod/tabela_cliente_landing" if self.env == "prod" else "s3a://bucket-bronze-lauraxiz/tabela_cliente_landing"
        self.silver_bucket_s3 = "s3a://bucket-silver-prod/tb_cliente" if self.env == "prod" else "s3a://bucket-silver-lauraxiz/tb_cliente"

        #self.bronze_bucket_s3 = "s3a://bucket-bronze-prod/tabela_cliente_landing" if self.env == "prod" else "s3a://bucket-bronze/tabela_cliente_landing"
        #self.silver_bucket_s3 = "s3a://bucket-silver-prod/tb_cliente" if self.env == "prod" else "s3a://bucket-silver/tb_cliente"

        ######### Melhorias futura #########
        #self.args = get_args()
        #self.filename = self.args.filename
        #self.region = self.env_properties[self.env]


## Arquivo libs.util.connection.py
class Connection():
    #def __init__(self, config: Type[Config]) -> None:
    def __init__(self, config: Config) -> None:
        self.logger = set_logger(__name__)
        self.config = config
        self.spark = self._create_spark_session()
        self.spark_context = self.spark.sparkContext
        self.partitions = None
        #### Melhoria futura
        # self.path = config.source_path
        # self.file_system = self.file_system()
        # self.df = self.df()
        # self.repartition_number = config.table_config["repartition_number"]


    def _create_spark_session(self) -> SparkSession:
        if self.config.env in ('prod', 'stag'):
            path = self.config.lib
        #else: #path = f'{self.config.libs_home_dir}/{self.config.libs_subdir}/{self.config.lib}' # melhoria futura
        spark_ss = SparkSession.builder.appName("etl_clientes").getOrCreate()
        spark_ss.conf.set(
            "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
        ) # Remove _SUCCESS durante a escrita no S3
        return spark_ss


    def write_data_in_s3(self, df: DataFrame, bucket: str, operation: str) -> None:
        """
            Escrita do DataFrame na camada Bronze no S3.

            Args:
                df: DataFrame de entrada com dados brutos.
                bucket: Caminho do bucket S3 para escrita dos dados.
                operation: Tipo de operação de escrita (append, overwrite, etc.)

            Returns:
                não se aplica.

            Raises:
                não se aplica.  
        """
        self.logger.info("[DEBUG] - Escrevendo dados no bucket")
        self.logger.info(bucket)

        # "anomesdia" Partição física para entender ao requisito de particionamento por data de processamento
        #self.logger.info("[DEBUG] - Partitions:",df.select('anomesdia').distinct().collect())
        df.write \
        .mode(operation) \
        .partitionBy("anomesdia") \
        .parquet(bucket) ####### Necessário incluir como 's3a' pois utiliza S3AFileSystem do Hadoop.
        self.logger.info("[DEBUG] - Dados escritos com sucesso no S3")

    ######### Melhorias futura #########
    #        def df(self) -> Type[DataFrame]:
    #                return self._read_avro(self.path)
    #
    #        def _read_avro(self, path: str) -> Type[DataFrame]:
    #            self.self.logger.info(f'Reading avro data from: {path}')
    #            return self.spark.read.format("avro").option('recursiveFileLookup', 'true').load(path)


## Arquivo libs.util.transformation.py
class Transformation():
    #def __init__(self, config: Type[Config], connection: Type[Connection]) -> None:
    def __init__(self, config: Config, connection: Connection) -> None:
        self.logger = set_logger(__name__)
        self.config = config
        self.connection = connection
        self.spark = self.connection.spark
        self.spark_context = self.spark.sparkContext


    def show_df(self, df: DataFrame= None):
        """
            Exibe exemplo de dados do DataFrame.

            Args:
                df: DataFrame de entrada com dados brutos.

            Returns:
                não se aplica.

            Raises:
                não se aplica.  
        """
        self.logger.info("Exemplo de dados no dataframe")
        if df is not None:
            return df.show(limit=5, truncate=False)


    def read_csv(self, spark: SparkSession) -> DataFrame:
        """
            Leitura do arquivo CSV e permanencia em Dataframe Spark.

            Args:
                spark: sessão Spark ativa.

            Returns:
                DataFrame: DataFrame Spark contendo os dados lidos do CSV.

            Raises:
                não se aplica.  
        """
        self.logger.info("[DEBUG] - Realizando leitura de input")
        df = (
            spark.read
            .schema(self.get_bronze_schema())
            .option("header", "true")
            .csv(self.config.input_path)
        )
        self.logger.info("[DEBUG] - leitura finalizada")
        return df


    def get_bronze_schema(self) -> StructType:
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


    def transform_bronze(self, df:DataFrame) -> DataFrame:
        """
            Aplica transformação para a camada Bronze:
            - Mantém dados brutos
            - Adiciona partição de data de processamento (anomesdia)    

            Args:
                df: DataFrame de entrada com dados brutos.

            Returns:
                DataFrame: DataFrame transformado para a camada Bronze.

            Raises:
                não se aplica.  
        """
        self.logger.info("[DEBUG] - Transformação dados camada Bronze")
        df = df.withColumn("anomesdia", date_format(current_date(), "yyyyMMdd"))
        df = df.repartition(4, "anomesdia") # Controle de paralelismo para melhor perfomance
        return df


    def transform_silver(self, df:DataFrame) -> DataFrame:
        """
            Aplica transformações para a camada Silver:
            - Converte campos de data e número para tipos apropriados
            - Deduplica o dataset mantendo sempre somente a ultima data de atualizacao do cadastro de cada cliente
            - Trata a coluna de telefone de modo a permitir somente valores que sigam o padrao (NN)NNNNN-NNNN os demais devem ficar nulos

            Args:
                df: DataFrame de entrada com dados brutos.

            Returns:
                DataFrame: DataFrame transformado para a camada Silver.

            Raises:
                não se aplica.  
        """
        self.logger.info("[DEBUG] - Aplicando transformações da camada Silver")

        # Tratamento dos dados de data, numéricos e coluna nome_cliente
        df = (
            df
            .withColumn(
                "nm_cliente",
                upper(col("nm_cliente"))
            )
            .withColumn(
                "dt_nascimento_cliente",
                to_date(col("dt_nascimento_cliente"), "yyyy-MM-dd")
            )
            .withColumn(
                "dt_atualizacao",
                to_timestamp(col("dt_atualizacao"), "yyyy-MM-dd")
            )
            .withColumn(
                "vl_renda",
                col("vl_renda").cast(DecimalType(15, 2))
            )
        )

        # Remove duplicados mantendo o mais recente por cliente
        dedup_client = (
            Window
            .partitionBy("cod_cliente")
            .orderBy(col("dt_atualizacao").desc())
        )

        df = (
            df
            .withColumn("rn", row_number().over(dedup_client))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        # Tratamento da coluna telefone_cliente utilizando regex
        df = (
            df
            .withColumn(
                "num_telefone_cliente",
                when(
                    col("telefone_cliente").rlike(r"^\(\d{2}\)\d{5}-\d{4}$"),
                    col("telefone_cliente")
                ).otherwise(None)
            )
            .drop("telefone_cliente")
        )

        df = df.repartition(4, "anomesdia")
        return df


## Classes instantiation
config = Config()
conn = Connection(config)
transformation = Transformation(config, conn)

if __name__ == "__main__":
    main()
