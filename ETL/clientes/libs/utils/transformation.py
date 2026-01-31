from typing import Type
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import to_date, col, upper, row_number, to_timestamp, current_date, date_format, when
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from clientes.libs.config import Config
from clientes.libs.connection import Connection
from clientes.libs.utils.log import set_logger


class Transformation():
    def __init__(self, config: Type[Config], connection: Type[Connection]) -> None:
        self.logger = set_logger(__name__)
        self.config = config
        self.connection = connection
        self.spark = self.connection._create_spark_session()
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
        self.self.logger.info(f'Exemplo de dados no dataframe')
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
        self.logger.info(f"[DEBUG] - Realizando leitura de input {self.config.input_path}")
        return (
            spark.read
            .option("header", "true")
            .schema(self.get_bronze_schema())
            .csv(self.config.input_path)
        )

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
            - Nome do cliente em maiúsculo
            - Adiciona partição de data de processamento (anomesdia)    

            Args:
                df: DataFrame de entrada com dados brutos.

            Returns:
                DataFrame: DataFrame transformado para a camada Bronze.

            Raises:
                não se aplica.  
        """
        self.logger.info("[DEBUG] - Transformação dados camada Bronze")
        return (
            df
            .withColumn("nm_cliente", upper(col("nm_cliente")))
            .withColumn("anomesdia", date_format(current_date(), "yyyyMMdd"))
        )


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

        # Tratamento dos dados de data e numéricos
        df = (
            df
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

        # Remove duplicados mantendo o mais recente por cliente]
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
        return df
