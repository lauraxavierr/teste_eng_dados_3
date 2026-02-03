#from typing import Type
from pyspark.sql import SparkSession, DataFrame, Window
from libs.config import Config
from libs.utils.log import set_logger


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

    #def _create_spark_session(self) -> Type[SparkSession]:
    def _create_spark_session(self) -> SparkSession:
        if self.config.env in ('prod', 'stag'):
            path = self.config.lib
        #else: #path = f'{self.config.libs_home_dir}/{self.config.libs_subdir}/{self.config.lib}' # melhoria futura
        spark_ss = SparkSession.builder.appName("etl_clientes").getOrCreate()
        spark_ss.conf.set(
            "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
        ) # Remove _SUCCESS durante a escrita no S3
        return spark_ss


    def write_data_in_s3(self, df: DataFrame, bucket: str) -> None:
        """
            Escrita do DataFrame na camada Bronze no S3.

            Args:
                df: DataFrame de entrada com dados brutos.
                bucket: Caminho do bucket S3 para escrita dos dados.

            Returns:
                não se aplica.

            Raises:
                não se aplica.  
        """
        self.logger.info("[DEBUG] - Escrevendo dados no bucket")
        self.logger.info(bucket)

        # "anomesdia" Partição física para entender ao requisito de particionamento por data de processamento
        self.logger.info("Partitions:",df.select('anomesdia').distinct().collect())
        df.write \
        .mode("append") \
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