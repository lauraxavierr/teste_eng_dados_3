from clientes.libs.config import Config
from clientes.libs.utils.log import set_logger
from clientes.libs.connection import Connection
from clientes.libs.utils.transformation import Transformation

ENV = "stag" # Melhoria futura: utilizar variavel de ambiente
logger = set_logger("ETL_clientes", level="DEBUG")


## Classes instantiation
config = Config(ENV)
conn = Connection(config)
transformation = Transformation(config, conn)

try:
    logger.info(f"[DEBUG] - Iniciando processo ETL | ENV={ENV}")
    spark = conn.spark
    df = transformation.read_csv(spark)
    df_bronze = transformation.transform_bronze(df)
    conn.write_data_in_s3(df_bronze, config.bronze_bucket_s3)

    df_silver = transformation.transform_silver(df_bronze)
    conn.write_data_in_s3(df_silver, config.silver_bucket_s3)

    logger.info("[SUCCESS] - Processo de ETL concluído")

except Exception as error:
    logger.exception("[ERROR] - ETL process failed")
    raise error

finally:
    if spark in locals():
        logger.info("[DEBUG] - Stopping SparkSession")
        spark.stop()
