import sys
from libs.config import Config
from libs.utils.log import set_logger
from libs.connection import Connection
from libs.utils.transformation import Transformation

sys.path.append("/workspace")
logger = set_logger("ETL_clientes")


## Classes instantiation
config = Config()
conn = Connection(config)
transformation = Transformation(config, conn)

try:
    logger.info("[DEBUG] - Iniciando processo ETL")
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
