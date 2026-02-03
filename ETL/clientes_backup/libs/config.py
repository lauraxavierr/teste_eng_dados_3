import os
import json
from libs.utils.log import set_logger
#import datetime
#import argparse

ENV = os.getenv("ENV", "dev")

##### Melhoria futura: Poderia ser evoluído para um módulo separado com utilização do Airflow 
#def get_args():
#    parser = argparse.ArgumentParser()
#    parser.add_argument("table_name",
#                        type=str,
#                        help="Nome da tabela a ser executada"
#                        )
#    parser.add_argument("airflow_date",
#                        type=lambda s: datetime.datetime.strptime(
#                            s, '%Y-%m-%d'),
#                        help="Data de execução do airflow no formato %Y-%m-%d"
#                        )
#    return parser.parse_args()


class Config():
    def __init__(self, env: str = None) -> None:
        self.logger = set_logger(__name__)
        self.env = ENV
        ############# IMPORTANTE
        #### Necessário incluir como 's3a' pois utiliza S3AFileSystem do Hadoop; Caso contrario, cria e mantem arquivos poluídos no S3
        #############
        self.input_path = "s3://bucket-input-prod/clientes_sinteticos.csv" if self.env == "prod" else "file:///mnt/notebooks/clientes_sinteticos.csv"
        self.bronze_bucket_s3 = "s3a://bucket-bronze-prod/clientes_sinteticos.parquet" if self.env == "prod" else "s3a://bucket-bronze-lauraxiz/clientes_sinteticos"
        self.silver_bucket_s3 = "s3a://bucket-silver-prod/clientes_sinteticos.parquet" if self.env == "prod" else "s3a://bucket-silver-lauraxiz/clientes_sinteticos"


        ######### Melhorias futura #########
        #self.args = get_args()
        #self.filename = self.args.filename
        #self.region = self.env_properties[self.env]
