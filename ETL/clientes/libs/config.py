import os
import json
from clientes.libs.utils.log import set_logger
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


class Config(env: ENV):
    def __init__(self, env: str = None) -> None:
        self.logger = set_logger(__name__)
        self.env = env if env else os.environ.get('ENV', 'stag').lower()
        self.input_path = "s3://bucket-input-prod/clientes_sinteticos.csv" if self.env == "prod" else "data/clientes_sinteticos.csv"
        self.bronze_bucket_s3 = "s3://bucket-bronze-prod/clientes_sinteticos.csv" if self.env == "prod" else "s3://bucket-bronze/clientes_sinteticos.csv"
        self.silver_bucket_s3 = "s3://bucket-silver-prod/clientes_sinteticos.csv" if self.env == "prod" else "s3://bucket-silver/clientes_sinteticos.csv"
        
        ######### Melhorias futura #########
        #self.args = get_args()
        #self.filename = self.args.filename
        #self.region = self.env_properties[self.env]
