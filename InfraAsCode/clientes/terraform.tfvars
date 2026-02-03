project_name = "desafio-itau"
environment  = "stag"
region       = "us-east-1"

glue_job_name      = "analise-clientes-sparksql"
glue_script_bucket = "bucket-glue-scripts-lauraxiz"
glue_script_key    = "analise/analise.py"
glue_temp_bucket   = "bucket-glue-temp-lauraxiz"
common_tags = {
  projeto     = "teste_eng_dados"
  environment = "stag"
  terraform   = "true"
}