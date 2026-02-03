variable "project_name" {
  type        = string
  description = "Nome do projeto"
}

variable "environment" {
  type        = string
  description = "Ambiente (dev, stag, prod)"
}

variable "region" {
  type        = string
  description = "AWS Region"
}

variable "glue_job_name" {
  type        = string
  description = "Nome do Glue Job"
}

variable "glue_script_bucket" {
  type        = string
  description = "Bucket S3 onde o script Spark será armazenado"
}

variable "glue_script_key" {
  type        = string
  description = "Path do script Spark no S3"
}

variable "glue_temp_bucket" {
  type        = string
  description = "Bucket S3 para arquivos temporários do Glue"
}

variable "common_tags" {
  type        = map(string)
  description = "Tags comuns para os recursos AWS"
}
