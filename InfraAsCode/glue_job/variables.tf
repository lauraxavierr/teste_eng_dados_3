variable "project_name" {}
variable "environment" {}
variable "region" {}

variable "glue_job_name" {}
variable "glue_script_bucket" {}
variable "glue_script_key" {}
variable "glue_temp_bucket" {}
variable "common_tags" {
  type = map(string)
}

