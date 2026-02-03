module "glue_job" {
  source = "../glue_job"

  project_name      = var.project_name
  environment       = var.environment
  region            = var.region
  glue_job_name     = var.glue_job_name
  glue_script_bucket = var.glue_script_bucket
  glue_script_key    = var.glue_script_key
  glue_temp_bucket  = var.glue_temp_bucket

  common_tags = var.common_tags
}
