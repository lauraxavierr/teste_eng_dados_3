resource "aws_glue_job" "analise_clientes" {
  name     = var.glue_job_name
  role_arn = aws_iam_role.glue_role.arn

  glue_version = "5.0"

  worker_type       = "G.1X"
  number_of_workers = 10

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/${var.glue_script_key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${var.glue_temp_bucket}/temp/"
    "--INPUT_PATH"                       = "s3://bucket-input-lauraxiz/clientes_sinteticos.csv"
  }

  tags = var.common_tags
}
