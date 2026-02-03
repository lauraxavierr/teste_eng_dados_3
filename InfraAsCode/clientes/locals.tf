locals {
  time_zone = "America/Sao_Paulo"

  common_tags = {
    projeto     = "teste_eng_dados"
    environment = var.environment
    terraform   = "true"
  }
}
