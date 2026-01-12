variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "repository_name" {
  type = string
}

# --- TUS VARIABLES ORIGINALES (LAS MANTENEMOS) ---
variable "job_name" {
  type = string
}

variable "dataset_id" {
  type = string
}

variable "scheduler_cron" {
  type    = string
  default = "0 6 * * *" # 6:00 AM todos los días
}

# --- LAS VARIABLES QUE FALTABAN (AGRÉGALAS) ---
# Terraform dio error porque main.tf busca estas dos y no las encontraba:

variable "service_name" {
  type        = string
  description = "Nombre para el Cloud Run Job (usado en main.tf)"
}

variable "bucket_name" {
  type        = string
  description = "Bucket para guardar logs y excels (usado en main.tf)"
}