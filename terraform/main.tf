provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. APIs
resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudscheduler.googleapis.com",
    "bigquery.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}

# 2. REPOSITORIO DOCKER
resource "google_artifact_registry_repository" "repo" {
  location      = var.region
  repository_id = var.repository_name
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]
}

# 3. BUILD DE LA IMAGEN (Local-exec para subir tu src/)
resource "null_resource" "build_push" {
  # Detectamos cambios en "../src"
  triggers = {
    dir_sha1 = sha1(join("", [for f in fileset("${path.module}/../src", "*") : filesha1("${path.module}/../src/${f}")]))
  }

  provisioner "local-exec" {
    command = "gcloud builds submit ${path.module}/../src --tag ${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/etl-seia:latest --project ${var.project_id}"
  }
  depends_on = [google_artifact_registry_repository.repo]
}

# 4. SERVICE ACCOUNT
resource "google_service_account" "job_sa" {
  account_id   = "seia-job-sa"
  display_name = "SA para Job SEIA"
}

resource "google_project_iam_member" "perms" {
  for_each = toset([
    "roles/bigquery.admin",
    "roles/storage.objectAdmin",
    "roles/logging.logWriter",
    "roles/artifactregistry.reader",
    "roles/run.invoker" # MODIFICACIÓN: Permiso para que el SA pueda ejecutar el Job
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.job_sa.email}"
}

# 5. CLOUD RUN JOB (Aquí está la magia para procesos largos)
resource "google_cloud_run_v2_job" "seia_job" {
  name     = var.service_name
  location = var.region

  template {
    template {
      service_account = google_service_account.job_sa.email
      timeout = "3600s" 
      
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/etl-seia:latest"
        
        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }
        
        # MODIFICACIÓN: Fuerza la actualización si el código cambia
        env {
          name  = "CODE_VERSION"
          value = null_resource.build_push.triggers.dir_sha1
        }

        env {
          name = "PROJECT_ID"
          value = var.project_id
        }
        env {
          name = "DATASET_ID"
          value = "dataset_ambiental"
        }
        env {
          name = "BUCKET_NAME"
          value = var.bucket_name
        }
      }
    }
  }
  depends_on = [null_resource.build_push, google_project_iam_member.perms]
}

# 6. SCHEDULER (El Cron)
resource "google_cloud_scheduler_job" "cron" {
  name        = "trigger-seia-daily"
  description = "Ejecuta el scraper diariamente"
  schedule    = "0 6 * * *"
  time_zone   = "America/Santiago"
  region      = var.region

  http_target {
    http_method = "POST"
    # MODIFICACIÓN: URI actualizada a la API v2 de Cloud Run
    uri         = "https://${var.region}-run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${var.service_name}:run"
    
    oauth_token {
      service_account_email = google_service_account.job_sa.email
    }
  }
  depends_on = [google_cloud_run_v2_job.seia_job]
}