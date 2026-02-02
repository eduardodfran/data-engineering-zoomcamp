terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.gcp_project
  region      = var.gcp_region
}

resource "google_storage_bucket" "demo_bucket" {
  name          = "${var.gcp_project}-demo-bucket"
  location      = var.gcp_region
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "dataset-demo" {
  dataset_id = var.bq_dataset
}
