variable "credentials" {
  description = "My Credentials"
  default = "./keys/creds.json"
}

variable "bq_dataset" {
  description = "BigQuery dataset name"
  default = "dataset_demo"
}

variable "gcp_project" {
  description = "GCP project ID"
  default     = "zoomcamp-data-engineer-484608"
}

variable "gcp_region" {
  description = "GCP region"
  default     = "asia-southeast1"
}