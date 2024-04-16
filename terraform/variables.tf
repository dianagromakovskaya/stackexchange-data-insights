variable "credentials" {
  description = "Your Credentials"
  default     = "../config/credentials/google_credentials.json"
}

variable "gcs_bucket" {
  description = "GCS bucket"
  default     = "stackexchange-data"
}

variable "project" {
  description = "Your GCP Project ID"
  default     = "dtc-de-course-412710"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  default     = "stackexchange_data"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}