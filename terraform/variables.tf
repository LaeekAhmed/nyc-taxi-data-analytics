# locals AKA constants:
locals {
  data_lake_bucket = "dtc_data_lake"
}

# passed at run-time, variables without the `default` prop are mandatory args (ie. will be asked by the CLI)
variable "project" {
  description = "Your GCP Project ID" 
  default = "zoom-camp-2023"
}

variable "credentials" {
   description = "json file created for service account"
   default = "/mnt/c/Users/User/.google/credentials/zoom-camp-2023-69ea6c471a65.json" # needs to be updated
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-east4" # N. Virginia (more compatible with dbt)
  type = string
}

# Not needed for now
# variable "bucket_name" {
#   description = "The name of the GCS bucket. Must be globally unique."
#   default = ""
# }

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "nyc_trips_data"
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type = string
  default = "test_table"
}