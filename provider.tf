terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  project = "deprojectv2"
  region = "us-central1"
  zone = "us-central1-a"

}