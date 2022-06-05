terraform {
  backend "s3" {
    bucket = "backend-infratf"
    key    = "resouces/terraform.tfstate"
    region = "us-east-1"
  }
}