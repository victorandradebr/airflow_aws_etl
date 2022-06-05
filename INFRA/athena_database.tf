resource "aws_athena_database" "rola" {
  name          = "database_etl_data"
  bucket        = "athena-tf-data"
  force_destroy = true
  depends_on = [
    aws_s3_bucket.athena-tf-data
  ]

}