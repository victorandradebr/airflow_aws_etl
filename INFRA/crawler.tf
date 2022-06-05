resource "aws_glue_crawler" "example" {
  database_name = "database_etl_data"
  name          = "crawler_etl_data"
  role          = "GlueServiceRoleEtlData"

  s3_target {
    path = "s3://curated-tf-data/DELIVERED/"
  }

  depends_on = [
    aws_athena_database.rola
  ]
}