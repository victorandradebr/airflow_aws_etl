############################################################################################################################################################

resource "aws_s3_bucket" "landing" {
  bucket        = "landing-tf-data"
  force_destroy = true

  tags = {
    Name        = "landing"
    Environment = "Dev"
  }
}

resource "aws_s3_object" "object_1" {
  bucket = "landing-tf-data"
  key    = "DADOS/CLIENTES.csv"
  source = "../DADOS/CLIENTES.csv"
  force_destroy = true

  etag = filemd5("../DADOS/CLIENTES.csv")

  depends_on = [
    aws_s3_bucket.landing
  ]
}


resource "aws_s3_object" "object_2" {
  bucket = "landing-tf-data"
  key    = "DADOS/COMPRAS.csv"
  source = "../DADOS/COMPRAS.csv"
  force_destroy = true

  etag = filemd5("../DADOS/CLIENTES.csv")

  depends_on = [
    aws_s3_bucket.landing
  ]  
}


############################################################################################################################################################

resource "aws_s3_bucket" "processing" {
  bucket        = "processing-tf-data"
  force_destroy = true

  tags = {
    Name        = "processing"
    Environment = "Dev"
  }
}

############################################################################################################################################################

resource "aws_s3_bucket" "curated" {
  bucket        = "curated-tf-data"
  force_destroy = true

  tags = {
    Name        = "curated"
    Environment = "Dev"
  }
}

############################################################################################################################################################

resource "aws_s3_bucket" "emr-codes" {
  bucket        = "emr-tf-data-codes"
  force_destroy = true

  tags = {
    Name        = "emr-codes"
    Environment = "Dev"
  }
}

resource "aws_s3_object" "object_3" {
  bucket = "emr-tf-data-codes"
  key    = "landing_to_processing.py"
  source = "../EMR_CODES/landing_to_processing.py"
  force_destroy = true

  etag = filemd5("../EMR_CODES/landing_to_processing.py")

  depends_on = [
    aws_s3_bucket.emr-codes
  ]  
}


resource "aws_s3_object" "object_4" {
  bucket = "emr-tf-data-codes"
  key    = "processing_to_curated.py"
  source = "../EMR_CODES/processing_to_curated.py"
  force_destroy = true

  etag = filemd5("../EMR_CODES/processing_to_curated.py")

  depends_on = [
    aws_s3_bucket.emr-codes
  ]    
}

#############################################################################################################################################################

resource "aws_s3_bucket" "emr-logs" {
  bucket        = "emr-tf-data-logs"
  force_destroy = true

  tags = {
    Name        = "emr-logs"
    Environment = "Dev"
  }
}

############################################################################################################################################################

resource "aws_s3_bucket" "athena-tf-data" {
  bucket        = "athena-tf-data"
  force_destroy = true

  tags = {
    Name        = "athena-tf-data"
    Environment = "Dev"
  }
}
