from pyspark.sql import SparkSession

PATH_LANDING = "s3://landing-tf-data/DADOS/CLIENTES.csv"
PATH_LANDING_2 = "s3://landing-tf-data/DADOS/COMPRAS.csv"
PATH_PROCESSING = "s3://processing-tf-data/CLIENTES"
PATH_PROCESSING_2 = "s3://processing-tf-data/COMPRAS"

def landing_to_processing(spark):

    df_cliente = (
        spark
        .read
        .format("csv")
        .option("header", True)
        .option("sep", ",")
        .load(PATH_LANDING)
    )


    (
        df_cliente
        .write
        .mode("overwrite")
        .format("parquet")
        .save(PATH_PROCESSING)
    )

    ###########################################################################

    df_compras = (
        spark
        .read
        .format("csv")
        .option("header", True)
        .option("sep", ",")
        .load(PATH_LANDING_2)
    )


    (
        df_compras
        .write
        .mode("overwrite")
        .format("parquet")
        .save(PATH_PROCESSING_2 )
    )


if __name__ == '__main__':

    spark = (
        SparkSession
        .builder
        .appName('ETL_AWS_VICTOR')
        .getOrCreate()
    )

    landing_to_processing(spark)