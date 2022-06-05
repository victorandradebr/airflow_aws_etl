from pyspark.sql import SparkSession

PATH_PROCESSING = "s3://processing-tf-data/CLIENTES"
PATH_PROCESSING_2 = "s3://processing-tf-data/COMPRAS"
PATH_CURATED = "s3://curated-tf-data/DELIVERED"

QUERY = """

    SELECT
        com.ID,
        cli.NOME,
        cli.SOBRENOME,        
        com.PRODUTO,
        CAST(com.PRECO as decimal(10,2)) as PRECO
    FROM 
        COMPRAS com
    LEFT JOIN 
        CLIENTES cli
    ON
        com.ID = cli.ID

"""

def processing_to_curated(spark):

    df_compras = (
        spark
        .read
        .format("parquet")
        .load(PATH_PROCESSING_2)
    )


    df_clientes = (
        spark
        .read
        .format("parquet")
        .load(PATH_PROCESSING)
    )


    df_compras.createOrReplaceTempView("COMPRAS")

    df_clientes.createOrReplaceTempView("CLIENTES")

    del(df_compras)
    del(df_clientes)

    df_final = spark.sql(QUERY)

    spark.catalog.dropTempView('COMPRAS')
    spark.catalog.dropTempView('CLIENTES')

    (
        df_final
        .write
        .mode("overwrite")
        .format("parquet")
        .save(PATH_CURATED)
    )


if __name__ == '__main__':

    spark = (
        SparkSession
        .builder
        .appName('ETL_AWS_VICTOR')
        .getOrCreate()
    )

    processing_to_curated(spark)