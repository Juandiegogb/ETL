from pyspark.sql import SparkSession


spark: SparkSession = SparkSession.builder.appName("ETL").getOrCreate()


df = spark.read.csv("static/tables.csv")


df.write.parquet("src/tables")
