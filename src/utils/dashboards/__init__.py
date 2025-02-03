from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.appName("Hosvital").getOrCreate()
