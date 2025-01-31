import csv
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from pyspark.errors.exceptions.base import AnalysisException


class Worker:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def create_warehouse(self, csv_file: str, db_origin_url):
        spark = self.spark

        with open(csv_file, "r", encoding="UTF-8") as file:
            data = csv.reader(file)

            not_found_col = []

            for row in data:
                name = row[0].strip()
                columns = [col for col in row[1].strip().split(" ") if col]
                if columns:
                    try:
                        dataframe = spark.read.jdbc(db_origin_url, name).select(columns)
                    except AnalysisException as e:
                        not_found_col.append(e.getMessageParameters()["objectName"])
                        continue
                else:
                    dataframe = spark.read.jdbc(db_origin_url, name)
                try:
                    dataframe.write.parquet(f"datalake/{name}", mode="overwrite")
                except Py4JJavaError as e:
                    print("java err")
                except AttributeError:
                    continue
