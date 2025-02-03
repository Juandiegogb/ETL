import csv
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from pyspark.errors.exceptions.base import AnalysisException


class Worker:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def create_datalake(self, csv_file: str, db_origin_url):
        spark = self.spark
        file = open(csv_file, "r", encoding="UTF-8")
        data = csv.reader(file)
        not_found_col = []

        for row in data:
            name = row[0].strip()
            columns = [col for col in row[1].strip().split(" ") if col]

            try:
                if columns:
                    dataframe = spark.read.jdbc(db_origin_url, name).select(columns)
                else:
                    dataframe = spark.read.jdbc(db_origin_url, name)
                dataframe.write.parquet(f"datalake/{name}", mode="overwrite")

            # except Py4JJavaError:
            #     print("Table not found")
            # except AnalysisException as e:
            #     not_found_col.append(e.getMessageParameters()["objectName"])
            #     continue
            # except Py4JJavaError:
            #     print("java err")
            # except AttributeError:
            #     continue
            except Exception as e:
                print(e)

        file.close()
