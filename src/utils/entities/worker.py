import csv
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from os import path, PathLike
import os
from utils.tools.custom_print import print_error
from utils.entities.db import DB
import vaex


class Worker:
    def __init__(self):
        self.spark: SparkSession
        self.workdir: PathLike
        self.data_origin_url: str
        self.data_destiny_url: str

        workdir = os.getenv("IMPERIUM_WORKDIR")

        if not workdir:
            print_error("Missing environment variable IMPERIUM_WORKDIR")

        if not path.exists(workdir):
            print_error(f"Directory '{workdir}' not found")

        self.workdir = workdir

    def getWorkdir(self) -> PathLike:
        return self.workdir

    def create_datalake(self, database: DB):
        self.data_origin_url = database.url
        self.spark = (
            SparkSession.builder.appName("ETL")
            .master("local[*]")
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
            .getOrCreate()
        )

        spark = self.spark
        workdir = self.workdir
        csv_file = path.join(workdir, "tables.csv")
        try:
            file = open(csv_file, "r", encoding="UTF-8")
            data = csv.reader(file)

            ## this loop check if the tables exists in DB
            for row in data:
                name = row[0].strip()
                columns = [col for col in row[1].strip().split(" ") if col]
                spark.read.jdbc(self.data_origin_url, name)

            for row in data:
                name = row[0].strip()
                columns = [col for col in row[1].strip().split(" ") if col]

                if columns:
                    dataframe = spark.read.jdbc(self.data_origin_url, name).select(
                        columns
                    )
                else:
                    dataframe = spark.read.jdbc(self.data_origin_url, name)
                dataframe.write.parquet(f"{workdir}/datalake/{name}", mode="overwrite")

        except FileNotFoundError:
            print_error("File tables.csv not found on workdir, please create it")

        except IndexError:
            print_error("Invalid csv (Comma separated values) format, check the file")

        except Py4JJavaError as e:
            print(e)
            print_error(f"Table {name} not found")

        file.close()

    def test(self):
        datalake_memers = os.listdir(path.join(self.workdir, "datalake"))
        print(datalake_memers)
