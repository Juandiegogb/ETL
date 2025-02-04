import csv
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from os import path, PathLike
import os
from utils.tools.custom_print import print_error
from utils.entities.db import DB
from types import ModuleType
from pyspark.errors.exceptions.captured import AnalysisException


class Worker:
    def __init__(self):
        self.spark: SparkSession
        self.workdir: PathLike
        self.data_origin_url: str
        self.data_destiny_url: str
        self.datalake: PathLike
        self.warehouse: PathLike

        workdir = os.getenv("IMPERIUM_WORKDIR")

        if not workdir:
            print_error("Missing environment variable IMPERIUM_WORKDIR")

        if not path.exists(workdir):
            print_error(f"Directory '{workdir}' not found")

        self.workdir = workdir
        self.datalake = path.join(self.workdir, "datalake")
        self.warehouse = path.join(self.workdir, "warehouse")

    def getWorkdir(self) -> PathLike:
        return self.workdir

    def create_datalake(self, database: DB):
        self.data_origin_url: str = database.url
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
            with open(csv_file, "r", encoding="UTF-8") as file:
                data = list(csv.reader(file))

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
                dataframe.write.parquet(f"{self.datalake}/{name}", mode="overwrite")

        except FileNotFoundError:
            print_error("File tables.csv not found on workdir, please create it")

        except IndexError:
            print_error("Invalid csv (Comma separated values) format, check the file")

        except AnalysisException as e:
            print(e.args)
            print_error("youuuu")

        except Py4JJavaError:
            print_error(f"Table {name} not found")

        file.close()

    def execute(self, modules: list[ModuleType]):
        if not all(isinstance(mod, ModuleType) for mod in modules):
            print_error(
                "modules arg must be a list of modules (ModuleType) from worker.execute()"
            )

        for mod in modules:
            mod.etl(self.datalake, self.warehouse)

    def test(self):
        datalake_memers = os.listdir(path.join(self.workdir, "datalake"))
        print(datalake_memers)

    def load_data(self):
        try:
            tables = [
                [table, path.join(self.warehouse, table)]
                for table in os.listdir(self.warehouse)
            ]

            for table_path in tables:
                name = table_path[0]
                df = self.spark.read.parquet(table_path)
                df.write.jdbc(self.data_destiny_url, name, mode="overwrite")

            os.removedirs(self.datalake, self.warehouse)

        except FileNotFoundError:
            print_error("Folder warehouse not found, use worker.execute to create it")
