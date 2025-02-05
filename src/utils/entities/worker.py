import csv
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from os import path, PathLike, cpu_count
import os
from utils.tools.custom_print import print_error, print_success
from utils.entities.db import DB
from types import ModuleType
from pyspark.errors.exceptions.captured import AnalysisException
import shutil
import numpy as np
from multiprocessing import Process


class Worker:
    def __init__(self):
        self.spark: SparkSession
        self.workdir: PathLike
        self.data_origin_url: str
        self.data_destiny_url: str
        self.datalake: PathLike
        self.warehouse: PathLike
        self.cpu: int = cpu_count()

        workdir = os.getenv("IMPERIUM_WORKDIR")

        if not workdir:
            print_error("Missing environment variable IMPERIUM_WORKDIR")

        if not path.exists(workdir):
            print_error(f"Directory '{workdir}' not found")

        self.workdir = workdir
        self.datalake = path.join(self.workdir, "datalake")
        self.warehouse = path.join(self.workdir, "warehouse")

        print_success("Worker created")

    def create_datalake(self, database: DB):
        print_success("Creating datalake ...")
        self.spark = (
            SparkSession.builder.appName("ETL")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")
        self.data_origin_url: str = database.url

        tables_in_db = (
            self.spark.read.jdbc(self.data_origin_url, "INFORMATION_SCHEMA.TABLES")
            .select("TABLE_NAME")
            .collect()
        )

        csv_file = path.join(self.workdir, "tables.csv")
        try:
            with open(csv_file, "r", encoding="UTF-8") as file:
                data = list(csv.reader(file))

            tables_in_db = [row["TABLE_NAME"].upper() for row in tables_in_db]
            expected_tables = [row[0].upper() for row in data]
            missing_tables = [
                table for table in expected_tables if table not in tables_in_db
            ]

            if missing_tables:
                print_error(
                    f"This tables {missing_tables} not exist.\nData origin -> {database.db_host}.{database.db_name}"
                )

            for row in data:
                name = row[0].strip()
                columns = [col for col in row[1].strip().split(" ") if col]

                if columns:
                    dataframe = self.spark.read.jdbc(self.data_origin_url, name).select(
                        columns
                    )
                else:
                    dataframe = self.spark.read.jdbc(self.data_origin_url, name)
                dataframe.write.parquet(f"{self.datalake}/{name}", mode="overwrite")

        except FileNotFoundError:
            print_error("File tables.csv not found on workdir, please create it")

        except IndexError:
            print_error("Invalid csv (Comma separated values) format, check the file")

        except AnalysisException as e:
            print(vars(e))

        except Py4JJavaError as e:
            print(e)
            print_error(f"Table {name} not found")

        print_success("Datalake created")

    def process_chunck(self, chunck):
        return chunck.etl(self.datalake, self.warehouse)

    def execute(self, modules: list[ModuleType]):
        print_success("Creating dataWarehouse")
        if not path.exists(self.warehouse):
            os.mkdir(self.warehouse)

        if not all(isinstance(mod, ModuleType) for mod in modules):
            print_error(
                "modules arg must be a list of modules (ModuleType) from worker.execute()"
            )

        # for i in range(10):
        #     print_success("executing module")
        #     modules[0].etl(self.datalake, self.warehouse)

        chunks: list[ModuleType] = np.array_split(modules, self.cpu)

        for chunk in chunks:
            p = Process(target=self.process_chunck, args=(chunk,))
            p.start()

    def test(self):
        datalake_memers = os.listdir(path.join(self.workdir, "datalake"))
        print(datalake_memers)

    def load_data(self, database: DB):
        self.data_destiny_url = database.url

        if self.data_destiny_url == self.data_origin_url:
            print_error("Origin DB and destiny DB are the same")

        # try:
        #     tables = [
        #         [table, path.join(self.warehouse, table)]
        #         for table in os.listdir(self.warehouse)
        #     ]

        #     for table_path in tables:
        #         name = table_path[0]
        #         df = self.spark.read.parquet(table_path)
        #         df.write.jdbc(self.data_destiny_url, name, mode="overwrite")

        #     os.removedirs(self.datalake, self.warehouse)

        # except FileNotFoundError:
        #     print_error("Folder warehouse not found, use worker.execute to create it")

        shutil.rmtree(self.datalake)
        shutil.rmtree(self.warehouse)
