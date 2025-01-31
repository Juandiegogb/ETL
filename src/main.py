from types import ModuleType

# from dashboards import control_enfermeria, ingreso_pacientes
# from os import path
# from utils.config import current_datetime, logs_folder
# from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from utils.config import get_url
from entities.worker import Worker


origin_db = get_url("ORIGIN_DB")
destiny_db = get_url("TEST_BI")
spark: SparkSession = (
    SparkSession.builder.appName("ETL")
    .master("local[*]")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    .getOrCreate()
)


worker = Worker(spark)
worker.create_warehouse("static/tables.csv", origin_db)


# modules: list[ModuleType] = [control_enfermeria, ingreso_pacientes]


# success = 0
# errors = 0

# with open(path.join(logs_folder, "errors.txt"), "a") as file:
#     file.write("\n" + "-" * 100)
#     file.write(f"\nEXECUTION TIME: {current_datetime}\n")

#     for module in modules:
#         try:
#             module.etl()
#             success += 1
#         except Py4JJavaError as e:
#             errors += 1
#             file.write(f"\tError executing {module.__name__}.py {e.java_exception}\n")

#     file.write(f"{success} modules executed successfully\n")
#     file.write(f"{errors} modules raises an error\n")


# spark.stop()
