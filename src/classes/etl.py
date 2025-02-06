from os import PathLike, cpu_count
from pyspark.sql import SparkSession
from os.path import join
from tools.utils import create_folder, check_workdir, check_databases
from classes.db import DB


class ETL:
    _cpu: int = cpu_count()
    _workdir: PathLike = check_workdir()
    _datalake: PathLike = join(_workdir, "datalake")
    _warehouse: PathLike = join(_workdir, "warehouse")
    _origin_db: DB = DB("ORIGIN_DB")
    _destiny_db: DB = DB("DESTINY_DB")

    _spark: SparkSession = (
        SparkSession.builder.appName("ETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(_cpu))
        .config("spark.executor.memory", "4g")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )
    _spark.sparkContext.setLogLevel("ERROR")

    create_folder(_datalake)
    create_folder(_warehouse)
    check_databases(_origin_db, _destiny_db)

    @property
    def spark(cls) -> SparkSession:
        return cls._spark

    @property
    def datalake(cls) -> PathLike:
        return cls._datalake

    @property
    def warehouse(cls) -> PathLike:
        return cls._warehouse

    @property
    def cpu(cls):
        return cls._cpu

    @property
    def origin_db(cls) -> str:
        return cls._origin_db

    @property
    def destiny_db(cls) -> str:
        return cls._destiny_db
