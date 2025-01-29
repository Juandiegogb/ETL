from . import spark
from utils.db import origin_db


def etl():
    df = spark.read.jdbc(origin_db, "tenfermeria")
    df.show(5, truncate=False)
