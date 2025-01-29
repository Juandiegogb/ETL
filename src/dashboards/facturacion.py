from . import spark
from pyspark.sql.functions import lower
from utils.db import origin_db, destiny_db


def etl():
    tenfermeria = spark.read.jdbc(origin_db, "tenfermeria")

    transact_dev = spark.read.jdbc(origin_db, "Hosvital.TransactDevicesPivot")

    df_joined = tenfermeria.join(
        transact_dev,
        (tenfermeria["Tipo_Documento"] == transact_dev["ResTDoPac"])
        & (tenfermeria["Numero_Documento"] == transact_dev["ResDocPac"])
        & (
            lower(tenfermeria["Nombre_razon_social"])
            == lower(transact_dev["EmpRazSoc"])
        )  # Normalización para insensibilidad
        & (tenfermeria["HISCSEC"] == transact_dev["ResFolPac"]),
        "left",
    )

    df_joined.write(destiny_db, "pyspark_tenfermeria")
