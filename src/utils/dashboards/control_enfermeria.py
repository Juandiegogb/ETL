import vaex
from os import PathLike
from os import path
import os

def etl(workdir: PathLike, warehouse: PathLike):
    df = vaex.open(path.join(workdir, "ADGLOSAS", "*.parquet"))
    df.export_parquet(path.join(warehouse, "adglosas.parquet"))
    print("hello from module")

    # transact_dev = spark.read.jdbc(origin_db, "Hosvital.TransactDevicesPivot")

    # df_joined = tenfermeria.join(
    #     transact_dev,
    #     (tenfermeria["Tipo_Documento"] == transact_dev["ResTDoPac"])
    #     & (tenfermeria["Numero_Documento"] == transact_dev["ResDocPac"])
    #     & (
    #         lower(tenfermeria["Nombre_razon_social"])
    #         == lower(transact_dev["EmpRazSoc"])
    #     )  # Normalización para insensibilidad
    #     & (tenfermeria["HISCSEC"] == transact_dev["ResFolPac"]),
    #     "left",
    # )

    # df_joined.write.jdbc(destiny_db, "pyspark_tenfermeria" , mode="overwrite")



""""
dimPatient :traer datos de personas from CAPBAS
dimDiag : diagnosticos from maedia => traer datos agrupados de AGRPDIAG
dimPavilion : traer datos from MAEPAB
dimOperator : traer datos from empress
dimConsultingRoom : traer datos from consul consul1
dimContract : traer datos from MAEEMP
dimBussines group : traer datos from EMPRESA
dimHeadquarters : traer datos from MAESED

"""