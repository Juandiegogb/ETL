from utils.entities.worker import Worker
from dotenv import load_dotenv
from utils.entities.db import DB
from os import PathLike

load_dotenv()

origin_db = DB("ORIGIN_DB")
destiny_db = DB("TEST_BI")
stage_db = DB("STAGE_DB")


worker: Worker = Worker()
workdir: PathLike = worker.getWorkdir()
# worker.create_datalake(origin_db)

worker.test()

# modules: list[ModuleType] = [control_enfermeria, ingreso_pacientes]
