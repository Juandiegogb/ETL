from utils.entities.worker import Worker
from dotenv import load_dotenv
from utils.entities.db import DB
from utils.dashboards import control_enfermeria, ingreso_pacientes

load_dotenv()

origin_db = DB("ORIGIN_DB")
destiny_db = DB("TEST_BI")
stage_db = DB("STAGE_DB")

modules = [control_enfermeria, ingreso_pacientes]


worker: Worker = Worker()
# worker.create_datalake(origin_db)
# worker.execute(modules)
worker.load_data()
