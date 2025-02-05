from utils.entities.worker import Worker
from dotenv import load_dotenv
from utils.entities.db import DB
from utils.dashboards import planta_de_personal


def main():
    load_dotenv()

    origin_db = DB("ORIGIN_DB")
    destiny_db = DB("TEST_BI")
    stage_db = DB("STAGE_DB")

    modules = [planta_de_personal]

    worker: Worker = Worker()
    worker.create_datalake(origin_db)
    # worker.load_data(origin_db)

    # worker.test()
    worker.execute(modules)
    # worker.load_data()


if __name__ == "__main__":
    main()
