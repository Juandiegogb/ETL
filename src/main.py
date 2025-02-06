from utils.entities.worker import Worker
from dotenv import load_dotenv
from utils.entities.db import DB
from utils.dashboards import test


modules = [test, test, test, test, test, test, test, test, test]


def main():
    load_dotenv()

    origin_db = DB("ORIGIN_DB")
    destiny_db = DB("TEST_BI")
    stage_db = DB("STAGE_DB")

    worker: Worker = Worker()
    # worker.create_datalake(origin_db)
    # worker.load_data(origin_db)

    # worker.test()
    worker.execute(modules)
    # worker.load_data()


def test():
    for i in modules:
        i.etl(" fasf", "C:\ImperiumBI\warehouse")


if __name__ == "__main__":
    main()
