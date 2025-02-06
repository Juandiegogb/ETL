from classes.worker import Worker


def main():
    origin_db = DB("ORIGIN_DB")
    destiny_db = DB("TEST_BI")

    worker: Worker = Worker()
    worker.create_datalake(origin_db)

    # worker.test()
    worker.execute(tasks)
    worker.load_data(destiny_db)


if __name__ == "__main__":
    # main()
    w = Worker()
    w.test()
