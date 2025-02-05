from datetime import datetime


def etl(algo, warehouse):
    with open(f"{warehouse}/{datetime.now()}.txt") as file:
        for i in range(10000):
            file.write(f"{i}\n")
