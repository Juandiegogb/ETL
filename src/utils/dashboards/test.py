from time import time


def etl(algo, warehouse):
    with open(f"{warehouse}/{round(time())}.txt") as file:
        for i in range(10000):
            file.write(f"{i}\n")
