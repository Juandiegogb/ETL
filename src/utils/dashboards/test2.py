from time import time


def etl(algo, warehouse):
    with open(f"{warehouse}/{time()}.txt") as file:
        for i in range(10000):
            file.write(f"{i}\n")
