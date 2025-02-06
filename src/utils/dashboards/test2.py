from time import time
from os import path


def etl(algo, warehouse):
    print("hola from etl")
    path_file = path.join(warehouse, f"{round(time())}.txt")
    with open(path_file, "a") as file:
        for i in range(1000000):
            file.write(f"{i}\n")


