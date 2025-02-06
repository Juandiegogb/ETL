from concurrent.futures import ProcessPoolExecutor
from time import sleep


def test():
    sleep(1)
    print("hola")


def main():
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(test) for i in range(8)]


def slow():
    for i in range(8):
        test()


if __name__ == "__main__":
    slow()
    # main()
