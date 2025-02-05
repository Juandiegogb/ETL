from multiprocessing import Process
from time import sleep


def test(i):
    sleep(1)
    print(i)


class Worker:
    def __init__(self):
        pass

    def execute(self, modules: list[str]):
        for i in modules:
            p = Process(target=test, args=(i,))
            p.start()
            p.join()


if __name__ == "__main__":
    w = Worker()

    modules = ["h", "j", "j", "j", "j", "j", "j"]
    w.execute(modules)
