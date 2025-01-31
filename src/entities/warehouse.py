from abc import ABC, abstractmethod



class WareHouse(ABC):
    def __init__(self, folder: str):
        self.folder = folder

    def extract(self):
        print("holaaaaaa")

    @abstractmethod
    def transform(self):
        pass

    def load(self):
        print("loading")


class Enfermeria(WareHouse):
    def __init__(self, name):
        super().__init__(name)

    def transform(self):
        print("esta modificada")


# Esto ahora obliga a que transform sea implementado:
enfermeria = Enfermeria("Enfermeria Dashboard")
enfermeria.transform()
