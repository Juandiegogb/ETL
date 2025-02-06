from classes.task import Task
from vaex.dataframe import DataFrameLocal
import vaex
from os import path


class Control_enfermeria(Task):
    def __init__(self):
        super().__init__()

    def execute_task(self):
        personas = self.get_dataframe("ADGLOSAS")
        print(personas.schema())
