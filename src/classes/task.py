import os
from os import path
from abc import ABC, abstractmethod
from os import path
import vaex
from vaex.dataframe import DataFrameLocal

workdir = os.getenv("IMPERIUM_WORKDIR")


class Task(ABC):
    def __init__(self):
        self.datalake = path.join(workdir, "datalake")
        self.warehouse = path.join(workdir, "warehouse")

    def get_dataframe(self, table_name: str) -> DataFrameLocal:
        table_path = path.join(self.datalake, table_name, "*.parquet")
        df: DataFrameLocal = vaex.open(table_path)
        if df:
            return df

    @abstractmethod
    def execute_task(self):
        pass
