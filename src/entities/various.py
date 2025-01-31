from typing import TypedDict, Literal


class DB(TypedDict):
    host: str
    db_name: str
    user: str
    pwd: str
    port: int
    engine: str


class Url_dict(TypedDict):
    mssql: str
    pg: str


Engines = Literal["mssql", "pg"]

