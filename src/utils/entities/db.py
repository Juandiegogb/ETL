from json import loads, JSONDecodeError
from os import getenv
from utils.tools.custom_print import print_error
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


class DB:
    def __init__(self, variable_name: str):
        self.url: str
        self.db_name: str
        self.db_host: str

        env_variable = getenv(variable_name)

        if env_variable:
            try:
                db: DB = loads(env_variable)
            except JSONDecodeError as e:
                raise ValueError(f"Error decoding ->  {variable_name} variable") from e

        else:
            print_error(f"Missing variable : {variable_name} ")

        required_keys: set = {"host", "db_name", "user", "pwd", "port"}
        if not required_keys.issubset(db.keys()):
            print_error(f"missing keys {required_keys - db.keys()}")

        host = db["host"]
        db_name = db["db_name"]
        user = db["user"]
        pwd = db["pwd"]
        port = db["port"]

        test_connection_url = f"mssql+pymssql://{user}:{pwd}@{host}/?charset=utf8"
        try:
            create_engine(test_connection_url).connect()
        except OperationalError:
            print_error(f"Login failed with {variable_name} credentials")
        self.db_host = host
        self.db_name = db_name
        self.url = f"jdbc:sqlserver://{host}:{port};databaseName={db_name};user={user};password={pwd};encrypt=false;"
