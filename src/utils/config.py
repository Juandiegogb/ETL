"""This module read de env variables"""

from os import getenv
from json import loads, JSONDecodeError
from dotenv import load_dotenv
from entities.various import DB, Url_dict, Engines
from os import getcwd, path
import os
from datetime import datetime


load_dotenv()


cwd = getcwd()
logs_folder = path.join(cwd, "logs")
current_datetime = datetime.now().strftime("%d-%m-%Y at %H:%M")

if not path.exists(logs_folder):
    os.mkdir(logs_folder)


def get_url(variable_name: str) -> str:
    """Return url from json in env file"""
    env_variable = getenv(variable_name)

    if env_variable:
        try:
            db: DB = loads(env_variable)
        except JSONDecodeError as e:
            raise ValueError(f"Error decoding ->  {variable_name} variable") from e

    else:
        raise ValueError(f"Missing variable : {variable_name} ")

    required_keys: set = {"host", "db_name", "user", "pwd", "port", "engine"}
    if not required_keys.issubset(db.keys()):
        raise ValueError(f"missing keys {required_keys - db.keys()}")

    host = db["host"]
    db_name = db["db_name"]
    user = db["user"]
    pwd = db["pwd"]
    port = db["port"]
    engine: Engines = db["engine"]
    options: Url_dict = {
        "mssql": f"jdbc:sqlserver://{host}:{port};databaseName={db_name};user={user};password={pwd};encrypt=false;",
        "pg": f"jdbc:postgresql://{host}:{port}/{db_name}?user={user}&password={pwd}",
    }
    if engine not in options:
        raise ValueError(f"The db engine {engine} is not supported")

    return options[engine]
