from shutil import rmtree
from os.path import exists
from os import PathLike, mkdir, getenv
from dotenv import load_dotenv
from tools.custom_print import print_error
from classes.db import DB

load_dotenv()


def create_folder(folder_name: PathLike):
    if exists(folder_name):
        rmtree(folder_name)
    else:
        mkdir(folder_name)


def check_workdir() -> str:
    workdir = getenv("IMPERIUM_WORKDIR")
    if not workdir:
        print_error("Missing environment variable IMPERIUM_WORKDIR")

    if not exists(workdir):
        print_error(f"Directory '{workdir}' not found")

    return workdir


def check_databases(origin_db: DB, destiny_db: DB):
    if origin_db.url == destiny_db.url:
        print_error("Origin db and destiny db are the same database")
