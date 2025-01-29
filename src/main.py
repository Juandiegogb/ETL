from types import ModuleType
from dashboards import facturacion, controlEnfermeria
from os import path
from utils.config import current_datetime, logs_folder
from py4j.protocol import Py4JJavaError
from dashboards import spark


modules: list[ModuleType] = [
    facturacion,
    controlEnfermeria,
    facturacion,
    controlEnfermeria,
    controlEnfermeria,
    facturacion,
    controlEnfermeria,
]


success = 0
errors = 0

with open(path.join(logs_folder, "errors.txt"), "a") as file:
    file.write("\n" + "-" * 100)
    file.write(f"\nEXECUTION TIME: {current_datetime}\n")

    for module in modules:
        try:
            module.etl()
            success += 1
        except Py4JJavaError as e:
            errors += 1
            file.write(f"\tError executing {module.__name__}.py {e.java_exception}\n")

    file.write(f"{success} modules executed successfully\n")
    file.write(f"{errors} modules raises an error\n")


spark.stop()