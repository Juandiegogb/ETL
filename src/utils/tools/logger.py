from datetime import datetime

current_datetime = datetime.now().strftime("%d-%m-%Y at %H:%M")


class Logger:
    def __init__(self):
        pass


# success = 0
# errors = 0

# with open(path.join(logs_folder, "errors.txt"), "a") as file:
#     file.write("\n" + "-" * 100)
#     file.write(f"\nEXECUTION TIME: {current_datetime}\n")

#     for module in modules:
#         try:
#             module.etl()
#             success += 1
#         except Py4JJavaError as e:
#             errors += 1
#             file.write(f"\tError executing {module.__name__}.py {e.java_exception}\n")

#     file.write(f"{success} modules executed successfully\n")
#     file.write(f"{errors} modules raises an error\n")
