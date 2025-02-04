from colorama import init, Fore, Style

init()


def print_error(message: str):
    print(Fore.RED, message, Style.RESET_ALL)
    exit(1)


def print_success(message: str):
    print(Fore.GREEN, message, Style.RESET_ALL)
