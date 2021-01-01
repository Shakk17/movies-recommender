from colorama import Fore, Style


def blue(string):
    return f"{Fore.CYAN}{string}{Style.RESET_ALL}"


def red(string):
    return f"{Fore.RED}{string}{Style.RESET_ALL}"


def green(string):
    return f"{Fore.GREEN}{string}{Style.RESET_ALL}"


def yellow(string):
    return f"{Fore.YELLOW}{string}{Style.RESET_ALL}"


def magenta(string):
    return f"{Fore.MAGENTA}{string}{Style.RESET_ALL}"
