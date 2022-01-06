
from argparse import ArgumentTypeError

def alnum(x: str) -> str:
    if not x.isalnum():
        raise ArgumentTypeError('ID must be alphanumeric')
    return x