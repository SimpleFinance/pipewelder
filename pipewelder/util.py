import os
import contextlib
import json


@contextlib.contextmanager
def cd(new_path):
    """
    Change to a different directory within a limited context.
    """
    saved_path = os.getcwd()
    os.chdir(new_path)
    yield
    os.chdir(saved_path)


def load_json(filename):
    with open(filename) as f:
        try:
            data = json.load(f)
        except ValueError as e:
            raise ValueError("Unable to parse '{0}' as json; {1}"
                             .format(filename, e))
    return data
