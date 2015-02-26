import os
import contextlib


@contextlib.contextmanager
def cd(new_path):
    """
    Change to a different directory within a limited context.
    """
    saved_path = os.getcwd()
    os.chdir(new_path)
    yield
    os.chdir(saved_path)
