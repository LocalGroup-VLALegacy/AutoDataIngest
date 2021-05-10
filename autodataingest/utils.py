
import os
from copy import copy

def uniquify(path):
    filename, extension = os.path.splitext(path)
    counter = 1

    while os.path.exists(path):
        path = f"{filename}_{str(counter)}{extension}"
        counter += 1

    return path


def uniquify_folder(path):

    counter = 1

    orig_path = copy(path)
    while os.path.exists(path):
        path = f"{orig_path}_{str(counter)}"
        counter += 1

    return path
