import json
import os
import sys


def create_folder(folder_name):
    try:
        os.makedirs(folder_name)
    except:
        pass


def load_config(callable_filename):
    if len(sys.argv) != 2:
        print(f"Usage: python {callable_filename} <path_to_config>")
        sys.exit(1)
    with open(sys.argv[1], 'r') as f:
        return json.load(f)
