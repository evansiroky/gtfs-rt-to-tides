import json
import os
import sys

def create_folder(folder_name):
    try:
        os.makedirs(folder_name)
    except:
        pass

def load_config(callable_filname):
    if len(sys.argv) != 2:
        print(f"Usage: python {callable_filname} <path_to_config>")
        sys.exit(1)
    with open(sys.argv[1], 'r') as f:
        return json.load(f)