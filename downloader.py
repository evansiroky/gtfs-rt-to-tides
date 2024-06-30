### A script continually download a single GTFS-RT URL file and save files to a folder

import json
import os
import requests
import sys
import time
from datetime import datetime, timedelta

from utils import create_folder, load_config

def sleep_until_next_polling_mark(config, next_polling_mark):
    if not next_polling_mark:
        next_polling_mark = datetime.now()
        next_polling_mark = next_polling_mark.replace(microsecond=0)
        next_polling_mark = next_polling_mark.replace(
          second=next_polling_mark.second - next_polling_mark.second % config['polling_interval']
        )
        next_polling_mark = next_polling_mark + timedelta(seconds=config['polling_interval'])
    
    delta = next_polling_mark - datetime.now()
    sleep_seconds = delta.total_seconds()
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
    
    return next_polling_mark + timedelta(seconds=config['polling_interval'])
    
def make_request(url, file_path):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check if the request was successful

        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"File saved successfully to {file_path}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download the file: {e}")

def main():
    config = load_config('downloader.py')
    next_polling_mark = None
    
    create_folder(config['save_folder'])
    
    while True:
        next_polling_mark = sleep_until_next_polling_mark(config, next_polling_mark)
        request_time = datetime.now()
        formatted_request_time = f"{request_time:%Y-%m-%d-%H-%M-%S}"
        print(f"making request for {formatted_request_time}")
        make_request(config['url'], os.path.join(config['save_folder'], formatted_request_time))

if __name__ == '__main__':
    main()