### A script to continually download and save multiple GTFS files to folders organized by
# feed and date

import logging
import os
import shutil
import sys
import time

from datetime import datetime

import requests
import schedule
import pytz

from gtfs_functions import Feed
from utils import create_folder, load_config


feeds = {}
save_folder = 'saved_data'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

RT_URLS = ['service_alerts_url', 'trip_updates_url', 'vehicle_positions_url']

def download_file(url, save_filename):
    logging.info(f"Downloading from {url}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check if the request was successful

        with open(save_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        logging.info(f"File saved successfully to {save_filename}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download from URL ({url}): {e}")


def download_and_process_config():
    global feeds, save_folder
    config = load_config('downloader.py')
    save_folder = config['save_folder']
    create_folder(save_folder)

    found_feeds = set()
    fetch_timezones = set()

    for feed_config in feeds.values():
        fetch_timezones.add(feed_config['timezone'])

    for name, urls in config['feeds'].items():
        found_feeds.add(name)
        if name not in feeds:
            logging.info(f"Processing new feed: {name}")
            feeds[name] = {}

            # create feed save folder
            feed_save_folder = os.path.join(save_folder, name)
            create_folder(feed_save_folder)

            # download initial schedule feed
            initial_gtfs_schedule_path = os.path.join(feed_save_folder, 'init_gtfs_schedule.zip')
            download_file(urls['schedule_url'], initial_gtfs_schedule_path)

            # add feed to the timezone the first agency is associated with
            feed = Feed(initial_gtfs_schedule_path)
            print(feed.agency[0])
            sys.exit()
            agency_timezone = feed.agency[0].agency_timezone
            print(agency_timezone)
            feeds[name]['timezone'] = agency_timezone

            if agency_timezone not in fetch_timezones:
                # schedule
                schedule.every().day.at('2:30', agency_timezone).do(
                    download_schedule_files_for_timezone, timezone=agency_timezone)
                fetch_timezones.add(agency_timezone)

            # copy initial file to appropriate date folder
            current_date = datetime.now(pytz.timezone(agency_timezone)).date()
            schedule_folder = os.path.join(feed_save_folder, current_date, 'schedule')
            create_folder(schedule_folder)
            shutil.copy(initial_gtfs_schedule_path, os.path.join(schedule_folder, 'gtfs.zip'))

        feeds[name]['urls'] = urls

    # remove feeds no longer present
    for feed_name in feeds.keys():
        if feed_name not in found_feeds:
            del feeds[feed_name]
    

def download_rt_files():
    global feeds, save_folder

    for name, feed_config in feeds.items():
        now = datetime.now(pytz.timezone(feed_config['timezone']))
        formatted_request_time = f"{now:%Y-%m-%d-%H-%M-%S}"
        feed_date = now.date()
        feed_date_save_folder = os.path.join(save_folder, name, feed_date)

        for url_type in RT_URLS:
            if url_type in feed_config['urls']:
                feed_date_url_type_save_folder = os.path.join(feed_date_save_folder, url_type)
                create_folder(feed_date_url_type_save_folder)
                download_file(
                    feed_config['urls'][url_type],
                    os.path.join(feed_date_url_type_save_folder, f"{formatted_request_time}.pb")
                )


def download_schedule_files_for_timezone(timezone):
    global feeds, save_folder

    now = datetime.now(pytz.timezone(timezone))
    feed_date = now.date()

    for name, feed_config in feeds.items():
        if feed_config['timezone'] != timezone:
            continue

        feed_date_url_type_save_folder = os.path.join(save_folder, name, feed_date, 'schedule')
        create_folder(feed_date_url_type_save_folder)
        download_file(
            feed_config['urls']['schedule'],
            os.path.join(feed_date_url_type_save_folder, 'gtfs.zip')
        )


def main():
    # first load and process config to save initial schedule files and also schedule future
    # schedule feed downloads
    download_and_process_config()

    # schedule rt file downloads for every 20 seconds
    schedule.every().minute.at(':00').do(download_rt_files)
    schedule.every().minute.at(':20').do(download_rt_files)
    schedule.every().minute.at(':40').do(download_rt_files)

    # update config every minute
    schedule.every().minute.at(':45').do(download_and_process_config)
    
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()