### A script to continually download and save multiple GTFS files to folders organized by
# feed and date

import logging
import os
import shutil
import time

from datetime import datetime

import gtfs_kit
import requests
import schedule
import pytz

from utils import create_folder, load_config

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(filename)s:%(lineno)d| %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# constants
RT_URLS = ['service_alerts_url', 'trip_updates_url', 'vehicle_positions_url']

# globals
global_feeds = {}
global_save_folder = 'saved_data'


def download_file(url, save_filename):
    logger.info(f"Downloading from {url}")
    try:
        response = requests.get(url, timeout=10, stream=True)
        response.raise_for_status()  # Check if the request was successful

        with open(save_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        logger.info(f"File saved successfully to {save_filename}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download from URL ({url}): {e}")


def download_and_process_config():
    global global_feeds, global_save_folder

    logger.info('downloading and processing config')
    config = load_config('downloader.py')
    global_save_folder = config['save_folder']
    create_folder(global_save_folder)

    found_feed_names = set()
    fetch_timezones = set()

    for feed_config in global_feeds.values():
        fetch_timezones.add(feed_config['timezone'])

    for name, urls in config['feeds'].items():
        found_feed_names.add(name)
        if name not in global_feeds:
            logger.info(f"Processing new feed: {name}")
            global_feeds[name] = {}

            # create feed save folder
            feed_save_folder = os.path.join(global_save_folder, name)
            create_folder(feed_save_folder)

            # download initial schedule feed
            initial_gtfs_schedule_path = os.path.join(feed_save_folder, 'init_gtfs_schedule.zip')
            download_file(urls['schedule_url'], initial_gtfs_schedule_path)

            # add feed to the timezone the first agency is associated with
            feed = gtfs_kit.read_feed(initial_gtfs_schedule_path, 'm')

            agency_timezone = feed.agency.at[0, 'agency_timezone']
            global_feeds[name]['timezone'] = agency_timezone

            if agency_timezone not in fetch_timezones:
                # schedule
                schedule.every().day.at('02:30', agency_timezone).do(
                    download_schedule_files_for_timezone, timezone=agency_timezone)
                fetch_timezones.add(agency_timezone)

            # copy initial file to appropriate date folder
            now = datetime.now(pytz.timezone(agency_timezone))
            schedule_folder = os.path.join(feed_save_folder, f"{now:%Y-%m-%d}", 'schedule')
            create_folder(schedule_folder)
            shutil.copy(initial_gtfs_schedule_path, os.path.join(schedule_folder, 'gtfs.zip'))
            logger.info(f"Finished processing new feed: {name}")

        global_feeds[name]['urls'] = urls

    # remove feeds no longer present
    for feed_name in global_feeds.keys():
        if feed_name not in found_feed_names:
            logger.info(f"Removing feed: {feed_name}")
            del global_feeds[feed_name]

    logger.info('Finished processing config')
    

def download_rt_files():
    global global_feeds, global_save_folder

    logger.info('Begin downloading rt files')

    for name, feed_config in global_feeds.items():
        now = datetime.now(pytz.timezone(feed_config['timezone']))
        # add timestamp to account for daylight savings time transitions
        formatted_request_time = f"{int(now.timestamp())}-{now:%Y-%m-%d-%H-%M-%S}"
        feed_date_save_folder = os.path.join(global_save_folder, name, f"{now:%Y-%m-%d}")

        for url_type in RT_URLS:
            if url_type in feed_config['urls']:
                feed_date_url_type_save_folder = os.path.join(feed_date_save_folder, url_type)
                create_folder(feed_date_url_type_save_folder)
                download_file(
                    feed_config['urls'][url_type],
                    os.path.join(feed_date_url_type_save_folder, f"{formatted_request_time}.pb")
                )

    logger.info('Finished downloading rt files')


def download_schedule_files_for_timezone(timezone):
    global global_feeds, global_save_folder

    logger.info('Begin downloading schedule files')

    now = datetime.now(pytz.timezone(timezone))
    feed_date = f"{now:%Y-%m-%d}"

    for name, feed_config in global_feeds.items():
        if feed_config['timezone'] != timezone:
            continue

        feed_date_url_type_save_folder = os.path.join(global_save_folder, name, feed_date, 'schedule')
        create_folder(feed_date_url_type_save_folder)
        download_file(
            feed_config['urls']['schedule_url'],
            os.path.join(feed_date_url_type_save_folder, 'gtfs.zip')
        )

    logger.info('Finished downloading schedule files')


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
