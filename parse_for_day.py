import logging
import os
import sys

import gtfs_kit

from utils import load_config, service_id_is_active_on_date, create_folder, get_trips_for_date

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(filename)s:%(lineno)d| %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def main():
    config = load_config('parse_for_day.py')
    raw_data_folder = config['raw_data_path']
    analysis_date_str = config['date']
    tides_output_folder = config['tides_output_folder']

    logger.info('loading GTFS Schedule data')
    schedule_feed = gtfs_kit.read_feed(
        os.path.join(raw_data_folder, analysis_date_str, 'schedule', 'gtfs.zip'),
        'm'
    )

    # get the trips and start and end time for the analysis date
    trips = schedule_feed.get_trips('20240704')
    [start_time, end_time] = schedule_feed.get_start_and_end_times('20240704')
    print(start_time, end_time)

    # create a dictionary for storing pings
    pings = dict()

    # iterate through downloaded raw GTFS-RT vehicle positions for the analysis date
    files = os.listdir(os.path.join(raw_data_folder, analysis_date_str, 'vehicle_positions_url'))
    files.sort()
    for rt_file in files:
        # open rt file
        print(rt_file)
        sys.exit()

        for vehicle in vehicles:
            # check if vehicle ping has been observed before
            pass

    create_folder(os.path.join(tides_output_folder, analysis_date_str))
    pings_df = pd.DataFrame.from_dict(pings, orient='index')
    pings_df.to_csv(os.path.join(tides_output_folder, analysis_date_str, 'vehicle_locations.csv'))


if __name__ == '__main__':
    main()
