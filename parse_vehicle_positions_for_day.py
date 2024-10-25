import csv
import logging
import os
import sys
import textwrap
import time

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gtfs_kit

from google.transit import gtfs_realtime_pb2

from utils import load_config, create_folder

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(filename)s:%(lineno)d| %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

VEHICLE_LOCATIONS_HEADER = [
    'location_ping',
    'service_date',
    'event_timestamp',
    'trip_id_performed',
    'trip_id_scheduled',
    'trip_stop_sequence',
    'scheduled_stop_sequence',
    'vehicle_id',
    'stop_id',
    'current_status',
    'latitude',
    'longitude',
    'heading',
    'speed',
    'schedule_relationship'
]


def generate_vehicle_ping_id(vehicle):
    try:
        s = f"""
        id:{vehicle.vehicle.id}_position:{vehicle.position.latitude},{vehicle.position.longitude}
        {vehicle.position.bearing}-{vehicle.position.speed}_T{vehicle.timestamp}
        """
        return textwrap.dedent(s).strip().replace('\n', '-')
    except Exception as e:
        print(vehicle)
        print(e)
        sys.exit()


def write_row(analysis_date_str, writer, ping_id, vehicle, trip_ids_on_analysis_date, agency_tz):
    # create default values for trip-less ping
    service_date = ''
    trip_id_performed = ''
    trip_id_scheduled = ''
    trip_stop_sequence = ''
    scheduled_stop_sequence = ''
    stop_id = ''
    current_status = ''
    schedule_relationship = ''

    event_dt = datetime.fromtimestamp(vehicle.timestamp, tz=agency_tz)

    # check for trip
    trip_id = vehicle.trip.trip_id
    if trip_id != '':
        service_date = analysis_date_str
        trip_id_performed = trip_id
        trip_stop_sequence = vehicle.current_stop_sequence
        stop_id = vehicle.stop_id
        current_status = vehicle.current_status
        schedule_relationship = vehicle.trip.schedule_relationship

        if trip_id in trip_ids_on_analysis_date:
            trip_id_scheduled = trip_id
            scheduled_stop_sequence = trip_stop_sequence

    # write row
    writer.writerow([
        ping_id,
        service_date,
        event_dt.isoformat(),
        trip_id_performed,
        trip_id_scheduled,
        trip_stop_sequence,
        scheduled_stop_sequence,
        vehicle.vehicle.id,
        stop_id,
        current_status,
        vehicle.position.latitude,
        vehicle.position.longitude,
        vehicle.position.bearing,
        vehicle.position.speed,
        schedule_relationship
    ])


def main():
    script_start_time = time.time()
    config = load_config('parse_vehicle_positions_for_day.py')
    raw_data_folder = config['raw_data_path']
    analysis_date_str = config['date']
    gtfs_kit_analysis_date = analysis_date_str.replace('-', '')
    date_format = "%Y-%m-%d"
    analysis_date_obj = datetime.strptime(analysis_date_str, date_format)
    tides_output_folder = config['tides_output_folder']
    vehicle_positions_folder = os.path.join(raw_data_folder, analysis_date_str, 'vehicle_positions_url')

    logger.info(f"Loading GTFS Schedule data for {analysis_date_str}")
    schedule_feed = gtfs_kit.read_feed(
        os.path.join(raw_data_folder, analysis_date_str, 'schedule', 'gtfs.zip'),
        'm'
    )
    first_agency_tz = ZoneInfo(schedule_feed.agency.iloc[0].agency_timezone)

    # get the trips and start and end time for the analysis date
    logger.info(f"Getting trips for date {analysis_date_str}")
    trips_on_analysis_date = schedule_feed.get_trips(gtfs_kit_analysis_date)
    trip_ids_on_analysis_date = trips_on_analysis_date['trip_id'].tolist()
    trip_stats = gtfs_kit.compute_trip_stats(schedule_feed)
    trip_stats_for_analysis_date = trip_stats[trip_stats['trip_id'].isin(trip_ids_on_analysis_date)]
    [start_time, end_time] = schedule_feed.get_start_and_end_times(gtfs_kit_analysis_date)
    start_seconds = gtfs_kit.timestr_to_seconds(start_time)
    end_seconds = gtfs_kit.timestr_to_seconds(end_time)
    analysis_start_datetime = analysis_date_obj + timedelta(seconds=start_seconds) - timedelta(hours=2)
    analysis_start_datetime = analysis_start_datetime.replace(tzinfo=first_agency_tz)
    analysis_start_timestamp = analysis_start_datetime.timestamp()
    analysis_end_datetime = analysis_date_obj + timedelta(seconds=end_seconds) + timedelta(hours=2)
    analysis_end_datetime = analysis_end_datetime.replace(tzinfo=first_agency_tz)
    analysis_end_timestamp = analysis_end_datetime.timestamp()
    files_end_seconds = 86400

    # iterate through downloaded raw GTFS-RT vehicle positions for the analysis date
    logger.info('queueing up files for analysis date')
    files = list(map(lambda x: os.path.join(vehicle_positions_folder, x), os.listdir(vehicle_positions_folder)))

    # add extra days as needed and as available
    while files_end_seconds < end_seconds:
        files_date_obj = analysis_date_obj + timedelta(seconds=files_end_seconds)
        cur_date_str = files_date_obj.strftime(date_format)
        next_dir = os.path.join(raw_data_folder, cur_date_str, 'vehicle_positions_url')
        logger.info(f"Also adding some files for {cur_date_str} for late/long trips")
        if os.path.exists(next_dir):
            files.extend(map(lambda x: os.path.join(next_dir, x), os.listdir(next_dir)))
        else:
            break
        files_end_seconds += 86400

    files.sort()

    # create a dictionary for storing pings
    pings = dict()
    num_pings = 0

    # open output file and write data as it comes
    create_folder(os.path.join(tides_output_folder, analysis_date_str))
    output_path = os.path.join(tides_output_folder, analysis_date_str, 'vehicle_locations.csv')
    with open(output_path, mode='w', newline='') as file:
        writer = csv.writer(file)

        # write header
        writer.writerow(VEHICLE_LOCATIONS_HEADER)

        for rt_file in files:
            # open rt file
            logger.info(f"Parsing file {rt_file}")
            with open(rt_file, 'rb') as f:
                message = gtfs_realtime_pb2.FeedMessage()
                message.ParseFromString(f.read())

            # determine if message should be analyzed
            # don't analyze if earlier than analysis start time
            if message.header.timestamp < analysis_start_timestamp:
                logger.info("Skipping file, before start of analysis timeperiod")
                continue

            # if after analysis end time, stop analyzing files
            if message.header.timestamp > analysis_end_timestamp:
                logger.info("Reached end of analysis timeperiod")
                break

            for vehicle_entity in message.entity:
                # check if vehicle ping has been observed before
                vehicle = vehicle_entity.vehicle
                ping_id = generate_vehicle_ping_id(vehicle)
                if ping_id not in pings:
                    pings[ping_id] = True
                    write_row(analysis_date_str, writer, ping_id, vehicle, trip_ids_on_analysis_date, first_agency_tz)
                    num_pings += 1
                    if num_pings % 100 == 0:
                        print(f"Found {num_pings} pings")
                    if vehicle.trip.start_date != '':
                        print(vehicle.trip)
                        sys.exit()

    script_end_time = time.time()
    script_elapsed_time = script_end_time - script_start_time
    logger.info(f"Finished analyzing vehicle data, found {num_pings} total pings")
    logger.info(f"Parsing data for {analysis_date_str} took {script_elapsed_time:.4f} seconds")


if __name__ == '__main__':
    main()
