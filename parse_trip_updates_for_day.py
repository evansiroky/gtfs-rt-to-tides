import csv
import logging
import os
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

TRIP_UPDATES_FOLDER_NAME = 'trip_updates_url'
TRIPS_PERFORMED_HEADER = [
    'service_date',
    'trip_id_performed',
    'vehicle_id',
    'trip_id_scheduled',
    'route_id',
    'shape_id',
    'block_id',
    'trip_start_stop_id',
    'trip_end_stop_id',
    'schedule_trip_start',
    'schedule_trip_end',
    'actual_trip_start',
    'actual_trip_end',
    'trip_type',
    'schedule_relationship'
]


def gtfs_to_datetime(date_obj, gtfs_time, tz):
    dt = date_obj + timedelta(seconds=gtfs_kit.timestr_to_seconds(gtfs_time))
    dt = dt.replace(tzinfo=tz)
    return dt


def main():
    script_start_time = time.time()
    config = load_config('parse_trip_updates_for_day.py')
    raw_data_folder = config['raw_data_path']
    analysis_date_str = config['date']
    gtfs_kit_analysis_date = analysis_date_str.replace('-', '')
    date_format = "%Y-%m-%d"
    analysis_date_obj = datetime.strptime(analysis_date_str, date_format)
    tides_output_folder = config['tides_output_folder']
    trip_updates_folder = os.path.join(raw_data_folder, analysis_date_str, TRIP_UPDATES_FOLDER_NAME)

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

    # iterate through downloaded raw GTFS-RT trip updates for the analysis date
    logger.info('queueing up files for analysis date')
    files = list(map(lambda x: os.path.join(trip_updates_folder, x), os.listdir(trip_updates_folder)))

    # add extra days as needed and as available
    while files_end_seconds < end_seconds:
        files_date_obj = analysis_date_obj + timedelta(seconds=files_end_seconds)
        cur_date_str = files_date_obj.strftime(date_format)
        next_dir = os.path.join(raw_data_folder, cur_date_str, TRIP_UPDATES_FOLDER_NAME)
        logger.info(f"Also adding some files for {cur_date_str} for late/long trips")
        if os.path.exists(next_dir):
            files.extend(map(lambda x: os.path.join(next_dir, x), os.listdir(next_dir)))
        else:
            break
        files_end_seconds += 86400

    files.sort()

    # create a dictionary for storing trips
    found_trips = dict()

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

        for trip_update_entity in message.entity:
            if trip_update_entity.trip_update.trip.trip_id not in found_trips:
                print(f"Found new trip with id `{trip_update_entity.trip_update.trip.trip_id}`")
                # create new trip record
                scheduled_trip_stats = trip_stats_for_analysis_date[
                    trip_stats_for_analysis_date['trip_id'] == trip_update_entity.trip_update.trip.trip_id
                ]
                if not scheduled_trip_stats.empty:
                    scheduled_trip_stats = scheduled_trip_stats.to_dict(orient='records')[0]
                    route_id = trip_update_entity.trip_update.trip.route_id or scheduled_trip_stats['route_id']
                    trip_performed = {
                        'service_date': analysis_date_str,
                        'trip_id_performed': trip_update_entity.trip_update.trip.trip_id,
                        'vehicle_id': trip_update_entity.trip_update.vehicle.id,
                        'trip_id_scheduled': trip_update_entity.trip_update.trip.trip_id,
                        'route_id': route_id,
                        'shape_id': scheduled_trip_stats['shape_id'],
                        'trip_start_stop_id': None,  # to be filled in later by subsequent RT data
                        'trip_end_stop_id': None,  # to be filled in later by subsequent RT data
                        'schedule_trip_start': gtfs_to_datetime(
                            analysis_date_obj,
                            scheduled_trip_stats['start_time'],
                            first_agency_tz
                        ).isoformat(),
                        'schedule_trip_end': gtfs_to_datetime(
                            analysis_date_obj,
                            scheduled_trip_stats['end_time'],
                            first_agency_tz
                        ).isoformat(),
                        'actual_trip_start': None,  # to be filled in later by subsequent RT data
                        'actual_trip_end': None,  # to be filled in later by subsequent RT data
                        'trip_type': None,  # to be filled in later by subsequent RT data
                        'schedule_relationship': None,  # to be filled in later by subsequent RT data
                        '_lowest_stop_sequence': float('inf'),
                        '_highest_stop_sequence': float('-inf')
                    }
                else:
                    trip_performed = {
                        'service_date': analysis_date_str,
                        'trip_id_performed': trip_update_entity.trip_update.trip.trip_id,
                        'vehicle_id': trip_update_entity.trip_update.vehicle.id,
                        'trip_id_scheduled': None,
                        'route_id': trip_update_entity.trip_update.trip.route_id,
                        'shape_id': None,
                        'trip_start_stop_id': None,  # to be filled in later by subsequent RT data
                        'trip_end_stop_id': None,  # to be filled in later by subsequent RT data
                        'schedule_trip_start': None,
                        'schedule_trip_end': None,
                        'actual_trip_start': None,  # to be filled in later by subsequent RT data
                        'actual_trip_end': None,  # to be filled in later by subsequent RT data
                        'trip_type': None,  # to be filled in later by subsequent RT data
                        'schedule_relationship': None,  # to be filled in later by subsequent RT data
                        '_lowest_stop_sequence': float('inf'),
                        '_highest_stop_sequence': float('-inf')
                    }
                found_trips[trip_update_entity.trip_update.trip.trip_id] = trip_performed
            else:
                trip_performed = found_trips[trip_update_entity.trip_update.trip.trip_id]

            trip_performed['schedule_relationship'] = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                trip_update_entity.trip_update.trip.schedule_relationship
            ).capitalize()

            for stop_time_update in trip_update_entity.trip_update.stop_time_update:
                timestamp = stop_time_update.arrival.time or stop_time_update.departure.time
                if stop_time_update.stop_sequence <= trip_performed['_lowest_stop_sequence']:
                    # overwrite stats about the start of the trip
                    trip_performed['_lowest_stop_sequence'] = stop_time_update.stop_sequence
                    trip_performed['trip_start_stop_id'] = stop_time_update.stop_id
                    trip_performed['actual_trip_start'] = datetime.fromtimestamp(timestamp, tz=first_agency_tz).isoformat()

                if stop_time_update.stop_sequence >= trip_performed['_highest_stop_sequence']:
                    # overwrite stats about the start of the trip
                    trip_performed['_highest_stop_sequence'] = stop_time_update.stop_sequence
                    trip_performed['trip_end_stop_id'] = stop_time_update.stop_id
                    trip_performed['actual_trip_end'] = datetime.fromtimestamp(timestamp, tz=first_agency_tz).isoformat()

    # post-processing
    for trip_performed in found_trips.values():
        if trip_performed['schedule_relationship'] == 'Canceled':
            pass  # placeholder for later?
        elif trip_performed['schedule_relationship'] == 'Deleted':
            del found_trips[trip_performed['trip_id']]
        else:
            trip_performed['trip_type'] = 'In service'

    # add missing trips
    for trip_id in trip_ids_on_analysis_date:
        if trip_id not in found_trips:
            scheduled_trip_stats = trip_stats_for_analysis_date[
                trip_stats_for_analysis_date['trip_id'] == trip_id
            ]
            scheduled_trip_stats = scheduled_trip_stats.to_dict(orient='records')[0]
            found_trips[trip_id] = {
                'service_date': analysis_date_str,
                'trip_id_performed': trip_id,
                'vehicle_id': None,
                'trip_id_scheduled': trip_id,
                'route_id': scheduled_trip_stats['route_id'],
                'shape_id': scheduled_trip_stats['shape_id'],
                'trip_start_stop_id': None,
                'trip_end_stop_id': None,
                'schedule_trip_start': gtfs_to_datetime(
                    analysis_date_obj,
                    scheduled_trip_stats['start_time'],
                    first_agency_tz
                ).isoformat(),
                'schedule_trip_end': gtfs_to_datetime(
                    analysis_date_obj,
                    scheduled_trip_stats['end_time'],
                    first_agency_tz
                ).isoformat(),
                'actual_trip_start': None,
                'actual_trip_end': None,
                'trip_type': None,
                'schedule_relationship': 'Missing'
            }

    # write output data
    create_folder(os.path.join(tides_output_folder, analysis_date_str))
    output_path = os.path.join(tides_output_folder, analysis_date_str, 'trips_performed.csv')
    logger.info(f"Writing TIDES data to {output_path}")
    with open(output_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=TRIPS_PERFORMED_HEADER)
        writer.writeheader()
        for row in found_trips.values():
            filtered_row = {key: row[key] for key in TRIPS_PERFORMED_HEADER if key in row}
            writer.writerow(filtered_row)

    logger.info(f"Finished writing TIDES data")

    script_end_time = time.time()
    script_elapsed_time = script_end_time - script_start_time
    logger.info(f"Parsing data for {analysis_date_str} took {script_elapsed_time:.4f} seconds")


if __name__ == '__main__':
    main()
