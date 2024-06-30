### A script to parse and analyze downloaded GTFS-RT trip updates files and 
### thus summarize the trips into a TIDES Trips Performed output file.

### The config is assumed to point to a folder with a single service day's worth
### of raw GTFS-RT data.

import csv
import os

from google.transit import gtfs_realtime_pb2
from utils import create_folder, load_config

def parse_file(file):
    with open(file, 'rb') as f:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(f.read())
        return feed
    
def process_trip_entity(observed_trips, file, entity):
    trip_id = entity.trip_update.trip.trip_id
            
    # detect if trip has been observed yet
    if trip_id not in observed_trips:
        # no record of trip, create it
        trip_record = { 'trip_id': trip_id }
    else:
        trip_record = observed_trips[trip_id]
    
    observed_trips[trip_id] = trip_record
    
def write_data(config, observed_trips):
    file_path = os.path.join(config['tides_output_folder'], 'output.csv')

    trips_data = [trip for trip in observed_trips.values()]
    headers = trips_data[0].keys()
    
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        
        # Write headers
        writer.writeheader()
        
        # Write rows
        for row in trips_data:
            writer.writerow(row)
        
        print(f"CSV file '{file_path}' created successfully.")
      
def main():
    config = load_config('parse_trips.py')
    
    create_folder(config['tides_output_folder'])
    
    observed_trips = {}
    
    # list files
    files = os.listdir(config['rt_folder'])
    
    # sort files (for now assuming files are chronologically sorted by alphanumeric sorting)
    files.sort()
    
    # for each file
    for file in files:
        # parse GTFS-RT file
        message = parse_file(os.path.join(config['rt_folder'], file))
        
        # analyze each entity
        for entity in message.entity:
            process_trip_entity(observed_trips, file, entity)
            
    write_data(config, observed_trips)

if __name__ == '__main__':
    main()