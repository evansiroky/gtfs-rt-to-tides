# gtfs-rt-to-tides
A set of scripts to turn collected GTFS-RT into TIDES data

## Usage

### 1. Download data, potentially continuously.

```bash
python downloader.py config/example_download_config.json
```

### 2. Process raw data into TIDES Data

#### Raw GTFS-RT Vehicle Positions data > TIDES Vehicle Locations csv

```shell
python parse_vehicle_positions_for_day.py config/example_vehicle_locations_parser_config.json
```

#### Raw GTFS-RT Trip Updates data > TIDES Trips Performed csv

```shell
python parse_trip_updates_for_day.py config/example_trip_updates_parser_config.json
```