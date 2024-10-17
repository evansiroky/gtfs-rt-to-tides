# gtfs-rt-to-tides
A set of scripts to turn collected GTFS-RT into TIDES data

## Usage

1. Download data, potentially continuously.

```bash
python downloader.py config/example_download_config.json
```

2. Process raw data into TIDES Vehicle Locations

```shell
python parse_for_day.py config/example_vehicle_locations_parser_config.json
```