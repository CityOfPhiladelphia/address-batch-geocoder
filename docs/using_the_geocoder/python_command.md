---
description: Instructions on how to use the Python CLI for the Address Batch Geocoder
icon: lucide/terminal
---

# Running a Python Command

Note that this method is not currently recommended, as it requires more setup, and does not automatically check for address file updates, and requires the user to manually convert a downloaded address file to a parquet. If you choose to run the program this way, you should periodically repeat steps 3-5 below to keep the address file up to date.

Package management is handled using uv, instead of pip. You can read more about uv here: https://docs.astral.sh/uv/

## Installing uv:
Follow the instructions for your machine for installing uv here, if you have not installed it already: https://docs.astral.sh/uv/getting-started/installation/

## Installing dependencies:
1. Navigate to the package directory if not already there: `cd address-batch-geocoder`
2. run `uv sync --no-group dev` to install the proper Python version and all package dependencies. This will create a virtual environment at `.venv`. If you intend to develop on the project, `run uv sync --group dev` or `uv sync --all-groups` if you intend to contribute to the documents as well.
3. Next, you will need to download the address file. This can be accessed via a public s3 bucket. It's best if you save this to a subfolder titled `address_geocoder_data`:
```
mkdir address_geocoder_data
curl -L -O --output-dir ./address_geocoder_data/ "https://opendata-downloads.s3.amazonaws.com/address_service_area_summary_public.csv.gz"
```
4. Unzip the file:
```
gunzip address_geocoder_data/address_service_area_summary_public.csv.gz
```
5. The file will need to be converted to a parquet file in order to work with the geocoder. You can do this by running the `csv_to_parquet.py` file in the repo.
```
uv run csv_to_parquet.py --input_path=address_geocoder_data/address_service_area_summary_public.csv --output_path=address_geocoder_data/address_service_area_summary_public.parquet
```
### Configuration and Running
6. Once you've installed the geocoder and its dependencies, you're ready to run it. You will need to configure a config.yml file. 
    
    You can use config_example.yml as a template. See [Using the Yaml Config](./yaml_config) for instructions on how to configure this file.

7. Run

    Once the file is configured, run:
    ```
    uv run geocoder.py --config_path=[PATH_TO_YOUR_CONFIG_FILE]
    ```
    Running ```uv run geocoder.py``` without the ```--config_path``` argument will default to ```config.yml```