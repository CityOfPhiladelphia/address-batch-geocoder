# address-geocoder
A tool to standardize and geocode Philadelphia addresses

Address Geocoder takes an input file containing addresses 
and adds latitude and longitude to those addresses, as well as any optional
fields that the user supplies.

## How The Geocoder Works

`Address-Geocoder` processes a csv file with addresses, and geolocates those addresses using the following steps:

1. Takes an input file of addresses, and standardizes those addresses using `passyunk`, Philadelphia's address standardization system.
2. Compares the standardized data to a local parquet file, `addresses.parquet`, and adds the user-specified fields as well as latitude and longitude from that file
3. Not all records will match to the address file. For those records that do not match, `Address-Batch-Geocoder` queries the Address Information System (AIS) API and adds returned fields. Please note that this process can take some time, so processing large files with a messy address field is not recommended. As an example, if you have a file that needs 1,000 rows to be sent to AIS, this will take approximately 3-4 minutes.
4. Records that don't match to the AIS API are then queried against TomTom, which has different address parsing capabilities and is also able to return
5. Records that successfully match to TomTom are then rerun against AIS to try to recover enrichment fields, if those addresses are in philly
6. The enriched file is then saved to the same directory as the input file.

The release executable of the address geocoder automatically checks an s3 bucket for an updated version of the address file. The address file is published to s3 via airflow, using this DAG configuration: https://github.com/CityOfPhiladelphia/databridge-airflow-v2-configs/blob/main/citygeo/address_service_area_summary_public.yml.

## Questions?
If you have questions about the geocoder that this FAQ cannot answer, feel free to contact citygeo at: maps@phila.gov

## 1. Prerequisites
You will need the following things:
1. An executable file called `geocoder.exe`. This is used to run the program. Do not save the executable in a folder that has spaces in the name.	
2. An AIS API key, provided to you by CityGeo. Instructions on how to obtain this are below.

### Obtaining an AIS Key
To obtain a key:
1. Email ithelp@phila.gov to create a new support ticket, and copy maps@phila.gov on the email.
2. Request that IT Help route the ticket to CityGeo.
3. Mention that the AIS Key is to use the batch geocoder, and provide a link to this GitHub repository for context.


## Installation
First, you will need to download and install the geocoder.

The geocoder file can be downloaded from GitHub. The latest release can be found at: https://github.com/CityOfPhiladelphia/address-geocoder/releases/

Read through the notes carefully, and then download the zip file at the bottom of the readme. You may get a dangerous file blocked warning from Chrome. Override this block and download anyway.

Extract the zip folder into a folder where you can easily find it. When opening the zipped file, you may be prompted to either `extract` or `run`. Hit `extract`, not `run`, as the script will need to exist in an uncompressed directory in order to create the subfolders needed to work.

The folder **must not** have spaces in its name. The zip folder contains two files: `geocoder.exe` and `release.txt`. If you delete or rename these files, you will need to download them again or rename them back. Deleting `release.txt` will stop the program from being able to inform you if there is a new version of the `.exe` file that you need to download.

Double-clicking `geocoder.exe` will launch the program. You may see a popup that says "Windows protected your PC." This file is safe, so bypass this protection by clicking `More info,` and then selecting `Run anyway`.

As a first-time installation, the script will download Python and Git if not present, then download the geocoder from GitHub and install the proper dependencies. The geocoder will be downloaded to a folder called address-geocoder-main. If there are problems with your install, you may try deleting this folder and running `geocoder.exe` again.

Note that this script will attempt to install Python 3.11 on your machine if you do not have Python 3.11 installed on your machine.

The script will then attempt to download the address file. This may take a few minutes. It will save the address file and a version file in a subfolder called geocoder_address_data. Under most circumstances, you should not remove this folder or any of the files in it. Doing so will cause the script to redownload the address file.

After the installation runs successfully, you are ready to set up the configuration file.

## 2. How to Use Address Geocoder
### Three Ways to Use the Address Geocoder

There are three ways to use the address geocoder:

1. A locally-hosted web app with a **graphical user interface.**
2. Configuring a **.yml** file.
3. **Advanced:** Linux or MacOS users, running a **python command.**

### 2.1 The Graphical User Interface (GUI)

After checking for updates, `geocoder.exe` will prompt the user with two run options:
```
Choose an option:
[1] Run with the user-interface
[2] Run with the .yml config
[Any other key]: exit:
```

Press `1` to use the user interface. A window will open up in your default browser.

The user interface has the following fields.

1. The AIS API key. Required. Enter the AIS API key provided to you by CityGeo.
2. The CSV upload option. This is where you upload the file that you wish to enrich.
3. The SRIDs field: Choose which SRIDs you wish to geocode in. Required.
4. The enrichment fields: Choose which optional fields to add to your data. Optional.
5. Config file upload. If you don't want to select the same options very time (which can be tedious), you can optionally upload a pre-saved configuration file. You may additionally save the configuration you've chosen for future use, as well.

Once the required fields are entered, a geocode button will appear. You can geocode the file. Please do not close the browser while the geocoder is working, as you will be unable to download the results.

To close the geocoder, you will need to close both the browser window and the terminal window running geocoder.exe.

### 2.2 Yaml File Config

After checking for updates, `geocoder.exe` will prompt the user with two run options:

```
Choose an option:
[1] Run with the user-interface
[2] Run with the .yml config
[Any other key]: exit:
```

Press `2` to use the .yml config method. Before using this method, ensure that you have set up your configuration file. By default,
`Address Geocoder` searches for a file named `config.yml`. Detailed steps for filling out the config file are in the next section.

#### Configuration

1. The script should make a config.yml file if no config.yml file exists. If the script did not do this, you can simply copy `config_example.yml` to `config.yml` either in the file explorer by running in the terminal:
```
cp config_example.yml config.yml
```
Do not delete, rename, or move `config_example.yml`. If you delete this file, you will need to redownload it from GitHub. 
In most cases, it is not recommended to delete, rename, or move `config.yml`. If you rename this file, the geocoder will be unable to find it and will create a new config.yml.

2. Open up config.yml, and add your AIS API Key here:

```
AIS_API_KEY:
```
3. Add the filepath for the input file (the file that you wish to enrich), and the address file. The address file should have been automatically downloaded by `geocoder.exe`, and the correct path should be in the config file by default. This should look something like this. If using relative filepaths, filepaths are relative to the address-geocoder-main folder downloaded from GitHub. For ease of use, exact filepaths are recommended. Do not put the filenames in quotes:
```
input_file: ./data/example_input_4.csv
address_file: ./geocoder_address_data/address_service_area_summary.parquet
```
4. The geocoder writes data incrementally. If your previous geocoding session was interrupted before it finished, you have the option to resume that file. In that case, you will set:
```
resume: True
```
You will still need to provide the name of the non-geocoded input file as the input file. The partially geocoded file must exist in the same directory as the
input file, with the format {input_file_name}_enriched.csv.
5. Map the address fields to the name of the fields in the csv that you wish to process. If you have one combined address field, map it to full_address_field. Otherwise, leave full_address_field blank and map column names to street, city, state, and zip. Street must be included, while the others are optional.

Example, for a csv with the following fields:
`addr_st, addr_city, addr_zip`

```
input_file: 'example.csv'

full_address_field:

address_fields:
  street_address: addr_st
  city: addr_city
  state:
  zip: addr_zip

```
If you have both full_address_field and the address fields filled in, the script will ask you which to use.

6. List which fields other than latitude and longitude you want to add.
  (Latitude and longitude will always be added.) If you enter an invalid field, the program will error out and ask you to try again.
  A complete list of valid fields can be found further down in this README. 

```
enrichment_fields:
  - census_tract_2020
  - census_block_group_2020
  - census_block_2020
```
7. List which SRIDs should be returned. SRID refers to the format of the coordinate system. There are two options: 4326 and 2272. 4326 is the WGS84 standard, and will be output as `geocode_lat` and `geocode_lon` and 2272 Southern Pennsylvania Projection and is output as `geocode_x` and `geocode_y`. 

```
# Which SRIDs to return for geocoding
srid_4326: true
srid_2272: true
```

The full config file should look something like this:
```
# Connection Credentials
AIS_API_KEY: YOUR_API_KEY

# File Config
input_file: ./data/example_input_4.csv
address_file: ./data/addresses.parquet

resume: False

full_address_field: address

# OR, IF ADDRESS IS SPLIT INTO MULTIPLE COLUMNS:
address_fields:
  street_address:
  city:
  state:
  zip:

# Enrichment Fields -- Aside from coordinates, what fields to add
enrichment_fields:
  - census_tract_2020
  - census_block_group_2020
  - census_block_2020
```

8. You're now ready to run the geocoder.

Double-click `geocoder.exe` -- the same file that you used to instal geocoder.

(If you get an error about a missing package, this means something didn't install properly. Try removing the `address-geocoder-main` folder and try again.)

The dialogue will ask you to specify a config file. Hit enter without typing anything to
keep the default config file ('./config.yml')

The output file will be saved in the same location as your input file, with _enriched attached to the filename.

Note that you may see various warnings about a USPS and election file not being found, and about SSL certification. This is to be expected.

One of the steps of the enrichment process is to check against Philadelphia's address information system (AIS). Please note that this process can take some time. It takes around 3-4 minutes to make 1,000 calls to AIS. Not all records will be checked against AIS -- just those that have no match in the `addresses.parquet` file.
So, it is important to provide an input file with as clean as an address field as possible, to minimize the number of times the script checks AIS.

### 2.3 Running a Python Command

Note that this method is not currently recommended, as it requires more setup, and does not automatically check for address file updates, and requires the user to manually convert a downloaded address file to a parquet. If you choose to run the program this way, you should periodically repeat steps 3-5 below to keep the address file up to date.

Package management is handled using uv, instead of pip. You can read more about uv here: https://docs.astral.sh/uv/

#### Installing uv:
Follow the instructions for your machine for installing uv here, if you have not installed it already: https://docs.astral.sh/uv/getting-started/installation/

#### Installing dependencies:
1. Navigate to the package directory if not already there: `cd address-batch-geocoder`
2. run `uv sync` to install the proper Python version and all package dependencies. This will create a virtual environment at `.venv`:
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
6. You will need to configure a config.yml file. You can use config_example.yml as a template. See section #2.2 for instructions on how to configure this file.
Once the file is configured, run:
```
uv run geocoder.py --config_path=[PATH_TO_YOUR_CONFIG_FILE]
```
running ```uv run geocoder.py``` without the ```--config_path``` argument will default to ```config.yml```

## 3. Enrichment Fields

| `Field` |
| --- |
|`address_high`|
|`address_low_frac`|
|`address_low_suffix`|
|`address_low`|
|`bin`|
|`census_block_2010`|
|`census_block_2020`|
|`census_block_group_2010`|
|`census_block_group_2020`|
|`census_tract_2010`|
|`census_tract_2020`|
|`center_city_district`|
|`clean_philly_block_captain`|
|`commercial_corridor`|
|`council_district_2016`|
|`council_district_2024`|
|`cua_zone`|
|`dor_parcel_id`|
|`eclipse_location_id`|
|`elementary_school`|
|`engine_local`|
|`h3_hex_grid_r7`|
|`h3_hex_grid_r8`|
|`h3_hex_grid_r9`|
|`h3_hex_grid_r10`|
|`high_school`|
|`highway_district`|
|`highway_section`|
|`highway_subsection`|
|`historic_district`|
|`historic_site`|
|`historic_street`|
|`ladder_local`|
|`lane_closure`|
|`leaf_collection_area`|
|`li_address_key`|
|`li_district`|
|`major_phila_watershed`|
|`middle_school`|
|`neighborhood_advisory_committee`|
|`opa_account_num`|
|`opa_address`|
|`opa_owners`|
|`philly_rising_area`|
|`planning_district`|
|`police_district`|
|`police_division`|
|`police_service_area`|
|`political_division`|
|`political_ward`|
|`ppr_friends`|
|`pwd_center_city_district`|
|`pwd_maint_district`|
|`pwd_parcel_id`|
|`pwd_pressure_district`|
|`pwd_treatment_plant`|
|`pwd_water_plate`|
|`recycling_diversion_rate`|
|`rubbish_recycle_day`|
|`sanitation_area`|
|`sanitation_convenience_center`|
|`sanitation_district`|
|`secondary_rubbish_day`|
|`seg_id`|
|`state_house_rep_2012`|
|`state_house_rep_2022`|
|`state_senate_2012`|
|`state_senate_2022`|
|`street_code`|
|`street_light_route`|
|`street_name`|
|`street_postdir`|
|`street_predir`|
|`street_suffix`|
|`traffic_district`|
|`traffic_pm_district`|
|`tobacco_free_school_zones`|
|`tobacco_retailer_permit_capped`|
|`unit_num`|
|`unit_type`|
|`us_congressional_2012`|
|`us_congressional_2018`|
|`us_congressional_2022`|
|`zip_4`|
|`zip_code`|
|`zoning_document_ids`|
|`zoning_rco`|
|`zoning`|

# 4. Development

The following section contains information relevant to developing this project.

#### 4.1 Testing

This package uses the pytest module to conduct unit tests. Tests are located in the `tests/` folder.

In order to run all tests, for example:

```
python3 pytest tests/
```

To run tests from one file:

```
python3 pytest tests/test_parser.py
```

To run one test within a file:

```
python3 pytest tests/test_parser.py::test_parse_address
```

##### Running the end to end test

 To run the full suite of end to end tests, you will need to export an environment variable with the AIS API KEY. Otherwise, some tests will be skipped.

In mac/linux:

```
export AIS_API_KEY="<AIS_API_KEY>"
```

To do this permanently, you'll need to edit the shell configuration file.

In powershell:

```
$env:AIS_API_KEY = "<AIS_API_KEY>"
```

To do so permanently in powershell:

```
[System.Environment]::SetEnvironmentVariable("VARIABLE_NAME", "value", [System.EnvironmentVariableTarget]::User)
```

#### 4.2 Updating the Powershell Script

If you make changes to the powershell script, you may want to test how it works with the executable file. You will need to install and run Invoke-PS2EXE. Please use the following command to convert the powershell script to an exe. Do not omit the `-noConsole:$false` option, as we need that for the program to run in the console properly.

`Invoke-ps2exe -inputFile $scriptFile -outputFile $outputExe -noConsole:$false`

Do not commit the executable to the repo. Releasing the executable is handled via a github action, as described in section 4.3.

#### 4.3 Publishing a Release

This code is intended to be called using an executable file generated from `powershell/geocoder_for_exe.ps1`. If changes are made to this file, we need to make a new release.

To create a release:

1. Update the exe_version variable in the ps1 file: `$exeVersion = "v1.1.0`
2. If necessary the min_exe_version.txt in the `powershell` folder. This is the minimum version a user should be allowed to run without being forced to redownload the exe.
3. Make sure the commit is tagged with a version number like `v1.0.0`:

```
# When you're ready to create a release:
git tag v1.0.0
git push origin v1.0.0
```

Publishing a new release will trigger the `build-and-publish` workflow, which calls the following command to create an executable file from the powershell script: `Invoke-ps2exe -inputFile $scriptFile -outputFile $outputExe -noConsole:$false`

#### 4.4 Publishing a Breaking Change

If your updates will cause previous installs of the geocoder to fail to work, (For example, a major change to how the powershell script interfaces with the backend), you will need to create a release as mentioned above and update `powershell/min_exe_version.txt` to match the version in the change. This will prevent the user from being able to run the executable if their version is older than the breaking change.

To publish a breaking change:
1. Update `powershell/min_exe_version.txt` to the latest semver number. The format is `v2.0.0`. In the future, we plan to remove the 'v' from the string.
2. Update `powershell/geocoder_for_exe.ps1` so that `$exeVersion` variable matches the latest semver number
3. Please flag in any PR that you have created a breaking change, and have a second person review.
4. Once merged, create a release and push to main with the latest semver number

The check works as follows:
1. The exe should pull the most recent code on `main`, which will pull the latest copy of `min_exe_version.txt`.
2. The exe will check the executable version in the file against `powershell/min_exe_version.txt`. **You must update the $exeVersion variable in the powershell file for this to work**. This check works by stripping the "v" from the semver string, and then using powershell's `[System.Version]` to compare versions.
3. If the executable version is too low, the script will give the user an error and exit.