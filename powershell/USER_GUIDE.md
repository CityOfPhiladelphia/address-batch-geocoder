# address-geocoder
A tool to standardize and geocode Philadelphia addresses

Address Geocoder takes an input file containing addresses 
and adds latitude and longitude to those addresses, as well as any optional
fields that the user supplies.

## Note:
For more information about the geocoder, consult the GitHub repository: https://github.com/CityOfPhiladelphia/address-geocoder. The README in this repo contains more details about the matching process, and information about how to run the geocoder from the command line, if desired.

## Questions?
If you have questions about the geocoder that this FAQ cannot answer, feel free to contact citygeo at: maps@phila.gov

## 1. Prerequisites
You will need the following things:
1. An executable file called `geocoder.exe`. This is used to run the program. Do not save the executable in a folder that has spaces in the name.	
2. An AIS API key, provided to you by CityGeo.


## Installation
First, you will need to download and install the geocoder.

The geocoder file can be downloaded from GitHub. The latest release can be found at: https://github.com/CityOfPhiladelphia/address-geocoder/releases/

Read through the notes carefully, and then download the zip file at the bottom of the readme.

Extract the zip folder into a folder where you can easily find it. When opening the zipped file, you may be prompted to either `extract` or `run`. Hit `extract`, not `run`, as the script will need to exist in an uncompressed directory in order to create the subfolders needed to work.

The folder **must not** have spaces in its name. The zip folder contains two files: `geocoder.exe` and `release.txt`. If you delete or rename these files, you will need to download them again or rename them back. Deleting `release.txt` will stop the program from being able to inform you if there is a new version of the `.exe` file that you need to download.

Double-clicking `geocoder.exe` will launch the program. As a first-time installation, the script will download Python and Git if not present, then download the geocoder from GitHub and install the proper dependencies. The geocoder will be downloaded to a folder called address-geocoder-main. If there are problems with your install, you may try deleting this folder and running `geocoder.exe` again.

Note that this script will attempt to install Python 3.10 on your machine if you do not have Python 3.10 installed on your machine.

The script will then attempt to download the address file. This may take a few minutes. It will save the address file and a version file in a subfolder called geocoder_address_data. Under most circumstances, you should not remove this folder or any of the files in it. Doing so will cause the script to redownload the address file.

After the installation runs successfully, you are ready to set up the configuration file.

## 2. How to Use Address Geocoder
In order to run `Address Geocoder`, first set up the configuration file. By default,
`Address Geocoder` searchers for a file named `config.yml`. Detailed steps for filling out the config file are in the next section.

### Configuration
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
3. Add the filepath for the input file (the file that you wish to enrich), and the geography file (the address file you have been given.) This should look something like this. If using relative filepaths, filepaths are relative to the address-geocoder-main folder downloaded from GitHub. For ease of use, exact filepaths are recommended. Do not put the filenames in quotes:
```
input_file: ./data/example_input_4.csv
geography_file: ./data/addresses.parquet
```
4. Map the address fields to the name of the fields in the csv that you wish to process. 
If you have one combined address field, map it to full_address_field. 
Otherwise, leave full_address_field blank and map column names to street, city, state, and zip. Street must be included, while the others are optional.

Example, for a csv with the following fields:
`addr_st, addr_city, addr_zip`

```
input_file: 'example.csv'

full_address_field:

address_fields:
  street: addr_st
  city: addr_city
  state:
  zip: addr_zip

If you have both full_address_field and the address fields filled in, the script will ask you which to use.

```
5. List which fields other than latitude and longitude you want to add.
(Latitude and longitude will always be added.) If you enter an invalid field, the program will error out and ask you to try again.
A complete list of valid fields can be found further down in this README. 

```
enrichment_fields:
  - census_tract_2020
  - census_block_group_2020
  - census_block_2020
```

The full config file should look something like this:
```
# Connection Credentials
AIS_API_KEY: YOUR_API_KEY

# File Config
input_file: ./data/example_input_4.csv
geography_file: ./data/addresses.parquet

full_address_field: address

# OR, IF ADDRESS IS SPLIT INTO MULTIPLE COLUMNS:
address_fields:
  street:
  city:
  state:
  zip:

# Enrichment Fields -- Aside from coordinates, what fields to add
enrichment_fields:
  - census_tract_2020
  - census_block_group_2020
  - census_block_2020
```

6. You're now ready to run the geocoder.

Double-click `geocoder.exe` -- the same file that you used to instal geocoder.

(If you get an error about a missing package, this means something didn't install properly. Try removing the `address-geocoder-main` folder and try again.)

The dialogue will ask you to specify a config file. Hit enter without typing anything to
keep the default config file ('./config.yml')

The output file will be saved in the same location as your input file, with _enriched attached to the filename.

Note that you may see various warnings about a USPS and election file not being found, and about SSL certification. This is to be expected.

One of the steps of the enrichment process is to check against Philadelphia's address information system (AIS). Please note that this process can take some time. It takes around 3-4 minutes to make 1,000 calls to AIS. Not all records will be checked against AIS -- just those that have no match in the `addresses.parquet` file.
So, it is important to provide an input file with as clean as an address field as possible, to minimize the number of times the script checks AIS.

## How The Geocoder Works
`Address-Geocoder` processes a csv file with addresses, and geolocates those
addresses using the following steps:

1. Takes an input file of addresses, and standardizes those 
addresses using `passyunk`, Philadelphia's address standardization system.
2. Compares the standardized data to a local parquet file, `addresses.parquet`,
and adds the user-specified fields as well as latitude and longitude from that file
3. Not all records will match to the address file. For those records that do not match,
`Address-Geocoder` queries the Address Information System (AIS) API and adds returned fields.
Please note that this process can take some time, so processing large files with a messy address field
is not recommended. As an example, if you have a file that needs 1,000 rows to be sent to AIS, this will take
approximately 3-4 minutes.
5. The enriched file is then saved to the same directory as the input file.

## Enrichment Fields
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