# address-geocoder
A tool to standardize and geocode Philadelphia addresses

## Setup
You will need the following things:
1. An addresses file, provided to you by CityGeo. 
2. An AIS API key, provided to you by CityGeo.
3. Python installed on your computer, at least version 3.9

Next, create a virtual environment:
```
python3 -m venv venv
```

Then, activate the virtual environment. This will need to be activated
every time you want to run the append tool, not just this once:
```
source venv/bin/activate
```

Finally, install the packages in requirements.text:
```
pip install -r requirements.txt
```

Once you have installed everything, it is time to fill in the config file.

## How to Use Address Geocoder
Address Geocoder takes an input file containing addresses 
and appends latitude and longitude to those addresses, as well as any optional
fields that the user supplies.

In order to run `Address Geocoder`, first set up the configuration file. By default,
`Address Geocoder` searchers for a file named `config.yml`. This is the recommended config filename. You can copy the template in
`config_example.yml` to a file named `config.yml` and continue from there. Detailed steps are below:

### Configuration
1. Copy `config_example.yml` to `config.yml` by running in the terminal:
```
cp config_example.yml config.yml
```
2. Add your AIS API Key here:

```
AIS_API_KEY:
```
3. Add the filepath for the input file (the file that you wish to append to), and the geography file (the address file you have been given.) This should look something like this:
```
input_file: ./data/example_input_4.csv
geography_file: ./data/addresses.parquet
```
4. Map the address fields to the name of the fields in the csv that you wish
to process. If you have one combined address field, map it to full_address_field.
Otherwise, leave full_address_field blank and map column names to street, city, state, and zip. Street must be included,
while the others are optional.

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
```
5. List which fields other than latitude and longitude you want to append.
(Latitude and longitude will always be appended.) A complete list of fields 
can be found further down in this README. 

```
append_fields:
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

# Append Fields -- Aside from coordinates, what fields to append
append_fields:
  - census_tract_2020
  - census_block_group_2020
  - census_block_2020
```
## Testing
This package uses the pytest module to conduct unit tests. Tests are
located in the `tests/` folder.

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

## How This Works
`Address-Geocoder` processes a csv file with addresses, and geolocates those
addresses using the following steps:

1. Takes an input file of addresses, and standardizes those 
addresses using `passyunk`, Philadelphia's address standardization system.
2. Compares the standardized data to a local parquet file, `addresses.parquet`,
and appends the user-specified fields as well as latitude and longitude from that file
3. Not all records will match to the address file. For those records that do not match,
`Address-Geocoder` queries the Address Information System (AIS) API and appends returned fields.
4. The appended file is then saved to the same directory as the input file.

## Append Fields
| `Field` |
| --- |
|`address_low`|
|`address_low_suffix`|
|`address_low_frac`|
|`address_high`|
|`street_predir`|
|`street_name`|
|`street_suffix`|
|`street_postdir`|
|`unit_type`|
|`unit_num`|
|`street_code`|
|`seg_id`|
|`zip_code`|
|`zip_4`|
|`pwd_parcel_id`|
|`dor_parcel_id`|
|`li_address_key`|
|`eclipse_location_id`|
|`bin`|
|`zoning_document_ids`|
|`pwd_account_nums`|
|`opa_account_num`|
|`opa_owners`|
|`opa_address`|
|`center_city_district`|
|`cua_zone`|
|`li_district`|
|`philly_rising_area`|
|`census_tract_2010`|
|`census_block_group_2010`|
|`census_block_2010`|
|`census_tract_2020`|
|`census_block_group_2020`|
|`census_block_2020`|
|`council_district_2016`|
|`council_district_2024`|
|`political_ward`|
|`political_division`|
|`state_house_rep_2012`|
|`state_house_rep_2022`|
|`state_senate_2012`|
|`state_senate_2022`|
|`us_congressional_2012`|
|`us_congressional_2018`|
|`us_congressional_2022`|
|`planning_district`|
|`elementary_school`|
|`middle_school`|
|`high_school`|
|`zoning`|
|`zoning_rco`|
|`commercial_corridor`|
|`historic_district`|
|`historic_site`|
|`police_division`|
|`police_district`|
|`police_service_area`|
|`rubbish_recycle_day`|
|`recycling_diversion_rate`|
|`leaf_collection_area`|
|`sanitation_area`|
|`sanitation_district`|
|`sanitation_convenience_center`|
|`clean_philly_block_captain`|
|`historic_street`|
|`highway_district`|
|`highway_section`|
|`highway_subsection`|
|`traffic_district`|
|`traffic_pm_district`|
|`street_light_route`|
|`lane_closure`|
|`pwd_maint_district`|
|`pwd_pressure_district`|
|`pwd_treatment_plant`|
|`pwd_water_plate`|
|`pwd_center_city_district`|
|`major_phila_watershed`|
|`neighborhood_advisory_committee`|
|`ppr_friends`|
|`engine_local`|
|`ladder_local`|