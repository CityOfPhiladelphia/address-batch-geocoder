---
description: Instructions on how to use the yaml file config for the Address Batch Geocoder
icon: lucide/file-text
---

# Yaml File Config

After checking for updates, `geocoder.exe` will prompt the user with two run options:

```
Choose an option:
[1] Run with the user-interface
[2] Run with the .yml config
[Any other key]: exit:
```

Press `2` to use the .yml config method. Before using this method, ensure that you have set up your configuration file. By default,
`Address Geocoder` searches for a file named `config.yml`. Detailed steps for filling out the config file are in the next section.

## Configuration
### 1. Create the config file
The script should make a config.yml file if no config.yml file exists. If the script did not do this, you can simply copy `config_example.yml` to `config.yml` either in the file explorer by running in the terminal:
```
cp config_example.yml config.yml
```
!!! warning
    Do not delete, rename, or move `config_example.yml`. If you delete this file, you will need to redownload it from GitHub. 
    
    In most cases, it is not recommended to delete, rename, or move `config.yml`. If you rename this file, the geocoder will be unable to find it and will create a new config.yml.

### 2. Add the API Key
Open up config.yml, and add your AIS API Key here:
```
AIS_API_KEY:
```

### 3. Add the input file
Add the filepath for the input file (the file that you wish to enrich), and the address file. 

The address file should have been automatically downloaded by `geocoder.exe`, and the correct path should be in the config file by default. 

This should look something like this. For ease of use, exact filepaths are recommended. Do not put the filenames in quotes:
```
input_file: ./data/example_input_4.csv
address_file: ./geocoder_address_data/address_service_area_summary.parquet
```
### 4. (Optional) Resuming a partially geocoded file
The geocoder writes data incrementally. If your previous geocoding session was interrupted before it finished, you have the option to resume that file. In that case, you will set:
```
resume: True
```
You will still need to provide the name of the non-geocoded input file as the input file. The partially geocoded file must exist in the same directory as the
input file, with the format {input_file_name}_enriched.csv.

### 5. Map the address fields
Map the address fields to the name of the fields in the csv that you wish to process. 

If you have one combined address field, map it to full_address_field. Otherwise, leave full_address_field blank and map column names to street, city, state, and zip. 

Street must be included, while the others are optional.

Example, for a csv with the following fields: `addr_st, addr_city, addr_zip`
```
input_file: 'example.csv'

full_address_field:

address_fields:
  street_address: addr_st
  city: addr_city
  state:
  zip: addr_zip
```
!!! note 
    If you have both full_address_field and the address fields filled in, the script will ask you which to use.

### 6. Select enrichment fields
List which fields other than latitude and longitude you want to add.(Latitude and longitude will always be added.) 

If you enter an invalid field, the program will error out and ask you to try again.
  
A complete list of valid fields can be found [here.](./enrichment_fields) 
```
enrichment_fields:
  - census_tract_2020
  - census_block_group_2020
  - census_block_2020
```
### 7. Choose which coordinate system to use
 List which SRIDs should be returned. SRID refers to the format of the coordinate system. There are two options: 4326 and 2272. 4326 is the WGS84 standard, and will be output as `geocode_lat` and `geocode_lon` and 2272 Southern Pennsylvania Projection and is output as `geocode_x` and `geocode_y`. 

```
# Which SRIDs to return for geocoding
srid_4326: true
srid_2272: true
```

### 8. Review the config file before submitting
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

### 9. Run the geocoder
Double-click `geocoder.exe` &mdash; the same file that you used to install the geocoder.

The dialogue will ask you to specify a config file. Hit enter without typing anything to keep the default config file ('./config.yml') 

The output file will be saved in the same location as your input file, with _enriched attached to the filename.

Note that you may see various warnings about a USPS and election file not being found, and about SSL certification. This is expected behavior, and can be safely ignored.

One of the steps of the enrichment process is to check against Philadelphia's address information system (AIS). Please note that this process can take some time. It takes around 3-4 minutes to make 1,000 calls to AIS. Not all records will be checked against AIS &mdash; just those that have no match in the `addresses.parquet` file.

So, it is important to provide an input file with as clean as an address field as possible, to minimize the number of times the script checks AIS.