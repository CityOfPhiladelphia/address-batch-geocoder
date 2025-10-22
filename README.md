# address-geocoder
A tool to standardize and geocode Philadelphia addresses

## Configuration
1. Copy `config_example.yml` to `config.yml` by running in the terminal:
```
cp config_example.yml config.yml
```
2. Add your AIS API Key and DB Credentials
3. Map the address fields to the name of the fields in the csv that you wish
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

1. Standardizes addresses using `passyunk`, Philadelphia's address
standardization system.