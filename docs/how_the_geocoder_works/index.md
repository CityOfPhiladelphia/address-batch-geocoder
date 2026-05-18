# How The Geocoder Works

The geocoder processes a csv file with addresses, and geolocates those addresses using the following steps:

### :lucide-a-large-small: 1. Standardization
The Geocoder takes an input file of addresses, and standardizes those addresses using `passyunk`, Philadelphia's address standardization system.

### :lucide-sheet: 2. Address File Comparison
The Geocoder compares standardized addresses to a local parquet file, `addresses.parquet`, and adds the user-specified fields as well as latitude and longitude from that file.

!!! note

    The release executable of the address geocoder automatically checks an s3 bucket for an updated version of the address file.

### :lucide-map-pinned: 3. Backup #1: Address Information System (AIS) Match

Not all records will match to the address file. For those records that do not match, `Address-Batch-Geocoder` queries the Address Information System (AIS) API and adds returned fields. Please note that this process can take some time, so processing large files with a messy address field is not recommended. As an example, if you have a file that needs 1,000 rows to be sent to AIS, this will take approximately 3-4 minutes.

### :lucide-map-pinned: 4. Backup #2: TomTom Match
Records that don't match to the AIS API are then queried against TomTom, which has different address parsing capabilities, and is also able to return information about addresses outside of Philadelphia.

### :lucide-database: 5. Caching
To reduce redundant API calls, the geocoder caches AIS and TomTom results for duplicate addresses within a single run.

### :lucide-rotate-ccw: 6. Rematching
Records that successfully match to TomTom are then rerun against AIS to try to recover enrichment fields, if those addresses are in Philadelphia

### :lucide-save: 7. Output
The enriched file is then saved to the same directory as the input file.