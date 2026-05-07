import yaml
import polars as pl
import requests
import click
import os
import tempfile
import csv
import warnings
from collections.abc import Generator
from datetime import datetime
from functools import partial
from utils.cache import LRUCache
from utils.encoder import detect_file_encoding, recode_to_utf8
from utils.parse_address import (
    find_address_fields,
    parse_address,
    infer_city_state_field,
    is_non_philly
)
from utils.ais_lookup import ais_lookup
from utils.tomtom_lookup import tomtom_lookup
from utils.zips import ZIPS
from mapping.ais_properties_fields import POSSIBLE_FIELDS
from passyunk.parser import PassyunkParser
from pathlib import PurePath, Path


def get_current_time():
    current_datetime = datetime.now()
    return current_datetime.strftime("%H:%M:%S")

def parse_with_passyunk_parser(
    parser, address_col: str, lf: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Given a polars LazyFrame, parses addresses in that LazyFrame
    using passyunk parser, and adds output address.

    Args:
        parser: A passyunk parser instance
        address_col: The address column to parse
        lf: The polars lazyframe with an address field to parse

    Returns:
        A polars lazyframe with output address, and address validity booleans
        added.
    """

    # Create struct of columns to be filled by parse address function
    
    # Use is_non_philly (not is_philly_addr) for routing — is_non_philly is set once
    # from the input address fields and is stable. is_philly_addr is an output field that is
    # an affirmative flag -- it is updated as matches are found, but during the intermediate
    # matching process, just because is_philly_addr is false doesn't mean it's actually not in philly
    new_cols = pl.Struct(
        [
            pl.Field("output_address", pl.String),
            pl.Field("is_addr", pl.Boolean),
            pl.Field("is_philly_addr", pl.Boolean),
            pl.Field("is_multiple_match", pl.Boolean),
            pl.Field("geocoder_used", pl.String),
        ]
    )

    lf = lf.with_columns(
        pl.col(address_col)
        .map_elements(lambda s: parse_address(parser, s), return_dtype=new_cols)
        .alias("passyunk_struct")
    ).unnest("passyunk_struct")

    return lf


def build_enrichment_fields(ais_enrichment_fields: list, srid_4326: bool, srid_2272: bool) -> tuple[list, list]:
    """
    Given a config dictionary, returns two lists of fields to be
    added to the input file. One list is the address file fieldnames,
    the other is the AIS fieldnames.

    Args:
        ais_enrichment_fields (list): A list of fields to append
        srid_4326 (bool): Whether or not to append SRID 4326
        srid_2272 (bool): Whether or not to append SRID 2272 

    Returns: A tuple with AIS fieldnames and address file fieldnames.
    """
    address_file_fields = []

    # Only append enrichment fields if set to avoid NoneType Error
    if ais_enrichment_fields:
        invalid_fields = [
            item for item in ais_enrichment_fields if item not in POSSIBLE_FIELDS.keys()
        ]

        if invalid_fields:
            to_print = ", ".join(field for field in invalid_fields)
            raise ValueError(
                "The following fields are not available:"
                f"{to_print}. Please correct these and try again."
            )

        [
            address_file_fields.append(POSSIBLE_FIELDS[item])
            for item in ais_enrichment_fields
        ]

    # Need street_address for joining
    address_file_fields.append("street_address")

    if srid_4326:
        address_file_fields.extend(["geocode_lat", "geocode_lon"])
    if srid_2272:
        address_file_fields.extend(["geocode_x", "geocode_y"])

    # Avoid issues if user specifies a field more than once
    # Return empty   if no enrichment fields set
    return (ais_enrichment_fields if ais_enrichment_fields else [], address_file_fields)

def add_address_file_fields(
    geo_filepath: str, input_data: pl.LazyFrame, address_fields: list, srid_4326: bool, srid_2272: bool
) -> tuple[pl.LazyFrame, dict]:
    """
    Given a list of address fields to add, adds those fields from
    the address file to each record in the input data. Does so via a
    left join on the full address.

    Args:
        geo_filepath: The filepath to the address_file. This is the main
        file used to geocode addresses.
        input_data: A lazyframe containing the input data to be enriched
        address_fields: A list of one or more address fields
        srid_4326: Whether or not to append SRID 4326
        srid_2272: Whether or not to append SRID 2272
    
        Returns:
            The appended data and a dict of renamed fields if there were fieldname conflicts
    """
    addresses = pl.scan_parquet(geo_filepath)
    addresses = addresses.select(address_fields)

    # Check which enrichment fields would conflict with existing columns
    existing_cols = input_data.collect_schema().names()
    
    conflicts = [
        key for key, value in POSSIBLE_FIELDS.items()
        if value in address_fields and value in existing_cols
    ]

    # Rename conflicting input columns to _left
    if conflicts:
        rename_input = {POSSIBLE_FIELDS[field]: POSSIBLE_FIELDS[field] + "_left" for field in conflicts}
        input_data = input_data.rename(rename_input)
    
    else:
        rename_input = {}

    rename_mapping = {
        value: key for key, value in POSSIBLE_FIELDS.items() if value in address_fields
    }

    joined_lf = input_data.join(
        addresses, how="left", left_on="output_address", right_on="street_address"
    ).rename(rename_mapping)

    # Mark match type as address_file if we got coordinates from the file
    # Check whichever SRID is enabled    
    if srid_4326:
        match_condition = pl.col("geocode_lat").is_not_null()
    elif srid_2272:
        match_condition = pl.col("geocode_x").is_not_null()
    else:
        # This shouldn't happen due to earlier validation, but just in case
        raise ValueError("At least one SRID must be enabled")
    
    joined_lf = joined_lf.with_columns(
        pl.when(match_condition)
        .then(pl.lit("address_file"))
        .otherwise("geocoder_used")
        .alias("geocoder_used")
    )

    return joined_lf, rename_input

class Geocoder:
    """
    Handles the full geocoding pipeline for a batch of addresses. 
    Does so without loading full address file into memory.

    Pipeline:
        1. Parse and join addresses to a local address file (Polars lazy, sink to disk)
        2. Iterate through sunk output, process unmatched records via AIS/TomTom APIs
        3. Write enriched records incrementally to output file
    """
    
    INTERNAL_COLS = {"__geocode_idx__", "joined_address", "is_non_philly", "is_undefined", "raw_address"}

    def __init__(self, config: dict):
        self.config: dict = config
        self.cache = LRUCache(max_size=20_000)

        # Perform checks to ensure that config is set up properly
        self.api_key = config.get("AIS_API_KEY")
        self.srid_4326 = bool(config.get("srid_4326"))
        self.srid_2272 = bool(config.get("srid_2272"))
        self.input_filepath = config.get("input_file")
        ## Due to a name change, config file may have older format 'geography_file' name or newer format: 'address_file' field
        self.geo_filepath = config.get("geography_file") or config.get("address_file")

        # Raise errors if config file malformed
        if not self.api_key:
            raise ValueError(
                "AIS API Key must be specified."
            )
        
        if not self.input_filepath:
            raise ValueError("An input filepath must be specified in the config file.")
        
        if not self.geo_filepath:
            raise ValueError("A filepath for the address_file must be specified in the config.")
        
        self.parser: PassyunkParser = PassyunkParser()
        self.session: requests.Session = requests.Session()

        # Programmatically generate other attributes
        self.address_fields: dict = find_address_fields(config)
        self.address_is_split: bool = False if self.address_fields.get("full_address") else True

        in_path = PurePath(self.input_filepath)
        stem = in_path.name.replace("".join(in_path.suffixes), "") 

        self.out_path = str(in_path.parent / f"{stem}_enriched.csv")

        if self.config.get("resume"):
            prev_config = self._infer_previous_config()
            self.__dict__.update(prev_config)  


        if not self.srid_4326 and not self.srid_2272:
            raise ValueError(
                "Invalid configuration: At least one SRID must be enabled. "
                "Set srid_4326 or srid_2272 to true in your config file."
            )
        
        # Determine which fields to append to the output
        # If resuming, get them from the partially geocoded file  
        # If we're resuming, don't run so we don't override
        # the enrichment field values
        if not self.config.get("resume"):           
            self.ais_enrichment_fields, \
            self.address_file_enrichment_fields \
                = build_enrichment_fields(
                    config.get("enrichment_fields", []),
                    srid_4326=self.srid_4326,
                    srid_2272=self.srid_2272
                )
        
        # Programmatically calculate lines in file
        with open(self.input_filepath, "r") as f:
            self.row_count = sum(1 for _ in f) - 1 # subtract header
        
        # Programmatically set batch size based on length of input file
        # To avoid writing too frequently for large files
        self.batch_size = max(1000, self.row_count // 100)
    
    # ------------ Functions Needed for Resume Functionality -------------- #
    def _count_output_rows(self) -> int:
        """Determines how many rows are in the output file. Needed to know
        when to resume geocoding."""
        if not os.path.exists(self.out_path):
            return 0
        with open(self.out_path, 'r', encoding='utf-8-sig') as f:
            return sum(1 for _ in f) - 1 # minus header
    
    def _infer_previous_config(self) -> None:
        """Determines the previous config for a partially geocoded file. Needed in order to determine
        what geode format and enrichment fields should be used. Overwrites existing geocode format and address field specifications."""

        prev_config = {}
        if not os.path.exists(self.out_path):
            raise FileNotFoundError(f"""No partially coded file found. 
                                    In order to resume geocoding {self.input_filepath}, 
                                    the following file must exist: {self.out_path}""")

        with open(self.out_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames
        
        # Determine geocode fields:
        if 'geocode_lat' in header and 'geocode_lon' in header:
            prev_config['srid_4326'] = True
        else:
            prev_config['srid_4326'] = False
        
        if 'geocode_x' in header and 'geocode_y' in header:
            prev_config['srid_2272'] = True
        else:
            prev_config['srid_2272'] = False
        
        if not (prev_config.get("srid_4326") or prev_config.get("srid_2272")):
            raise ValueError("Partially coded file is missing geocoded fields, and cannot be resumed.")

        # Identify enrichment fields
        prev_config["ais_enrichment_fields"], \
        prev_config["address_file_enrichment_fields"] = \
        build_enrichment_fields(
            [key for key in header if key in POSSIBLE_FIELDS.keys()],
            prev_config['srid_4326'],
            prev_config['srid_2272']
        )

        return prev_config

    def _read_from_tmp(self, tmp_path: str) -> Generator[dict, None, None]:
        """Reads the sunk temp file after address file join 
        and produces a generator of the records in that file.
        Coerces boolean fields back from strings."""
        
        bool_fields = {"is_undefined", "is_non_philly", 
                       "is_addr", "is_philly_addr"}
        
        with open(tmp_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:

                yield {k: (v.lower() == "true") if k in bool_fields 
                       else v for k, v in row.items()}
    
    def _write_batch(self, batch: list[dict], out_path: str) -> None:
        """Appends a batch of records to the output file."""

        if not batch:
            return
        
        # If file doesn't exist, we need to write the header.
        write_header = not os.path.exists(out_path)

        with open(out_path, 'a', newline='', encoding='utf-8-sig') as f:
            fieldnames = [key for key in batch[0].keys() if key != '__geocode_idx__']
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')

            if write_header:
                writer.writeheader()
            writer.writerows(batch)
            f.flush()


    def _is_matched(self, record: dict) -> bool:
        """Determines whether or not a record is geocoded, based on which
        srids the user has specified."""
        if self.srid_4326:
            return bool(record.get("geocode_lat") and record.get("geocode_lon"))
        elif self.srid_2272:
            return bool(record.get("geocode_x") and record.get("geocode_y")) 
              
        return False
      
    def _split_non_philly(self, lf: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """
        Given a polars LazyFrame, splits into two lazy frames:
        One for addresses located in Philadelphia, one for addresses
        not located in Philadelphia.

        Returns:
            (philly_lf, non_philly_lf)
        """

        fields = infer_city_state_field(self.config)
        full_address_field = fields.get("full_address")

        location_struct = pl.Struct(
        [pl.Field("is_non_philly", pl.Boolean), 
         pl.Field("is_undefined", pl.Boolean)]
        )

        non_philly_fn = partial(is_non_philly, 
                                address_is_split=self.address_is_split, 
                                zips=ZIPS)

        # If we have the full address, just use the full address field
        if full_address_field:
            flagged = lf.with_columns(
                pl.col(full_address_field)
                .map_elements(non_philly_fn, return_dtype=location_struct)
                .alias("location_info")
            ).unnest("location_info")

        # Otherwise, we need to get the address column names
        # from the config
        else:
            city_col = fields.get("city")
            state_col = fields.get("state")
            zip_col = fields.get("zip")

            # Then, we have to make an address struct based on which fields actually
            # are specified in the config, since city/state/zip are optional
            address_struct = pl.struct(
                [
                    (pl.col(city_col) if city_col else pl.lit(None, dtype=pl.Utf8)).alias(
                        "city"
                    ),
                    (pl.col(state_col) if state_col else pl.lit(None, dtype=pl.Utf8)).alias(
                        "state"
                    ),
                    (pl.col(zip_col) if zip_col else pl.lit(None, dtype=pl.Utf8)).alias(
                        "zip"
                    ),
                ]
            )

            flagged = lf.with_columns(
                address_struct.map_elements(
                    non_philly_fn, return_dtype=location_struct)
                .alias("location_info")
            ).unnest("location_info")

        return flagged.filter(~pl.col("is_non_philly")), \
            flagged.filter(pl.col("is_non_philly"))

    def _join_to_address_file(self, filepath: str | PurePath, sink_path: str) -> None:
        """Parses, normalizes, and joins input CSV to local address file.
        Sinks result to disk without loading full dataset into memory."""
        filepath = str(filepath)
        
        # Detect input file encoding and recode if necessary.
        # utf8_filepath is returned to the caller so they can clean it up
        # after the lazy frame has been materialized.
        encoding = detect_file_encoding(filepath)
        utf8_filepath = ""
        if encoding.lower() != "utf-8":
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False, encoding="utf-8"
            ) as temp_file:
                utf8_filepath = temp_file.name

            recode_to_utf8(filepath, utf8_filepath, encoding)
            filepath = utf8_filepath
        
        # infer schema = False infers everything as a string. Otherwise, polars
        # will attempt to infer zip codes like 19114-3409 as an int
        lf = pl.scan_csv(
            filepath,
            row_index_name="__geocode_idx__",
            infer_schema=False,
            encoding="utf8-lossy",
        )

        # Check if there are invalid address fields specified
        file_cols = lf.collect_schema().names()
        address_fields_list = [field for field in self.address_fields.values() if field]
        diff = [field for field in address_fields_list if field not in file_cols]

        if diff:
            raise ValueError(
                "The following fields specified in the config "
                f"file are not present in the input file: {diff}"
            )
        
        # ---------------- Passyunk Parse and Format -------------------#
        passyunk_address_field = self.address_fields.get(
            "full_address"
        ) or self.address_fields.get("street_address")

        # Create raw address field, used later to attempt to match
        # raw address against TomTom if the passyunk parsed address
        # fails to match
        lf = lf.with_columns(pl.col(passyunk_address_field).alias("raw_address"))

        lf = parse_with_passyunk_parser(self.parser, passyunk_address_field, lf)

        # After parsing with Passyunk, rebuild joined_address using the cleaned output_address
        # Only do this for split address fields (street/city/state/zip)
        # Don't do this for full_address fields, as Passyunk strips city/state
        # Joined address is used as the API lookup string
        if self.address_is_split:
            # Build list of available location components
            location_components = []
            for key in ["city", "state", "zip"]:
                if key in self.address_fields.keys() \
                    and self.address_fields[key] is not None:
                    location_components.append(
                        pl.col(self.address_fields[key]).fill_null("")
                )
            
            lf = lf.with_columns(
            pl.when(pl.col("output_address").is_not_null())
            .then(
                pl.concat_str(
                    [pl.col("output_address")] + location_components,
                    separator=" ",
                )
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
            )
            .otherwise(pl.col(passyunk_address_field))
            .alias("joined_address"),

            pl.concat_str(
                [pl.col("raw_address")] + location_components,
                separator=" ",
            )
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .alias("raw_address"),  # overwrite raw_address in place
            )
        
        else:
            # For full_address cases, use the original field as joined_address
            lf = lf.with_columns(pl.col(passyunk_address_field).alias("joined_address"))
        
        # ---------------- Address File Join -------------------#
        # Split non-philly addresses before joining to address file.
        # This prevents non-Philly addresses from incorrectly matching
        # Philly records — e.g. "1234 Market St, Pittsburgh" would
        # otherwise match "1234 MARKET ST" in the Philly address file.
        
        philly_lf, non_philly_lf = self._split_non_philly(lf)

        philly_joined_lf, input_renames = add_address_file_fields(
            self.geo_filepath, philly_lf, 
            self.address_file_enrichment_fields, 
            self.srid_4326,
            self.srid_2272
            )

        # If we renamed any columns on philly_joined_lf, we need to do
        # the same on non_philly_lf before combining
        if input_renames:
            non_philly_lf = non_philly_lf.rename(input_renames)
        
        # Concat non-philly back in before sinking so output preserves row order
        rejoined_lf = pl.concat([philly_joined_lf, non_philly_lf], how="diagonal").sort("__geocode_idx__")


        # Reorder fields so that all geocode fields are adjacent
        final_cols = rejoined_lf.collect_schema().names()

        # Remove all geocode columns from the list
        geo_cols = []
        if self.srid_4326:
            geo_cols.extend(["geocode_lat", "geocode_lon"])
        
        if self.srid_2272:
            geo_cols.extend(["geocode_x", "geocode_y"])

        cols_without_geo = [c for c in final_cols if c not in geo_cols]
        
        if "geocoder_used" in cols_without_geo:
            insert_idx = cols_without_geo.index("geocoder_used") + 1
        else:
            insert_idx = 0
        
        # Insert all geocode columns together after geocoder_used
        ordered_cols = (
            cols_without_geo[:insert_idx] + 
            geo_cols + 
            cols_without_geo[insert_idx:]
        )
        
        rejoined_lf = rejoined_lf.select(ordered_cols)

        try:
            rejoined_lf.sink_csv(sink_path, include_bom=True)
        finally:
            if utf8_filepath:
                os.remove(utf8_filepath)

    def _run_ais_lookup(self, record: dict) -> dict:
        api_address = (
            record["output_address"] + ", Philadelphia, PA"
            if record.get("is_undefined")
            else record["output_address"]
            )

        zip_field = self.address_fields.get("zip")
        full_address_field = self.address_fields.get("full_address")

        ais_lookup_args = {
            "sess": self.session,
            "api_key": self.api_key,
            "address": api_address,
            "zip": None,
            "enrichment_fields": self.ais_enrichment_fields,
            "existing_is_addr": record["is_addr"],
            "existing_is_philly_addr": record["is_philly_addr"],
            "original_address": record["output_address"],
            "fetch_4326": self.srid_4326,
            "fetch_2272": self.srid_2272
        }

        # Include zip field if full address is not specified
        if zip_field and not full_address_field:
            ais_lookup_args["zip"] = record[zip_field]

        ais_result = ais_lookup(**ais_lookup_args)

        return ais_result
        
    def _run_tomtom_lookup(self, record: dict) -> dict:

        api_address = (
            record["raw_address"] + ", Philadelphia, PA"
            if record.get("is_undefined")
            else record["raw_address"]
            )
        
        tomtom_lookup_args = {
            "sess": self.session,
            "parser": self.parser,
            "philly_zips": ZIPS,
            "address": api_address,
            "fallback_addr": record["output_address"],
            "fetch_4326": self.srid_4326,
            "fetch_2272": self.srid_2272
        }
        
        tomtom_result = tomtom_lookup(**tomtom_lookup_args)

        return tomtom_result

    def _process_unmatched_address(self, record: dict) -> dict:
        """
        AIS -> TomTom -> Conditional AIS re-match for a single unmatched record.
        """
        # First check and see if the record is in the cache
        addr = record.get("raw_address")
        if addr:
            cached = self.cache[addr]

            if cached:
                return cached

        # ------------ First AIS Match --------------- #

        # If address is in Philly, look up with AIS
        # We don't want to waste AIS calls on non-philly addresses
        # Assume is in Philadelphia if address is undefined

        # Use is_non_philly (not is_philly_addr) for routing — is_non_philly is set once
        # from the input address fields and is stable. is_philly_addr is an output field that is
        # an affirmative flag -- it is updated as matches are found, but during the intermediate
        # matching process, just because is_philly_addr is false doesn't mean it's actually not in philly

        # If it's likely the record is in philly
        if not record.get('is_non_philly'):
            record.update(self._run_ais_lookup(record))
        
        if self._is_matched(record):
            if addr:
                self.cache[addr] = record
            return record
        
        # ------------ TomTom Match --------------- #
        # We need to use the raw input address to match against
        # TomTom, not the passyunk parsed address
        # Assume in Philadelphia if address is undefined

        tomtom_result = self._run_tomtom_lookup(record)
        record.update(tomtom_result)

        # If it didn't match with TomTom, we've failed to match the record.
        # return it.
        if not self._is_matched(record):
            if addr:
                self.cache[addr] = record
            return record
        
        # ------------ AIS Rematch Attempt --------------- #
        # This time, we do use is_philly_addr, since TomTom may have
        # affirmatively marked an address in Philly
        if record.get("is_philly_addr"):
            ais_rematch = self._run_ais_lookup(record)

            if self._is_matched(ais_rematch):
                # AIS recovered the record - mark as tomtom-ais and use AIS result
                record.update(ais_rematch)
                record["geocoder_used"] = "tomtom-ais"
            
            # else: AIS failed, keep TomTom result as-is
        

        # Now that we've processed the record, add it to the cache
        if addr:
            self.cache[addr] = record

        return record

    def _to_output_record(self, record: dict) -> dict:
        return {k: v for k, v in record.items() if k not in self.INTERNAL_COLS}

    def geocode(self) -> None:
        """Full pipeline: Join, save, iterate, write."""

        sink_path = Path(self.out_path).with_suffix(".tmp")
        if sink_path.exists():
            sink_path.unlink()

        processed_rows = 0
        batch = []
        records = None

        # If not resuming, we need to clear the file
        if not self.config.get("resume"):
            if os.path.exists(self.out_path):
                os.remove(self.out_path)
        
        # Get where we need to start writing
        # On resume, skip rows already written to output
        # Iterating and discarding is cheap since no API calls are made
        resume_offset = self._count_output_rows() \
            if self.config.get("resume") else 0

        try:
            # Step 1: Join to the address file, then sink the results
            # as a tmp file
            self._join_to_address_file(self.input_filepath, sink_path)
            
            # Step 2: Process records that didn't match to the address file
            # one by one

            records = self._read_from_tmp(sink_path)

            for idx, record in enumerate(records):
                if idx < resume_offset:
                    continue

                if self._is_matched(record):
                    batch.append(self._to_output_record(record))
                
                else:
                    batch.append(
                        self._to_output_record(
                            self._process_unmatched_address(record)))
                
                processed_rows += 1

                if processed_rows % self.batch_size == 0:
                    self._write_batch(batch, self.out_path)
                    
                    # Clear batch after it's written
                    batch = []

        finally:
            # Write any remaining rows
            if records is not None:
                records.close() # Closes the records generator

            self._write_batch(batch, self.out_path)

            if os.path.exists(sink_path):
                os.remove(sink_path)

# Make a separate thin wrapper to pass config to
# process_data, so we can run this script as both
# the streamlit backend and with a yml config file
@click.command()
@click.option(
    "--config_path",
    default="./config.yml",
    prompt=True,
    show_default="./config.yml",
    help="The path to the config file.",
)
def run_process_csv(config_path):
    current_time = get_current_time()
    print(f"Beginning enrichment process at {current_time}.")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    geocoder = Geocoder(config)
    geocoder.geocode()

    # filepath lives here since the CLI is responsible for file I/O
    
    current_time = get_current_time()
    print(f"Enrichment complete at {current_time}. Output saved to {geocoder.out_path}.")

if __name__ == "__main__":
    run_process_csv()