import yaml, polars as pl, requests, click
from datetime import datetime
from utils.parse_address import find_address_fields, parse_address
from utils.ais_lookup import throttle_ais_lookup
from utils.tomtom_lookup import throttle_tomtom_lookup
from mapping.ais_properties_fields import fields
from passyunk.parser import PassyunkParser
from pathlib import PurePath


def get_current_time():
    current_datetime = datetime.now()
    return current_datetime.strftime("%H:%M:%S")


def parse_with_passyunk_parser(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Given a polars LazyFrame, parses addresses in that LazyFrame
    using passyunk parser, and adds output address.

    Args:
        lf: The polars lazyframe with an address field to parse

    Returns:
        A polars lazyframe with output address, and address validity booleans
        added.
    """
    p = PassyunkParser()

    # Create struct of columns to be filled by parse address function
    new_cols = pl.Struct(
        [
            pl.Field("output_address", pl.String),
            pl.Field("is_addr", pl.Boolean),
            pl.Field("is_philly_addr", pl.Boolean),
        ]
    )

    lf = lf.with_columns(
        pl.col("joined_address")
        .map_elements(lambda s: parse_address(p, s), return_dtype=new_cols)
        .alias("temp_struct")
    ).unnest("temp_struct")

    return lf


def build_enrichment_fields(config: dict) -> tuple[list, list]:
    """
    Given a config dictionary, returns two lists of fields to be
    added to the input file. One list is the address file fieldnames,
    the other is the AIS fieldnames.

    Args:
        config (dict): A dictionary read from the config yaml file

    Returns: A tuple with AIS fieldnames and address file fieldnames.
    """
    ais_enrichment_fields = config["enrichment_fields"]

    invalid_fields = [
        item for item in ais_enrichment_fields if item not in fields.keys()
    ]

    if invalid_fields:
        to_print = ", ".join(field for field in invalid_fields)
        raise ValueError(
            "The following fields are not available:"
            f"{to_print}. Please correct these and try again."
        )

    address_file_fields = []

    [address_file_fields.append(fields[item]) for item in ais_enrichment_fields]

    # Need street_address for joining
    address_file_fields.extend(["street_address", "geocode_lat", "geocode_lon"])

    return (ais_enrichment_fields, address_file_fields)


def add_address_file_fields(
    geo_filepath: str, input_data: pl.LazyFrame, address_fields: list
) -> pl.LazyFrame:
    """
    Given a list of address fields to add, adds those fields from
    the address file to each record in the input data. Does so via a
    left join on the full address.
    """
    addresses = pl.scan_parquet(geo_filepath)
    addresses = addresses.select(address_fields)

    rename_mapping = {
        value: key for key, value in fields.items() if value in address_fields
    }

    joined_lf = input_data.join(
        addresses, how="left", left_on="output_address", right_on="street_address"
    ).rename(rename_mapping)

    return joined_lf


def split_geos(data: pl.LazyFrame):
    """
    Splits a lazyframe into two lazy frames: one for records with latitude
    and longitude, and another for records without latitude and longitude.
    Used to determine which records need to be added using AIS.
    """
    has_geo = data.filter(
        (~pl.col("geocode_lat").is_null()) & (~pl.col("geocode_lon").is_null())
    )
    needs_geo = data.filter(
        (pl.col("geocode_lat").is_null()) | (pl.col("geocode_lon").is_null())
    )

    return (has_geo, needs_geo)


def enrich_with_ais(
    config: dict, to_add: pl.LazyFrame, enrichment_fields: list
) -> pl.LazyFrame:
    """
    Adds user-specified fields to a polars lazyframe from AIS.

    Args:
        config: A user config dict
        to_add: A polars lazyframe to be enriched
        enrichment_fields: A list of enrichment fields specified by the user

    Returns:
        An enriched polars lazyframe
    """

    new_cols = pl.Struct(
        [
            pl.Field("output_address", pl.String),
            pl.Field("is_addr", pl.Boolean),
            pl.Field("is_philly_addr", pl.Boolean),
            pl.Field("geocode_lat", pl.String),
            pl.Field("geocode_lon", pl.String),
            *[pl.Field(field, pl.String) for field in enrichment_fields],
        ]
    )

    API_KEY = config.get("AIS_API_KEY")

    field_names = [f.name for f in new_cols.fields]

    with requests.Session() as sess:
        added = (
            to_add.with_columns(
                pl.col("output_address")
                .map_elements(
                    lambda a: throttle_ais_lookup(sess, API_KEY, a, enrichment_fields),
                    return_dtype=new_cols,
                )
                .alias("temp_struct")
            )
            .with_columns(
                *[pl.col("temp_struct").struct.field(n).alias(n) for n in field_names]
            )
            .drop("temp_struct")
        )

    return added

def enrich_with_tomtom(to_add: pl.LazyFrame) -> pl.LazyFrame:
    
    new_cols = pl.Struct([
        pl.Field("output_address", pl.String),
        pl.Field("geocode_lat", pl.String),
        pl.Field("geocode_lon", pl.String)
    ])

    field_names = [f.name for f in new_cols.fields]

    with requests.Session() as sess:
        added = (
            to_add.with_columns(
                pl.col("output_address")
                .map_elements(
                    lambda a: throttle_tomtom_lookup(sess, a),
                    return_dtype=new_cols,
                )
                .alias("temp_struct")
            )
            .with_columns(
                *[pl.col("temp_struct").struct.field(n).alias(n) for n in field_names]
            )
            .drop("temp_struct")
        )

    return added

@click.command()
@click.option(
    "--config_path",
    default="./config.yml",
    prompt=True,
    show_default="./config.yml",
    help="The path to the config file.",
)
def process_csv(config_path) -> pl.LazyFrame:
    """
    Given a config file with the csv filepath, normalizes records
    in that file using Passyunk.

    Args:
        config_path (str): The path to the config file
        chunksize (int): Batch size for file reading

    Returns: A polars lazy dataframe
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    filepath = config.get("input_file")
    geo_filepath = config.get("geography_file")

    if not filepath:
        raise ValueError("An input filepath must be specified in the config " "file.")

    if not geo_filepath:
        raise ValueError(
            "A filepath for the geography file must be" "specified in the config."
        )
    # ---------------- Join Addresses to Address File -------------------#

    current_time = get_current_time()
    print(f"Joining addresses to address file at {current_time}.")

    # Determine which fields in the file are the address fields
    address_fields = find_address_fields(config_path)

    lf = pl.scan_csv(filepath, row_index_name="__geocode_idx__")

    # Concatenate address fields, strip extra spaces
    lf = lf.with_columns(
        pl.concat_str(
            [pl.col(field).fill_null("") for field in address_fields], separator=" "
        )
        .str.replace_all(r"\s+", " ")
        .alias("joined_address")
    )

    lf = parse_with_passyunk_parser(lf)

    # Generate the names of columns to add for both the AIS API
    # and the address file
    ais_enrichment_fields, address_file_enrichment_fields = build_enrichment_fields(
        config
    )

    joined_lf = add_address_file_fields(
        geo_filepath, lf, address_file_enrichment_fields
    )

    # Split out fields that did not match the address file
    # and attempt to match them with the AIS API

    # -------------------------- Add Fields from AIS ------------------ #
    current_time = get_current_time()
    print(f"Adding fields from AIS at {get_current_time()}")

    has_geo, needs_geo = split_geos(joined_lf)

    ais_enriched = enrich_with_ais(config, needs_geo, ais_enrichment_fields)

    ais_rejoined = (
        pl.concat([has_geo, ais_enriched])
        .sort("__geocode_idx__")
    )

    # -------------- Check Match Failures Against TomTom ------------------ #

    has_geo, needs_geo = split_geos(ais_rejoined)
    
    current_time = get_current_time()
    print(f"Adding fields from TomTom at {get_current_time()}")

    tomtom_enriched = enrich_with_tomtom(needs_geo)

    rejoined = (
        pl.concat([has_geo, tomtom_enriched])
        .sort("__geocode_idx__")
        .drop(["__geocode_idx__", "joined_address"])
    )

    # -------------------- Save Output File ---------------------- #

    in_path = PurePath(filepath)

    # If filepath has multiple suffixes, remove them
    stem = in_path.name.replace("".join(in_path.suffixes), "")

    out_path = f"{stem}_enriched.csv"

    out_path = str(in_path.parent / out_path)

    rejoined.sink_csv(out_path)

    current_time = get_current_time()
    print(f"Enrichment complete at {current_time}.")

if __name__ == "__main__":
    process_csv()
