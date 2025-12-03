import yaml, polars as pl, requests, click, os
from pathlib import Path
from datetime import datetime
from functools import partial
from utils.encoder import detect_file_encoding, recode_to_utf8
from utils.parse_address import (
    find_address_fields,
    parse_address,
    infer_city_state_field,
    is_non_philly_from_full_address,
    is_non_philly_from_split_address,
)
from utils.ais_lookup import throttle_ais_lookup
from utils.tomtom_lookup import throttle_tomtom_lookup
from utils.zips import ZIPS
from mapping.ais_properties_fields import POSSIBLE_FIELDS
from passyunk.parser import PassyunkParser
from pathlib import PurePath


def get_current_time():
    current_datetime = datetime.now()
    return current_datetime.strftime("%H:%M:%S")


def split_non_philly_address(config_path, lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Given a polars LazyFrame, splits into two lazy frames:
    One for addresses located in Philadelphia, one for addresses
    not located in Philadelphia.

    Returns:
        (philly_lf, non_philly_lf)
    """

    fields = infer_city_state_field(config_path)

    # If we are using full address field, we need to look up
    # against us-address.
    full_address_field = fields.get("full_address")

    if full_address_field:
        non_philly_fn = partial(is_non_philly_from_full_address, philly_zips=ZIPS)

        flagged = lf.with_columns(
            pl.col(full_address_field)
            .map_elements(non_philly_fn, return_dtype=pl.Boolean)
            .alias("is_non_philly")
        )

    # Otherwise, get address columns from config
    else:
        city_col = fields.get("city")
        state_col = fields.get("state")
        zip_col = fields.get("zip")

        # Make an address struct based on which fields exist
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

        # Partial helper function for searching for non philly records
        # used for mapping with polars
        non_philly_fn = partial(is_non_philly_from_split_address, zips=ZIPS)

        flagged = lf.with_columns(
            address_struct.map_elements(non_philly_fn, return_dtype=pl.Boolean).alias(
                "is_non_philly"
            )
        )

    non_philly_lf = flagged.filter(pl.col("is_non_philly"))
    philly_lf = flagged.filter(~pl.col("is_non_philly"))

    return philly_lf, non_philly_lf


def parse_with_passyunk_parser(parser, lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Given a polars LazyFrame, parses addresses in that LazyFrame
    using passyunk parser, and adds output address.

    Args:
        parser: A passyunk parser instance
        lf: The polars lazyframe with an address field to parse

    Returns:
        A polars lazyframe with output address, and address validity booleans
        added.
    """

    # Create struct of columns to be filled by parse address function
    new_cols = pl.Struct(
        [
            pl.Field("output_address", pl.String),
            pl.Field("is_addr", pl.Boolean),
            pl.Field("is_philly_addr", pl.Boolean),
            pl.Field("is_multiple_match", pl.Boolean),
            pl.Field("match_type", pl.String),
        ]
    )

    lf = lf.with_columns(
        pl.col("joined_address")
        .map_elements(lambda s: parse_address(parser, s), return_dtype=new_cols)
        .alias("passyunk_struct")
    ).unnest("passyunk_struct")

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
        item for item in ais_enrichment_fields if item not in POSSIBLE_FIELDS.keys()
    ]

    if invalid_fields:
        to_print = ", ".join(field for field in invalid_fields)
        raise ValueError(
            "The following fields are not available:"
            f"{to_print}. Please correct these and try again."
        )

    address_file_fields = []

    [
        address_file_fields.append(POSSIBLE_FIELDS[item])
        for item in ais_enrichment_fields
    ]

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

    Args:
        geo_filepath: The filepath to the geography file. This is the main
        file used to geocode addresses.
        input_data: A lazyframe containing the input data to be enriched
        address_fields: A list of one or more address fields
    """
    addresses = pl.scan_parquet(geo_filepath)
    addresses = addresses.select(address_fields)

    rename_mapping = {
        value: key for key, value in POSSIBLE_FIELDS.items() if value in address_fields
    }

    joined_lf = input_data.join(
        addresses, how="left", left_on="output_address", right_on="street_address"
    ).rename(rename_mapping)

    # If geocode is not null, then there is a match.
    # mark the match type as address file
    joined_lf = joined_lf.with_columns(
        pl.when(pl.col("geocode_lat").is_not_null())
        .then(pl.lit("address_file"))
        .otherwise("match_type")
        .alias("match_type")
    )

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
    config: dict,
    to_add: pl.LazyFrame,
    full_address_field: bool,
    enrichment_fields: list,
) -> pl.LazyFrame:
    """
    Enrich a lazyframe with user-specified columns from AIS.

    Args:
        config (dict): A dictionary of config information. Used
        to make API calls.
        to_add (polars LazyFrame): A lazyframe of data to enrich
        full_address_field (bool): Whether or not the user has specified
        that the input data has a full address field
        enrichment_fields: A list of fields to add to the lazyframe.
    """
    # Create a new struct of columns to add
    new_cols = pl.Struct(
        [
            pl.Field("output_address", pl.String),
            pl.Field("is_addr", pl.Boolean),
            pl.Field("is_philly_addr", pl.Boolean),
            pl.Field("geocode_lat", pl.String),
            pl.Field("geocode_lon", pl.String),
            pl.Field("is_multiple_match", pl.Boolean),
            pl.Field("match_type", pl.String),
            *[pl.Field(field, pl.String) for field in enrichment_fields],
        ]
    )

    API_KEY = config.get("AIS_API_KEY")
    field_names = [f.name for f in new_cols.fields]

    with requests.Session() as sess:
        addr_cfg = config.get("address_fields") or {}
        zip_field = addr_cfg.get("zip")

        # Don't include zip field if full address field is specified
        if zip_field and not full_address_field:
            struct_expr = pl.struct(["output_address", zip_field]).map_elements(
                lambda s: throttle_ais_lookup(
                    sess,
                    API_KEY,
                    s["output_address"],
                    s[zip_field],
                    enrichment_fields,
                ),
                return_dtype=new_cols,
            )
        else:
            struct_expr = pl.col("output_address").map_elements(
                lambda address: throttle_ais_lookup(
                    sess,
                    API_KEY,
                    address,
                    None,
                    enrichment_fields,
                ),
                return_dtype=new_cols,
            )

        tmp_name = "ais_struct"

        added = (
            to_add.with_columns(struct_expr.alias(tmp_name))
            .with_columns(
                *[pl.col(tmp_name).struct.field(n).alias(n) for n in field_names]
            )
            .drop(tmp_name)
        )

    return added


def enrich_with_tomtom(parser, to_add: pl.LazyFrame) -> pl.LazyFrame:
    """
    Enrich a lazy frame with latitude and longitude from TomTom.

    Args:
        parser: A passyunk parser object. Used to standardize TomTom output.
        to_add: A polars lazyframe to be enriched

    Returns:
        An enriched polars layzframe.
    """

    new_cols = pl.Struct(
        [
            pl.Field("output_address", pl.String),
            pl.Field("geocode_lat", pl.String),
            pl.Field("geocode_lon", pl.String),
            pl.Field("match_type", pl.String),
            pl.Field("is_addr", pl.Boolean),
            pl.Field("is_philly_addr", pl.Boolean),
        ]
    )

    field_names = [f.name for f in new_cols.fields]

    with requests.Session() as sess:
        added = (
            # Use the joined address for tomtom, as passyunk parser strips
            # state, city information
            to_add.with_columns(
                pl.struct(["joined_address", "output_address"])
                .map_elements(
                    lambda cols: throttle_tomtom_lookup(
                        sess,
                        parser,
                        ZIPS,
                        cols["joined_address"],
                        cols["output_address"],
                    ),
                    return_dtype=new_cols,
                )
                .alias("tomtom_struct")
            )
            .with_columns(
                *[pl.col("tomtom_struct").struct.field(n).alias(n) for n in field_names]
            )
            .drop("tomtom_struct")
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

    # Determine which fields in the file are the address fields
    address_fields = find_address_fields(config_path)

    # Detect input file encoding

    encoding = detect_file_encoding(filepath)

    # If encoding is not UTF-8, recode it
    utf8_filepath = ""
    if encoding != "UTF-8":

        print(f"Converting file encoding from {encoding} to UTF-8")
        utf8_filepath = Path(filepath).with_suffix(Path(filepath).suffix + ".utf8")
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
    address_fields_list = [field for field in address_fields.values() if field]
    diff = [field for field in address_fields_list if field not in file_cols]

    if diff:
        raise ValueError(
            "The following fields specified in the config"
            f"file are not present in the input file: {diff}"
        )

    # ---------------- Join Addresses to Address File -------------------#

    current_time = get_current_time()
    print(f"Joining addresses to address file at {current_time}.")
    # Concatenate address fields, strip extra spaces
    lf = lf.with_columns(
        pl.concat_str(
            [pl.col(field).fill_null("") for field in address_fields_list],
            separator=" ",
        )
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .alias("joined_address")
    )

    parser = PassyunkParser()
    lf = parse_with_passyunk_parser(parser, lf)

    # ---------------- Split out Non Philly Addresses -------------------#
    current_time = get_current_time()
    print(f"Identifying non-Philadelphia addresses at {current_time}.")
    philly_lf, non_philly_lf = split_non_philly_address(config_path, lf)

    # Generate the names of columns to add for both the AIS API
    # and the address file
    ais_enrichment_fields, address_file_enrichment_fields = build_enrichment_fields(
        config
    )

    joined_lf = add_address_file_fields(
        geo_filepath, philly_lf, address_file_enrichment_fields
    )

    # Split out fields that did not match the address file
    # and attempt to match them with the AIS API

    # -------------------------- Add Fields from AIS ------------------ #
    current_time = get_current_time()
    print(f"Adding fields from AIS at {get_current_time()}")

    has_geo, needs_geo = split_geos(joined_lf)

    uses_full_address = bool(address_fields.get("full_address"))
    ais_enriched = enrich_with_ais(
        config, needs_geo, uses_full_address, ais_enrichment_fields
    )

    ais_enriched.sink_csv('data/ais_enriched.csv')

    ais_rejoined = pl.concat([has_geo, ais_enriched]).sort("__geocode_idx__")

    # -------------- Check Match Failures Against TomTom ------------------ #

    has_geo, needs_geo = split_geos(ais_rejoined)

    current_time = get_current_time()
    print(f"Adding fields from TomTom at {get_current_time()}")

    # Rejoin the addresses marked as non-philly for tomtom search
    # at the beginning of the process
    needs_geo = pl.concat([non_philly_lf, needs_geo], how="diagonal").sort(
        "__geocode_idx__"
    )

    tomtom_enriched = enrich_with_tomtom(parser, needs_geo)

    rejoined = (
        pl.concat([has_geo, tomtom_enriched])
        .sort("__geocode_idx__")
        .drop(["__geocode_idx__", "joined_address", "is_non_philly"])
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

    if utf8_filepath:
        os.remove(utf8_filepath)


if __name__ == "__main__":
    process_csv()
