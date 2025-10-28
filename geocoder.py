import yaml, polars as pl
from utils.parse_address import generate_field_list, parse_address
from passyunk.parser import PassyunkParser

def process_csv(config_path) -> pl.LazyFrame:
    """
    Given a config file with the csv filepath, normalizes records
    in that file using Passyunk.

    Args:
        config_path (str): The path to the config file
        chunksize (int): Batch size for file reading
    
    Returns: A polars lazy dataframe
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    filepath = config.get('input_file')

    if not filepath:
        raise ValueError("An input filepath must be specified in the config " \
        "file.")
    
    # Determine which fields in the file are the address fields
    address_fields = generate_field_list(config_path)

    p = PassyunkParser()

    lf = pl.scan_csv(
        filepath
    )

    # Concatenate address fields, strip extra spaces
    lf = lf.with_columns(
        pl.concat_str(
            [pl.col(field).fill_null("") for field in address_fields], 
            separator=" ")\
                .str.replace_all(r"\s+", " ").alias("joined_address")
    )
    # Create struct of columns to be filled by parse address function
    new_cols = pl.Struct([
        pl.Field("parsed_address", pl.String),
        pl.Field("is_addr", pl.Boolean),
        pl.Field("is_philly_addr", pl.Boolean)
    ])

    lf = lf.with_columns(
        pl.col("joined_address").map_elements(
            lambda s: parse_address(p, s), return_dtype=new_cols
        ).alias("temp_struct")
    ).unnest("temp_struct")
    
    return lf

    


    