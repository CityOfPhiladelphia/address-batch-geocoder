import requests, polars as pl
from retrying import retry

# Code adapted from Alex Waldman and Roland MacDavid
# https://github.com/CityOfPhiladelphia/databridge-etl-tools/blob/master/databridge_etl_tools/ais_geocoder/ais_request.py

@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
def ais_lookup(sess: requests.Session, api_key: str, address: str) -> tuple[str, bool, bool]:
    """
    Given a passyunk-normalized address, looks up whether or not it is in the
    database. 
    
    Args:
        sess (requests Session object): A requests library session object
        api_key (str): An AIS api key
        address (str): The address to query
    
    Returns:
        The standardized address
    """

    ais_url = 'https://api.phila.gov/ais/v1/search/' + address
    params = {}
    params['gatekeeperKey'] = api_key

    response = sess.get(ais_url, params=params, timeout=10)

    if response.status_code >= 500:
        raise Exception('5xx response')
    elif response.status_code == 429:
        raise Exception('429 response')
    
    if response.status_code == 200:
        address = response['features'][0]['street_address']
        is_addr = True
        is_philly_addr = True

        return(address, is_addr, is_philly_addr)

    return (None, False, False)

    
def stream_null_geos(data: pl.LazyFrame, batch_rows: int):
    needs_geo = data.filter(
        (pl.col("latitude").is_null() | pl.col("latitude").is_nan()) |
        (pl.col("longitude").is_null() | pl.col("longitude").is_nan())
    )
    start = 0
    while True:
        batch_df = (
            needs_geo
            .slice(offset=start, length=batch_rows)
            .collect(streaming=True)
        )
        if batch_df.is_empty():
            break

        yield batch_df
        start += len(batch_df)


def ais_append(
        sess: requests.Session, api_key: str, data: pl.LazyFrame, 
        batch_rows: int = 50_000) -> pl.LazyFrame:
    
    """
    Takes a dataframe of databridge-appended geodata and attempts to
    look up records that did not append from databridge.

    Args:
        sess: a requests library session object
        api_key: the AIS api key
        data: A polars lazy dataframe with databridge-appended latitude and
        longitude
    
    Returns:
        polars lazy frame with ais-appended records
    """

    # Ensure incoming data is in right format
    lf_schema = data.collect_schema().names()

    if any(col not in lf_schema for col in ('output_address','is_addr','is_philly_addr')):
        raise ValueError("An appended dataframe must be passed.")
    