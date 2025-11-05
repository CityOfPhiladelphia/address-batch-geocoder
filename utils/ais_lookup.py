import requests, polars as pl, time
from retrying import retry

class RateLimiter:
    def __init__(self, rps):
        self.rate = 1.0 / rps
        self._last_time = 0.0

    def wait(self):
        now = time.perf_counter()
        remaining = self.rate - (now - self._last_time)

        if remaining > 0:
            time.sleep(remaining)
            now = time.perf_counter()

        self._last_time = now


limiter = RateLimiter(10)


# Code adapted from Alex Waldman and Roland MacDavid
# https://github.com/CityOfPhiladelphia/databridge-etl-tools/blob/master/databridge_etl_tools/ais_geocoder/ais_request.py
@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    stop_max_attempt_number=5,
)
def ais_lookup(
    sess: requests.Session, api_key: str, address: str, append_fields: list
) -> dict:
    """
    Given a passyunk-normalized address, looks up whether or not it is in the
    database.

    Args:
        sess (requests Session object): A requests library session object
        api_key (str): An AIS api key
        address (str): The address to query
        append_fields (list): The fields to append from AIS

    Returns:
        A dict with standardized address, latitude and longitude,
        and user-requested appended fields.
    """
    ais_url = "https://api.phila.gov/ais/v1/search/" + address
    params = {}
    params["gatekeeperKey"] = api_key

    response = sess.get(ais_url, params=params, timeout=10)

    if response.status_code >= 500:
        raise Exception("5xx response")
    elif response.status_code == 429:
        raise Exception("429 response")

    out_data = {}
    if response.status_code == 200:
        r_json = response.json()["features"][0]
        address = r_json.get("properties", "").get("street_address", "")

        try:
            lon, lat = r_json["geometry"]["coordinates"]

        except KeyError:
            lon, lat = ""

        out_data["output_address"] = address
        out_data["is_addr"] = True
        out_data["is_philly_addr"] = True
        out_data["geocode_lat"] = str(lat)
        out_data["geocode_lon"] = str(lon)

        for field in append_fields:
            out_data[field] = r_json.get("properties", "").get(field, "")

        return out_data

    out_data["output_address"] = ""
    out_data["is_addr"] = False
    out_data["is_philly_addr"] = False
    out_data["geocode_lat"] = None
    out_data["geocode_lon"] = None

    for field in append_fields:
        out_data[field] = None

    return out_data


def throttle_ais_lookup(
    sess: requests.Session, api_key: str, address: str, append_fields: list
) -> dict:
    """
    Helper function to throttle the number of API requests to 10 per second.
    """
    limiter.wait()
    return ais_lookup(sess, api_key, address, append_fields)


def split_geos(data: pl.LazyFrame):
    """
    Splits a lazyframe into two lazy frames: one for records with latitude
    and longitude, and another for records without latitude and longitude.
    Used to determine which records need to be appended using AIS.
    """
    has_geo = data.filter(
        (~pl.col("geocode_lat").is_null()) & (~pl.col("geocode_lon").is_null())
    )
    needs_geo = data.filter(
        (pl.col("geocode_lat").is_null()) | (pl.col("geocode_lon").is_null())
    )

    return (has_geo, needs_geo)
