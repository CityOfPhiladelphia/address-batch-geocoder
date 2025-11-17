import requests
from utils.ais_lookup import RateLimiter
from retrying import retry

limiter = RateLimiter(10)


# Code adapted from Alex Waldman and Roland MacDavid
# https://github.com/CityOfPhiladelphia/databridge-etl-tools/blob/master/databridge_etl_tools/ais_geocoder/ais_request.py
@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    stop_max_attempt_number=5,
)
def tomtom_lookup(sess: requests.Session, address: str) -> dict:
    """
    Given a passyunk-normalized address, looks up via TomTom.

    Args:
        sess (requests Session object): A requests library session object
        address (str): The address to query

    Returns:
        A dict with standardized address, latitude and longitude, returned
        from TomTom.
    """
    tomtom_url = "https://citygeo-geocoder-aws.phila.city/arcgis/rest/services/TomTom/US_StreetAddress/GeocodeServer/findAddressCandidates"

    # Need to specify json format, HTML by default
    params = {
        'Address': address,
        'f': 'pjson'
    }

    response = sess.get(tomtom_url, params=params, timeout=10)

    if response.status_code >= 500:
        raise Exception("5xx response")
    elif response.status_code == 429:
        raise Exception("429 response")

    out_data = {}
    if response.status_code == 200:
        # TomTom returns an empty list if no addresses match
        if response.json()['candidates']:
            # First response should be most probable match
            r_json = response.json()["candidates"][0]
            address = r_json.get("address", "")

            try:
                lon = r_json["location"]["x"]
                lat = r_json["location"]["y"]

            except KeyError:
                lon, lat = ""

            out_data["output_address"] = address
            out_data["geocode_lat"] = str(lat)
            out_data["geocode_lon"] = str(lon)

            return out_data

    out_data["output_address"] = ""
    out_data["geocode_lat"] = None
    out_data["geocode_lon"] = None

    return out_data

def throttle_tomtom_lookup(
    sess: requests.Session, address: str
) -> dict:
    """
    Helper function to throttle the number of API requests to 10 per second.
    """
    limiter.wait()
    return tomtom_lookup(sess, address)

