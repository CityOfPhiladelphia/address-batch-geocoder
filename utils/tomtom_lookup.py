import requests
from .rate_limiter import RateLimiter
from retrying import retry
from .parse_address import tag_full_address, flag_non_philly_address

TOMTOM_RATE_LIMITER = RateLimiter(max_calls=10, period=1.0)

def _fetch_tomtom_coordinates(
    sess: requests.Session,
    address: str,
    srid: int
) -> tuple[str, str]:
    """
    Helper function to fetch coordinates for a specific SRID.
    Returns (coord1, coord2) or (None, None) if failed.
    """
    TOMTOM_RATE_LIMITER.wait()
    tomtom_url = "https://citygeo-geocoder-aws.phila.city/arcgis/rest/services/TomTom/US_StreetAddress/GeocodeServer/findAddressCandidates"
    params = {"Address": address, "f": "pjson", "outSR": str(srid)}
    
    response = sess.get(tomtom_url, params=params, timeout=10)
    
    if response.status_code >= 500:
        raise Exception("5xx response. There may be a problem with TomTom API server.")
    elif response.status_code == 429:
        raise Exception("429 response. Too many API calls to TomTom.")
    
    if response.status_code == 200 and response.json().get("candidates"):
        r_json = response.json()["candidates"][0]
        try:
            coord1 = r_json["location"]["x"]
            coord2 = r_json["location"]["y"]
            return str(coord1), str(coord2)
        except KeyError:
            return None, None
    
    return None, None

def _do_tomtom_lookup(
    sess: requests.Session,
    parser,
    philly_zips: list,
    address: str,
    fetch_4326: bool,
    fetch_2272: bool,
    match_type: str = "tomtom",
) -> dict:
    """
    Makes a single TomTom request and returns a populated out_data dict,
    or None if no match.
    """

    if not address:
        return None 
    
    TOMTOM_RATE_LIMITER.wait()
    tomtom_url = "https://citygeo-geocoder-aws.phila.city/arcgis/rest/services/TomTom/US_StreetAddress/GeocodeServer/findAddressCandidates"
    params = {"Address": address, "f": "pjson", "outSR": "4326"}

    response = sess.get(tomtom_url, params=params, timeout=10)

    if response.status_code >= 500:
        raise Exception("5xx response. There may be a problem with TomTom API server.")
    elif response.status_code == 429:
        raise Exception("429 response. Too many API calls to TomTom.")

    if response.status_code == 200 and response.json().get("candidates"):
        r_json = response.json()["candidates"][0]
        matched_address = r_json.get("address", "")
        address_tagged = tag_full_address(matched_address)
        address_flagged = flag_non_philly_address(address_tagged, philly_zips)
        is_philly_addr = not address_flagged["is_non_philly"]

        parsed_address = parser.parse(matched_address).get("components", "").get("output_address", "")

        out_data = {}
        out_data["output_address"] = parsed_address if parsed_address else matched_address
        out_data["match_type"] = match_type
        out_data["is_addr"] = True
        out_data["is_philly_addr"] = is_philly_addr

        if fetch_4326:
            try:
                lon = r_json["location"]["x"]
                lat = r_json["location"]["y"]
                out_data["geocode_lat"] = str(round(float(lat), 8))
                out_data["geocode_lon"] = str(round(float(lon), 8))
            except KeyError:
                out_data["geocode_lat"] = None
                out_data["geocode_lon"] = None

        if fetch_2272:
            geo_x, geo_y = _fetch_tomtom_coordinates(sess, matched_address, 2272)
            out_data["geocode_x"] = str(round(float(geo_x), 8))
            out_data["geocode_y"] = str(round(float(geo_y), 8))

        return out_data

    return None

# Code adapted from Alex Waldman and Roland MacDavid
# https://github.com/CityOfPhiladelphia/databridge-etl-tools/blob/master/databridge_etl_tools/ais_geocoder/ais_request.py
@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    stop_max_attempt_number=5,
)
def tomtom_lookup(
    sess: requests.Session, 
    parser, 
    philly_zips: list, 
    address: str, 
    fallback_addr,
    fetch_4326: bool = True,
    fetch_2272: bool = True,
) -> dict:
    """
    Given a passyunk-normalized address, looks up via TomTom.

    Args:
        sess (requests Session object): A requests library session object
        parser: A passyunk parser object, used to normalize output
        philly_zips (list): A list of philadelphia zips to validate
        tomtom output against
        address (str): The address to query
        fallback_addr (str): The address to return if no match is found
        fetch_4326 (bool): Whether or not to pull coordinates in 4326
        fetch_2272 (bool): Whether or not to pull coordinates in 2272

    Returns:
        A dict with standardized address, latitude and longitude, returned
        from TomTom.
    """
    out_data = _do_tomtom_lookup(sess, parser, philly_zips, address, fetch_4326, fetch_2272)
    
    if out_data is not None:
        return out_data

    # Truly no match
    out_data = {
        "output_address": fallback_addr if fallback_addr else address,
        "match_type": None,
        "is_addr": False,
        "is_philly_addr": False,
    }

    if fetch_4326:
        out_data["geocode_lat"] = None
        out_data["geocode_lon"] = None
    
    if fetch_2272:
        out_data["geocode_x"] = None
        out_data["geocode_y"] = None
    
    return out_data