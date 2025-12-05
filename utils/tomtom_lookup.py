import requests
from .rate_limiter import RateLimiter
from retrying import retry
from .parse_address import tag_full_address, flag_non_philly_address

TOMTOM_RATE_LIMITER = RateLimiter(max_calls=10, period=1.0)


# Code adapted from Alex Waldman and Roland MacDavid
# https://github.com/CityOfPhiladelphia/databridge-etl-tools/blob/master/databridge_etl_tools/ais_geocoder/ais_request.py
@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    stop_max_attempt_number=5,
)
def tomtom_lookup(
    sess: requests.Session, parser, philly_zips: list, address: str, fallback_addr
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

    Returns:
        A dict with standardized address, latitude and longitude, returned
        from TomTom.
    """
    tomtom_url = "https://citygeo-geocoder-aws.phila.city/arcgis/rest/services/TomTom/US_StreetAddress/GeocodeServer/findAddressCandidates"

    # Need to specify json format, HTML by default
    params = {"Address": address, "f": "pjson"}

    response = sess.get(tomtom_url, params=params, timeout=10)

    if response.status_code >= 500:
        raise Exception(
            "5xx response. There may be a problem with Tomtom" "API server."
        )
    # 429 response indicates we're being blocked by the API.
    elif response.status_code == 429:
        raise Exception(
            "429 response. Too many API calls" "to TomTom in a short amount of time."
        )

    out_data = {}
    if response.status_code == 200:
        # TomTom returns an empty list if no addresses match
        if response.json().get("candidates"):
            # First response should be most probable match,
            # so hopefully no need to tiebreak.
            r_json = response.json()["candidates"][0]
            address = r_json.get("address", "")

            try:
                lon = r_json["location"]["x"]
                lat = r_json["location"]["y"]

            except KeyError:
                lon, lat = ""

            # TomTom returns addresses as a full string. We need to
            # determine whether or not the full address is in Philadelphia,
            # so we use the usaddress module.
            address_tagged = tag_full_address(address)
            
            address_flagged = flag_non_philly_address(address_tagged, philly_zips)

            is_philly_addr = not address_flagged['is_non_philly']

            parsed_address = (
                parser.parse(address).get("components", "").get("output_address", "")
            )
            out_data["output_address"] = parsed_address if parsed_address else address
            out_data["geocode_lat"] = str(lat)
            out_data["geocode_lon"] = str(lon)
            out_data["match_type"] = "tomtom"
            out_data["is_addr"] = True
            out_data["is_philly_addr"] = is_philly_addr

            return out_data

    # If no match, return none. Use a passyunk-parsed address if possible,
    # otherwise use the unparsed input address.
    out_data["output_address"] = fallback_addr if fallback_addr else address
    out_data["geocode_lat"] = None
    out_data["geocode_lon"] = None
    out_data["match_type"] = None
    out_data["is_addr"] = False
    out_data["is_philly_addr"] = False

    return out_data


def throttle_tomtom_lookup(
    sess: requests.Session, parser, philly_zips: list, address: str, fallback_addr: str
) -> dict:
    """
    Helper function to throttle the number of API requests to 10 per second.
    """
    TOMTOM_RATE_LIMITER.wait()
    return tomtom_lookup(sess, parser, philly_zips, address, fallback_addr)
