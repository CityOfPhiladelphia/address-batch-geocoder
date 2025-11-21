import requests, time
from retrying import retry
from .rate_limiter import RateLimiter
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Suppress the InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

AIS_RATE_LIMITER = RateLimiter(max_calls=5, period=1.0)


def tiebreak(response: dict, zip) -> dict:
    """
    If more than one result is returned by AIS, tiebreak by checking zip code.
    If no zip code is provided, return None and a flag that indicates a
    duplicate match.

    Returns:
        A dict with the zipcode-matched record, or if no match, None.
    """

    candidates = []
    for candidate in response.json()["features"]:
        
        if candidate["properties"].get("zip_code", "") == zip:
            candidates.append(candidate)

    if len(candidates) == 1:
        return candidates[0]

    # If multiple candidates have zip code,
    # or no candidates have zip code,
    # we cannot tie break. Return None.
    return None


# Code adapted from Alex Waldman and Roland MacDavid
# https://github.com/CityOfPhiladelphia/databridge-etl-tools/blob/master/databridge_etl_tools/ais_geocoder/ais_request.py
# @retry(
#     wait_exponential_multiplier=1000,
#     wait_exponential_max=10000,
#     stop_max_attempt_number=5,
# )
def ais_lookup(
    sess: requests.Session,
    api_key: str,
    address: str,
    zip: str = None,
    enrichment_fields: list = [],
) -> dict:
    """
    Given a passyunk-normalized address, looks up whether or not it is in the
    database.

    Args:
        sess (requests Session object): A requests library session object
        api_key (str): An AIS api key
        address (str): The address to query
        enrichment_fields (list): The fields to add from AIS

    Returns:
        A dict with standardized address, latitude and longitude,
        and user-requested fields.
    """
    ais_url = "https://api.phila.gov/ais/v1/search/" + address
    params = {}
    params["gatekeeperKey"] = api_key

    response = sess.get(ais_url, params=params, timeout=10, verify=False)

    if response.status_code >= 500:
        raise Exception("5xx response")
    elif response.status_code == 429:
        raise Exception("429 response")

    out_data = {}
    if response.status_code == 200:

        if len(response.json()["features"]) > 1:
            r_json = tiebreak(response, zip)

            # If tiebreak fails, return
            # null values for most fields.
            if not r_json:
                out_data["output_address"] = None
                out_data["is_addr"] = False
                out_data["is_philly_addr"] = True
                out_data["geocode_lat"] = None
                out_data["geocode_lon"] = None
                out_data["is_multiple_match"] = True
                out_data["match_type"] = "ais"

                for field in enrichment_fields:
                    out_data[field] = None

                return out_data

        else:
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
        out_data["is_multiple_match"] = False
        out_data["match_type"] = "ais"

        for field in enrichment_fields:
            field_value = r_json.get("properties", "").get(field, "")

            # Explicitly checking for existence of field value handles
            # cases where some fields (such as opa-owners) may be an
            # empty list
            if not field_value:
                out_data[field] = None

            else:
                out_data[field] = str(field_value)

        return out_data

    # If no match, return none
    out_data["output_address"] = address
    out_data["is_addr"] = False
    out_data["is_philly_addr"] = False
    out_data["geocode_lat"] = None
    out_data["geocode_lon"] = None
    out_data["is_multiple_match"] = False
    out_data["match_type"] = None

    for field in enrichment_fields:
        out_data[field] = None

    return out_data


def throttle_ais_lookup(
    sess: requests.Session,
    api_key: str,
    address: str,
    zip: str = None,
    enrichment_fields: list = [],
) -> dict:
    """
    Helper function to throttle the number of API requests to 10 per second.
    """
    AIS_RATE_LIMITER.wait()
    return ais_lookup(sess, api_key, address, zip, enrichment_fields)
