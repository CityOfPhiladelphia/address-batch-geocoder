import requests
from retrying import retry
from .rate_limiter import RateLimiter
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Suppress the InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

AIS_RATE_LIMITER = RateLimiter(max_calls=9, period=1.0)


def tiebreak(response: dict, zip) -> dict:
    """
    If more than one result is returned by AIS, tiebreak by checking zip code.
    If no zip code is provided, return None and a flag that indicates a
    duplicate match.

    Args:
        response (dict): An AIS API response
        zip (str): The zip code present on the input data. Used
        to check API responses against.

    Returns:
        A dict with the zipcode-matched record, or if no match, None.
    """

    candidates = []
    for candidate in response.json()["features"]:
        # If the AIS API zip code matches the zip code on the
        # incoming data, this record is a potential match
        if candidate["properties"].get("zip_code", "") == zip:
            candidates.append(candidate)

    # Sometimes AIS returns two addresses for the same lat lon
    # should write code in the future to more intelligently tiebreak
    # and behaves differently based on if the two addresses returned
    # are actually the same
    if len(candidates) == 1:
        return candidates[0]

    else:
        return None


def get_intersection_coords(ais_dict: dict) -> list[str, str]:
    """
    Given an intersection object type returned from AIS,
    get the coordinates for that intersection. Returns
    a list of coordinate pairs.

    Args:
        response: A JSON response from the AIS API
    """
    coords = []
    for feature in ais_dict.get("features"):
        geom = feature.get("geometry")
        if geom:
            lon, lat = geom["coordinates"]
            coords.append((lon, lat))

    return coords


def make_coordinate_lookups(
    sess: requests.Session,
    coords: list,
    api_key: str = None,
) -> list[dict]:
    """Given a list of coordinate pairs, do a reverse lookup
    against the AIS API. Returns a list of matches for each
    coordinate pair in the list."""

    out_data = []

    for coord in coords:
        lon, lat = coord
        ais_url = f"https://api.phila.gov/ais_doc/v1/reverse_geocode/{lon},{lat}"
        params = {}
        params["gatekeeperKey"] = api_key

        response = sess.get(ais_url, params=params, timeout=10, verify=False)

        if response.status_code >= 500:
            raise Exception("5xx response. There may be a problem with theAIS API.")
        elif response.status_code == 429:
            print(response.text)
            raise Exception("429 response. Too many calls to the AIS API.")

        elif response.status_code == 401:
            raise Exception("401 response. Invalid API key.")

        elif response.status_code == 200:
            out_data.append(response.json())

        else:
            raise ValueError(
                f"Error occurred with the following status code: {response.status_code}"
            )

    return out_data


def tiebreak_coordinate_lookups(responses: list[dict], zip: str):
    addresses = []

    for response in responses:
        candidates = response.get("features")
        # If the AIS API zip code matches the zip code on the
        # incoming data, this record is a potential match
        for candidate in candidates:
            if candidate["properties"].get("zip_code", "") == zip or not zip:
                addresses.append(candidate)

    # Sometimes AIS returns two addresses for the same lat lon
    # should write code in the future to more intelligently tiebreak
    # and behaves differently based on if the two addresses returned
    # are actually the same
    if addresses:
        return addresses[0]


# Code adapted from Alex Waldman and Roland MacDavid
# https://github.com/CityOfPhiladelphia/databridge-etl-tools/blob/master/databridge_etl_tools/ais_geocoder/ais_request.py
@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    stop_max_attempt_number=3,
    wait_fixed=200,
)
def ais_lookup(
    sess: requests.Session,
    api_key: str,
    address: str,
    zip: str = None,
    enrichment_fields: list = [],
    existing_is_addr: bool = False,
    existing_is_philly_addr: bool = False,
    original_address: str = None,
) -> dict:
    """
    Given a passyunk-normalized address, looks up whether or not it is in the
    database.

    Args:
        sess (requests Session object): A requests library session object
        api_key (str): An AIS api key
        address (str): The address to query
        zip (str): The zip code associated with the address, if present
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
        raise Exception("5xx response. There may be a problem with theAIS API.")
    elif response.status_code == 429:
        raise Exception("429 response. Too many calls to the AIS API.")

    out_data = {}
    # If status code is 200, that means API has found a match.
    # API will return a 404 if no match
    if response.status_code == 200:
        # If r_json is longer than 1, multiple matches
        # were returned and we need to tiebreak
        r_json = response.json()
        tiebroken_address = None

        if len(r_json["features"]) > 1 and r_json.get("search_type") == "address":
            tiebroken_address = tiebreak(response, zip)

        elif r_json.get("search_type") == "intersection":
            coord_pairs = get_intersection_coords(response.json())
            coord_lookup_results = make_coordinate_lookups(sess, coord_pairs, api_key)
            tiebroken_address = tiebreak_coordinate_lookups(coord_lookup_results, zip)

        # if r_json is not longer than 1, no need to tiebreak
        elif len(r_json["features"]) == 1:
            tiebroken_address = response.json()["features"][0]

        # If tiebreak fails, return
        # null values for most fields.
        if not tiebroken_address:
            tiebroken_address = response.json()
            normalized_addr = tiebroken_address.get("normalized", "")
            out_data["output_address"] = normalized_addr if normalized_addr else address
            out_data["is_addr"] = False
            out_data["is_philly_addr"] = True
            out_data["geocode_lat"] = None
            out_data["geocode_lon"] = None
            out_data["is_multiple_match"] = True
            out_data["match_type"] = "ais"

            for field in enrichment_fields:
                out_data[field] = None

            return out_data

        # If we successfully got a tiebroken_address, process it
        if tiebroken_address:
            out_address = tiebroken_address.get("properties", "").get(
                "street_address", ""
            )

            try:
                lon, lat = tiebroken_address["geometry"]["coordinates"]

            except KeyError:
                lon, lat = ""

            out_data["output_address"] = out_address if out_address else address
            out_data["is_addr"] = True
            out_data["is_philly_addr"] = True
            out_data["geocode_lat"] = str(lat)
            out_data["geocode_lon"] = str(lon)
            out_data["is_multiple_match"] = False
            out_data["match_type"] = "ais"

            for field in enrichment_fields:
                field_value = tiebroken_address.get("properties", "").get(field, "")

                # Explicitly checking for existence of field value handles
                # cases where some fields (such as opa-owners) may be an
                # empty list
                if not field_value:
                    out_data[field] = None

                else:
                    out_data[field] = str(field_value)

            return out_data

    # If no match, return none but preserve existing address validity flags
    # Use original_address if provided, otherwise fall back to address parameter
    out_data["output_address"] = original_address if original_address else address
    out_data["is_addr"] = existing_is_addr
    out_data["is_philly_addr"] = existing_is_philly_addr
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
    existing_is_addr: bool = False,
    existing_is_philly_addr: bool = False,
    original_address: str = None,
) -> dict:
    """
    Helper function to throttle the number of API requests to 10 per second.
    """
    AIS_RATE_LIMITER.wait()
    return ais_lookup(
        sess,
        api_key,
        address,
        zip,
        enrichment_fields,
        existing_is_addr,
        existing_is_philly_addr,
        original_address,
    )
