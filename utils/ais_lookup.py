import requests
from retrying import retry
from .rate_limiter import RateLimiter
from urllib.parse import quote
from dataclasses import dataclass, field, asdict
import urllib3

# Suppress the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

AIS_RATE_LIMITER = RateLimiter(max_calls=5, period=1.0)
CANARY_ADDRESS = "1234 Market St"


@dataclass
class AISResult:
    output_address: str
    is_addr: bool
    is_philly_addr: bool
    is_multiple_match: bool
    geocoder_used: str = field(default=None)
    geocode_lat: str = field(default=None)
    geocode_lon: str = field(default=None)
    geocode_x: str = field(default=None)
    geocode_y: str = field(default=None)


# ------- Authenticate ------- #
def validate_api_key(sess, api_key) -> None:
    """
    Used to determine whether a given API key is valid. Raises an error if not valid.

    Args:
        sess: A requests session object
        api_key: The API key to test
    """
    params = {"gatekeeperKey": api_key, "client_id": api_key}
    
    try:
        response = sess.get(f"https://api-prod.phila.gov/ais/v1/search/{quote(CANARY_ADDRESS)}", params=params)
    
    except requests.RequestException as e:
        raise Exception(f"Couldn't connect to AIS.") from e
    
    if response.status_code == 401:
        raise Exception("401 response. Invalid API key.")

# ------- Helper Functions -------- #
def tiebreak(features: list[dict], zip, strict: bool = False) -> dict:
    """
    If more than one result is returned by AIS, tiebreak by checking zip code.
    If no zip code is provided, return None and a flag that indicates a
    duplicate match.

    Args:
        response (dict): An AIS API response
        zip (str): The zip code present on the input data. Used
        to check API responses against.
        strict (bool): Whether or not to return the first address or none
        if there is more than one match

    Returns:
        A dict with the zipcode-matched record, or if no match, None.
    """

    candidates = []

    # Match only on first five of zip
    input_zip = zip[:5] if zip else ""
    for candidate in features:

        # If the AIS API zip code matches the zip code on the
        # incoming data, this record is a potential match
        candidate_zip = candidate.get("properties", {}).get("zip_code", "")
        if candidate_zip == input_zip or not zip:
            candidates.append(candidate)

    # Sometimes AIS returns two addresses for the same lat lon
    # should write code in the future to more intelligently tiebreak
    # and behaves differently based on if the two addresses returned
    # are actually the same

    if candidates:
        if strict:
            return candidates[0] if len(candidates) == 1 else None

        else:
            return candidates[0]

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


@retry(
    wait_exponential_multiplier=2000,
    wait_exponential_max=20000,
    stop_max_attempt_number=5,
)
def _lookup_service_area(sess: requests.Session, lat: int, lon: int, api_key: str):
    AIS_RATE_LIMITER.wait()
    ais_url = f"https://api-prod.phila.gov/ais/v1/service_areas/{lon},{lat}"
    params = {}

    # To handle backwards compatibility with people still using a gatekeeper key instead of
    # a client ID, we set both params here
    params["gatekeeperKey"] = api_key
    params["client_id"] = api_key

    response = sess.get(ais_url, params=params, timeout=10)

    if response.status_code >= 500:
        raise Exception("5xx response. There may be a problem with the AIS API.")
    elif response.status_code == 429:
        raise Exception("429 response. Too many calls to the AIS API.")

    elif response.status_code == 401:
        raise Exception("401 response. Invalid API key.")

    elif response.status_code == 200:
        return response.json()

    else:
        raise ValueError(
            f"Error occurred with the following status code: {response.status_code}"
        )


def fetch_service_area_enrichment_data(
    sess: requests.Session, api_key: str, lat: str, lon: str, enrichment_fields: list
) -> dict:
    """
    Looks up latitude and longitude against the AIS API service area endpoint,

    Args:
        sess: a requests Session object
        api_key: an AIS API key
        lat: latitude
        lon: longitude
        enrichment_fields: which fields to return from AIS

    Returns:
        (dict): A dictionary of enrichment data
    """

    result = _lookup_service_area(sess, lat, lon, api_key)

    if not result:
        return {}

    return {
        field: result.get("service_areas", {}).get(field) for field in enrichment_fields
    }


def parse_address_lookup(resp: dict, zip: str, enrichment_fields: list) -> dict:
    """
    Given an AIS address lookup response, tiebreak and get the street address
    and enrichment fields. Return as a dict.

    Args:
    resp (dict): A response from AIS
    enrichment_fields (list): A list of fields from the response to include
    """

    matched_address = None

    if len(resp["features"]) > 1:
        matched_address = tiebreak(resp["features"], zip, strict=True)

    # if json is not longer than 1, no need to tiebreak
    elif len(resp["features"]) == 1:
        matched_address = resp["features"][0]

    # If tiebreak fails, return
    # null values for most fields.
    if not matched_address:
        return {}

    # If we successfully got a tiebroken_address, process it

    out_address = matched_address.get("properties", "").get("street_address", "")

    lon, lat = matched_address["geometry"]["coordinates"]

    enriched_fields = {
        field: matched_address.get("properties", {}).get(field)
        for field in enrichment_fields
    }

    return {
        "output_address": out_address,
        "lat": lat,
        "lon": lon,
        "enriched_fields": enriched_fields,
    }


def parse_intersection_lookup(
    sess: requests.Session,
    api_key: str,
    resp: dict,
    original_address,
    zip: str,
    enrichment_fields: list,
) -> dict:
    features = resp.get("features")

    # Pick best feature
    feature = tiebreak(features, zip) if len(features) > 1 else features[0]
    if not feature:
        return {}

    # Get service area data back for intersection
    lon, lat = feature["geometry"]["coordinates"]

    enriched_fields = fetch_service_area_enrichment_data(
        sess, api_key, lat, lon, enrichment_fields
    )

    # We use the original address here because the address that we use
    # to search against AIS may be augmented with PHILADELPHIA, PA
    # if no city, state exists
    # We use original lat and lon here because its higher precision
    # than what the service area endpoint returns
    return {
        "output_address": original_address,
        "lat": lat,
        "lon": lon,
        "enriched_fields": enriched_fields,
    }


def _round_coordinates(coord) -> str:
    """Round and stringify a coordinate value, returning None if invalid."""
    try:
        return str(round(float(coord), 8))
    except (TypeError, ValueError):
        return None


def _fetch_ais_coordinates(
    sess: requests.Session, api_key: str, address: str, zip: str, srid: int
):
    """
    Fetches coordinates for a specific SRID. Returns (coord1, coord2) or
    (None, None) if failed.
    """

    AIS_RATE_LIMITER.wait()
    ais_url = f"https://api-prod.phila.gov/ais/v1/search/{quote(address)}"
    
    params = {}

    # To handle backwards compatibility with people still using a gatekeeper key instead of
    # a client ID, we set both params here
    params["gatekeeperKey"] = api_key
    params["client_id"] = api_key
    params["srid"] = srid
    params["max_range"] = 0

    response = sess.get(ais_url, params=params)

    if response.status_code >= 500:
        raise Exception("5xx response. There may be a problem with the AIS API.")
    elif response.status_code == 429:
        raise Exception("429 response. Too many calls to the AIS API.")
    elif response.status_code == 200:
        r_json = response.json()

        if r_json.get("features") and len(r_json["features"]) > 0:
            feature = r_json["features"][0]

            # Tiebreak if multiple results
            if len(r_json["features"]) > 1:
                feature = tiebreak(r_json["features"], zip)
                if not feature:
                    return None, None

            coord1, coord2 = feature["geometry"]["coordinates"]
            return str(coord1), str(coord2)

        return None, None

    return None, None


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
    enrichment_fields: list = None,
    existing_is_addr: bool = False,
    existing_is_philly_addr: bool = False,
    original_address: str = None,
    fetch_4326: bool = True,
    fetch_2272: bool = True,
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
        fetch_4326 (bool): Whether to fetch SRID 4326 coordinates (lat/lon)
        fetch_2272 (bool): Whether to fetch SRID 2272 coordinates (x/y)

    Returns:
        A dict with standardized address, latitude and longitude,
        and user-requested fields.
    """
    AIS_RATE_LIMITER.wait()

    # Don't attempt to geocode if address is null
    if address:
        ais_url = f"https://api-prod.phila.gov/ais/v1/search/{quote(address)}"

        params = {}

        # To handle backwards compatibility with people still using a gatekeeper key instead of
        # a client ID, we set both params here
        params["gatekeeperKey"] = api_key
        params["client_id"] = api_key
        params["srid"] = 4326
        params["max_range"] = 0

        try:
            response = sess.get(ais_url, params=params)
        except:
            print(
                f"Warning: AIS lookup failed for this address: {address}, {zip}, {original_address}"
            )
            response = None
    else:
        response = None
    
    if response and response.status_code >= 500:
        raise Exception("5xx response. There may be a problem with the AIS API.")
    elif response and response.status_code == 429:
        raise Exception("429 response. Too many calls to the AIS API.")

    # Initialize lat and lon values
    (
        lat,
        lon,
    ) = (
        None,
        None,
    )
    geocode_lat, geocode_lon, geocode_x, geocode_y = None, None, None, None

    # If status code is 200, that means API has found a match.
    # API will return a 404 if no match
    if response and response.status_code == 200:
        # If r_json is longer than 1, multiple matches
        # were returned and we need to tiebreak
        r_json = response.json()

        search_type = r_json.get("search_type")

        if search_type == "address":

            parsed_response = parse_address_lookup(r_json, zip, enrichment_fields)

            if not parsed_response:
                # If no match, return
                # null values for most fields.
                # Tiebreaking has failed in this case
                # so is_multiple_match = True
                normalized_addr = r_json.get("normalized", "")

                ais_result = AISResult(
                    output_address=normalized_addr if normalized_addr else address,
                    is_addr=False,
                    is_philly_addr=True,
                    is_multiple_match=True,
                    geocoder_used="ais-full-match",
                )

                return asdict(ais_result)

        # Intersection returns a different data structure with fewer
        # possible enrichment fields, so we need to handle this differently
        elif search_type == "intersection":

            parsed_response = parse_intersection_lookup(
                sess, api_key, r_json, original_address, zip, enrichment_fields
            )

            # If tiebreak fails, return
            # null values for most fields.
            if not parsed_response:
                ais_result = AISResult(
                    output_address=original_address if original_address else address,
                    is_addr=False,
                    is_philly_addr=True,
                    is_multiple_match=False,
                    geocoder_used="ais-intersection",
                )

                return asdict(ais_result)

        # We use the original address here because the address that we use
        # to search against AIS may be augmented with PHILADELPHIA, PA
        # if no city, state exists

        lat = parsed_response["lat"]
        lon = parsed_response["lon"]
        out_address = parsed_response["output_address"]

        if fetch_4326:
            # Don't need to make another lookup, we already have
            # coords from first lookup
            # get latitude and longitude from address search only
            # other searches -- against the service_areas endpoint
            # return lat/lon with less precision, so we just use the original
            # lat, lon

            geocode_lat = _round_coordinates(lat)
            geocode_lon = _round_coordinates(lon)

        if fetch_2272:
            geo_x, geo_y = _fetch_ais_coordinates(sess, api_key, out_address, zip, 2272)

            geocode_x = _round_coordinates(geo_x)
            geocode_y = _round_coordinates(geo_y)

        ais_result = AISResult(
            output_address=out_address if out_address else address,
            is_addr=True if search_type == "address" else False,
            is_philly_addr=True,
            is_multiple_match=False,
            geocoder_used=(
                "ais-full-match" if search_type == "address" else "ais-intersection"
            ),
            geocode_lat=geocode_lat,
            geocode_lon=geocode_lon,
            geocode_x=geocode_x,
            geocode_y=geocode_y,
        )

        return asdict(ais_result) | parsed_response["enriched_fields"]

    # If no match, return none but preserve existing address validity flags
    # Use original_address if provided, otherwise fall back to address parameter
    ais_result = AISResult(
        output_address=original_address if original_address else address,
        is_addr=existing_is_addr,
        is_philly_addr=existing_is_philly_addr,
        is_multiple_match=False,
    )

    return asdict(ais_result)
