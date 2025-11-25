import yaml, re, polars as pl, usaddress, sys
from typing import List


def infer_city_state_field(config_path) -> dict:
    """
    Args:
        config_path (str): The path of the config file.

    Returns dict: A dict mapping city and state fields to
    the field names in the user's input file
    """

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    full_addr = config.get("full_address_field")

    if full_addr:
        return {"full_address": full_addr}

    addr_fields = config.get("address_fields") or {}

    return {
        "city": addr_fields.get("city"),
        "state": addr_fields.get("state"),
        "zip": addr_fields.get("zip"),
    }


def tag_full_address(address: str):
    """
    Uses the usaddress module to extract
    city, state, and zip code from an address.
    Used to determine whether or not an address
    is in Philadelphia.

    Args:
        address (str): The address to tag
    """

    tagged, _ = usaddress.tag(address)

    city = tagged.get("PlaceName")
    state = tagged.get("StateName")
    zip_code = tagged.get("ZipCode")

    return {"city": city, "state": state, "zip": zip_code}


def flag_non_philly_address(address_data: dict, philly_zips: list) -> bool:
    """
    Given a dictionary that contains city, state, zip,
    determine whether or not an address is in Philly.

    Args:
        address_data (dict): A dictionary that may contain any
        combination of city, state, zip.
        philly_zips (list): A list of all valid Philadelphia zip codes

    Returns:
        True if non philly address, false otherwise.
    """
    city = address_data.get("city")
    state = address_data.get("state")
    zip_code = address_data.get("zip")

    if city:
        city = city.lower().strip()
    if state:
        state = state.lower().strip()
    # If there's any whitespace in the zip code, it will mess up slicing
    # to get only ZIP5.
    if zip_code:
        zip_code = zip_code.strip()

    philly_names = {"philadelphia", "phila", "philly"}
    pa_names = {"pennsylvania", "pa", "penn"}

    # Case 1: If Philly city and state, treat as Philly regardless
    # of zip:
    if city in philly_names and state in pa_names:
        return False  # in Philly

    # Case 2: If city is non philly or state is non PA, not in Philly:
    if city is not None and city not in philly_names:
        return True  # non-Philly
    if state is not None and state not in pa_names:
        return True  # non-Philly

    # Case 3: Use ZIP when city or state are missing, assume Philly
    # if Zip is none:
    if zip_code is None:
        return False  # Philly address

    if zip_code[:5] in philly_zips:
        return False  # Philly address

    # City/state are null, zip not in philly
    else:
        return True


def is_non_philly_from_full_address(address: str, *, philly_zips: list) -> bool:
    """
    Helper function that allows the flag_non_philly_address
    to be run as a mapped function within polars.

    Args:
        address (str)
        philly_zips (list)

    Returns:
        bool: True if the address is not within Philly.
    """
    if address is None:
        return False

    address_data = tag_full_address(address)

    return flag_non_philly_address(address_data, philly_zips)


def is_non_philly_from_split_address(
    address_data: dict,
    *,
    zips: list,
) -> bool:
    """
    Address_data: A row from a polars struct with keys
    'city', 'state', 'zip'.

    Zips are frozen with partial.

    Returns true if the address is non-philly.
    """
    if address_data is None:
        return False

    return flag_non_philly_address(address_data, zips)


def find_address_fields(config_path) -> List[str]:
    """
    Parses which address fields to consider in the input file based on
    the content of config.yml. Raises an error if neither full_address_field
    nor street are specified in the config file.

    Args:
        config_path (str): The path of the config file.

    Returns dict: A dict of address field names in the input file.

    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # There are two possible ways to input address in the yaml config
    # 1. Specifying a full address string (if address is stored in one column)
    # 2. Specifying a list of address fields (address, city, state, zip) for
    # if address is stored in multiple columns.
    full_addr = config.get("full_address_field")

    addr_fields = config.get("address_fields")

    # If user has not specified an address field, raise
    if not full_addr and not any(addr_fields.values()):
        raise ValueError(
            "An address field or address fields must be specified "
            "in the config file."
        )

    # Handle cases where user has specified both a full address field
    # and separate address fields.
    resp = ""

    if full_addr and addr_fields:

        print(
            "You have specified both a full address and separate"
            "address fields in the config file. "
            "Press 1 to use the full address, "
            "2 to use the address fields, or Q to quit."
        )

        while resp.lower() not in ["1", "2", "q", "quit"]:
            if full_addr and addr_fields:
                resp = input("Specify which fields to use: ")

            if resp == "1":
                return {"full_address": full_addr}

            elif resp == "2":
                break

            else:
                print("Exiting program...")
                sys.exit()

    if not addr_fields.get("street"):
        raise ValueError(
            "When full address field is not specified, "
            "address_fields must include a non-null value for "
            "street."
        )

    fields = addr_fields
    return fields


def combine_fields(fields: list, record: dict):
    joined = " ".join(record[field] for field in fields)

    # Strip residual spaces left from blank fields
    return re.sub(r"\s+", " ", joined)


def parse_address(parser, address: str) -> tuple[str, bool, bool]:
    """
    Given an address string, uses PassyunkParser to return
    a standardized address, and whether or not the given string
    is an extant address in Philadelphia. Makes some attempt
    to normalize alternate spellings of addresses: eg, 123 Mkt will
    evaluate to 123 MARKET ST

    Args:
        parser: A PassyunkParser object
        address: An address string

    Returns tuple(str, bool, bool): tuple with the standardized address, a
    boolean value indicating if the string is formatted as an address,
    and a boolean value indicating if the address is a valid Philadelphia
    address.
    """
    parsed = parser.parse(address)["components"]

    is_addr = parsed["address"]["isaddr"]
    # If address matches to a street code, it is a philly address
    is_philly_addr = bool(parsed["street"]["street_code"])

    output_address = parsed["output_address"] if is_philly_addr else address

    return {
        "output_address": output_address,
        "is_addr": is_addr,
        "is_philly_addr": is_philly_addr,
        "is_multiple_match": False,
        "match_type": None,
    }
