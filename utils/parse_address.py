import yaml
import re
import usaddress
import sys


def infer_city_state_field(address_fields) -> dict:
    """
    Args:
        config: The config object

    Returns dict: A dict mapping city and state fields to
    the field names in the user's input file
    """
    full_addr = address_fields.get("full_address_field")

    if full_addr:
        return {"full_address": full_addr}

    return address_fields


def tag_full_address(address: str):
    """
    Uses the usaddress module to extract
    city, state, and zip code from an address.
    Used to determine whether or not an address
    is in Philadelphia.

    Args:
        address (str): The address to tag
    """

    try:
        tagged, _ = usaddress.tag(address)

        city = tagged.get("PlaceName")
        state = tagged.get("StateName")
        zip_code = tagged.get("ZipCode")

        return {"city": city, "state": state, "zip": zip_code}

    except usaddress.RepeatedLabelError:
        return {"city": None, "state": None, "zip": None}


def flag_non_philly_address(address_data: dict, philly_zips: list) -> dict:
    """
    Given a dictionary that contains city, state, zip,
    determine whether or not an address is in Philly.

    Args:
        address_data (dict): A dictionary that may contain any
        combination of city, state, zip.
        philly_zips (list): A list of all valid Philadelphia zip codes

    Returns:
        Dict with 'is_non_philly' (bool) and 'is_undefined' (bool).
        is_undefined=True when we can't determine location with certainty.
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
        return {"is_non_philly": False, "is_undefined": False}  # in Philly

    # Case 2: If city is non philly or state is non PA, not in Philly:
    if city is not None and city not in philly_names:
        return {"is_non_philly": True, "is_undefined": False}

    if state is not None and state not in pa_names:
        return {"is_non_philly": True, "is_undefined": False}  # non-Philly

    # Case 3: Use ZIP when city or state are missing, assume Philly
    # if Zip is none:
    if zip_code is None:
        return {"is_non_philly": False, "is_undefined": True}  # Philly address

    if zip_code[:5] in philly_zips:
        return {"is_non_philly": False, "is_undefined": False}  # Philly address

    # City/state are null, zip not in philly
    else:
        return {"is_non_philly": True, "is_undefined": False}

def is_non_philly(address: str | dict | None, address_is_split: bool, zips) -> dict:
    """Determines whether an address is in Philadelphia. Handles
    A string full address or a dict split address.
    
    Args:
        address (str | dict | None)
        address_is_split (bool): whether or not the address is the full address, or split
        zips: list of Philadelphia zip codes
    
    Returns:
        dict: {'is_non_philly': bool, 'is_undefined': bool}
    """
    if not address:
        return {"is_non_philly": False, "is_undefined": True}
    
    # If address is in full address form, we need to tag it
    address_data = address if address_is_split else tag_full_address(address)

    return flag_non_philly_address(address_data, zips)

def find_address_fields(config) -> dict[str]:
    """
    Parses which address fields to consider in the input file based on
    the content of config.yml. Raises an error if neither full_address_field
    nor street are specified in the config file.

    Args:
        config (dict): A config object

    Returns dict: A dict of address field names in the input file.

    """
    # There are two possible ways to input address in the yaml config
    # 1. Specifying a full address string (if address is stored in one column)
    # 2. Specifying a list of address fields (address, city, state, zip) for
    # if address is stored in multiple columns.
    full_addr = config.get("full_address_field")

    addr_fields = config.get("address_fields") or {}

    # street_address used to be called street, adding this for backward compatibility
    # in case someone hasn't updated their config file to call it street_address
    if addr_fields.get("street") and not addr_fields.get("street_address"):
        addr_fields["street_address"] = addr_fields.pop("street")

    # If user has not specified an address field, raise
    if not full_addr and not any(addr_fields.values()):
        raise ValueError(
            "An address field or address fields must be specified in the config file."
        )

    # Handle cases where user has specified both a full address field
    # and separate address fields.
    resp = ""

    if full_addr and addr_fields:
        print(
            "You have specified both a full address and separate\n"
            "address fields in the config file.\n"
            "Press 1 to use the full address.\n"
            "Press 2 to use the address fields.\n"
             "Press or Q to quit.\n"
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
    
    if full_addr and not resp:
        return {"full_address": full_addr}
    
    if not addr_fields.get("street_address"):
        raise ValueError(
            "When full address field is not specified, "
            "address_fields must include a non-null value for "
            "street_address."
        )

    fields = {k: v for k, v in addr_fields.items() if v is not None}
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

    try:
        prsd = parser.parse(address)
        parsed = prsd["components"]
    
        has_street_code = False
        for street in ("street", "street_2"):
            sc = parsed.get(street, {}).get("street_code")
            if sc:
                has_street_code = True
                break

        # If address matches to a street code, it is a philly address
        is_addr = bool(has_street_code)
        is_philly_addr = bool(has_street_code)

        output_address = parsed.get("output_address", address)

    
    # Handle Passyunk parsing edge cases
    except Exception as e:
        output_address = address
        is_addr = False
        is_philly_addr = False

    return {
        "output_address": output_address,
        "is_addr": is_addr,
        "is_philly_addr": is_philly_addr,
        "is_multiple_match": False,
        "geocoder_used": None,
    }
