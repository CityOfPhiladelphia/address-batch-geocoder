import yaml, re
from typing import List


def find_address_fields(config_path) -> List[str]:
    """
    Parses which address fields to consider in the input file based on
    the content of config.yml. Raises an error if neither full_address_field
    nor street are specified in the config file.

    Args:
        config_path (str): The path of the config file.

    Returns list: A list of address field names in the input file.

    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    full_addr = config.get("full_address_field")
    if full_addr:
        return [full_addr]

    addr_fields = config.get("address_fields") or {}

    if not addr_fields.get("street"):
        raise ValueError(
            "When full address field is not specified, "
            "address_fields must include a non-null value for "
            "street."
        )

    fields = [v for v in addr_fields.values() if v is not None]
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

    output_address = parsed["output_address"]
    is_addr = parsed["address"]["isaddr"]
    # If address matches to a street code, it is a philly address
    is_philly_addr = bool(parsed["street"]["street_code"])

    return {
        "output_address": output_address,
        "is_addr": is_addr,
        "is_philly_addr": is_philly_addr,
    }
