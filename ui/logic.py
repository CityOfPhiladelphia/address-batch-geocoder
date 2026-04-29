def filtered_options(columns, exclude: set, none_option=False) -> list:
    """
    Returns a filtered list of options, based on what options have already been
    selected
    """
    if none_option:
        return ["(none)"] + [c for c in columns if c not in exclude]
    else:
        return [c for c in columns if c not in exclude]


def ready_to_geocode(session_state: dict, config: dict):
    """Determines whether or not user may geocode based on input they have given."""
    return bool(
    session_state["input_loaded"]
    and config.get("AIS_API_KEY")
    and (session_state["resume"] or config.get("srid_4326") or config.get("srid_2272"))
    and (config.get("full_address_field") or config.get("address_fields"))
)

def parse_config(yaml_dict: dict) -> dict:
    """
    Normalize a loaded YAML config into session state values.
    """
    config = {k: (v if v is not None else "") for k, v in yaml_dict.items()}

    srids = []
    if config.get("srid_4326"):
        srids.append(4326)
    if config.get("srid_2272"):
        srids.append(2272)

    parsed_config = {
        "input_filepath" : config.get("input_file", ""),
        "config_load_error": None,
        "api_key": config.get("AIS_API_KEY", ""),
        "srids": srids,
        "enrichment_fields": config.get("enrichment_fields", []),
        "resume": config.get("resume", False),
        "full_address_field" : config.get("full_address_field", ""),
        "address_fields" : config.get("address_fields") or {},
    }

    full_address_field = config.get("full_address_field")
    address_fields = config.get("address_fields") or {}

    if full_address_field:
        parsed_config["address_format_default"] = "Single address field"
        parsed_config["full_address_field_default"] = full_address_field
    elif address_fields:
        parsed_config["address_format_default"] = "Separate address / city / state / zip"
        parsed_config["street_col_default"] = address_fields.get("street_address")
        parsed_config["city_col_default"] = address_fields.get("city")
        parsed_config["state_col_default"] = address_fields.get("state")
        parsed_config["zip_col_default"] = address_fields.get("zip")
    
    return parsed_config