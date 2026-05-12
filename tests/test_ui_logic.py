from ui.logic import filtered_options, ready_to_geocode, parse_config


def test_filtered_options_excludes_columns():
    columns = ["city", "state", "address", "zip"]
    selected_columns = ["city", "zip"]
    filtered_columns = filtered_options(columns, selected_columns)

    assert filtered_columns == ["state", "address"]


def test_filtered_options_includes_none():
    columns = ["city", "state", "address", "zip"]
    selected_columns = {"city", "zip"}
    filtered_columns = filtered_options(columns, selected_columns, none_option=True)

    assert filtered_columns[0] == "(none)"


def test_filtered_options_returns_all_when_none_selected():
    columns = ["city", "state", "address", "zip"]
    selected_columns = {}
    filtered_columns = filtered_options(columns, selected_columns)

    assert filtered_columns == ["city", "state", "address", "zip"]


def test_filtered_options_excludes_everything_when_all_selected():
    columns = ["city", "state", "address", "zip"]
    selected_columns = {"city", "state", "address", "zip"}
    filtered_columns = filtered_options(columns, selected_columns)

    assert filtered_columns == []


def test_ready_to_geocode_all_conditions_met_returns_true():

    session_state = {
        "input_loaded": True,
        "resume": True,
    }

    config = {"AIS_API_KEY": 123, "full_address_field": "address"}

    assert ready_to_geocode(session_state, config) == True


def test_ready_to_geocode_input_not_loaded_returns_false():
    session_state = {
        "input_loaded": False,
        "resume": True,
    }

    config = {"AIS_API_KEY": 123, "full_address_field": "address"}

    assert ready_to_geocode(session_state, config) == False


def test_ready_to_geocode_no_api_key_returns_false():
    session_state = {
        "input_loaded": True,
        "resume": True,
    }

    config = {"AIS_API_KEY": None, "full_address_field": "address"}

    assert ready_to_geocode(session_state, config) == False


def test_ready_to_geocode_no_srid_no_resume_returns_false():
    session_state = {
        "input_loaded": True,
        "resume": False,
    }

    config = {
        "AIS_API_KEY": 123,
        "full_address_field": "address",
    }

    assert ready_to_geocode(session_state, config) == False


def test_ready_to_geocode_no_srid_resume_returns_true():
    session_state = {
        "input_loaded": True,
        "resume": True,
    }

    config = {
        "AIS_API_KEY": 123,
        "full_address_field": "address",
    }

    assert ready_to_geocode(session_state, config) == True


def test_ready_to_geocode_no_address_field_returns_false():
    session_state = {
        "input_loaded": True,
        "resume": True,
    }

    config = {
        "AIS_API_KEY": 123,
        "full_address_field": "",
    }

    assert ready_to_geocode(session_state, config) == False


def test_parse_config_handles_full_address_field():

    yaml_dict = {"full_address_field": "address"}

    result = parse_config(yaml_dict)

    assert result["address_format_default"] == "Single address field"
    assert result["full_address_field_default"] == "address"


def test_parse_config_handles_address_fields():

    yaml_dict = {
        "address_fields": {
            "street_address": "street_address",
            "city": "city",
            "state": "state",
            "zip": "zip",
        }
    }

    result = parse_config(yaml_dict)

    assert result["address_format_default"] == "Separate address / city / state / zip"
    assert result["street_col_default"] == "street_address"
    assert result["city_col_default"] == "city"
    assert result["state_col_default"] == "state"
    assert result["zip_col_default"] == "zip"


def test_parse_config_full_address_field_wins():

    yaml_dict = {
        "full_address_field": "address",
        "address_fields": {
            "street_address": "street_address",
            "city": "city",
            "state": "state",
            "zip": "zip",
        },
    }

    result = parse_config(yaml_dict)

    assert result["address_format_default"] == "Single address field"
    assert result.get("street_col_default") == None
    assert result.get("city_col_default") == None
    assert result.get("state_col_default") == None
    assert result.get("zip_col_default") == None
