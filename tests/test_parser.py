import pytest, yaml
from passyunk.parser import PassyunkParser
from functools import partial
from utils.parse_address import parse_address, combine_fields, find_address_fields

p = PassyunkParser()
parse = partial(parse_address, p)


def write_yaml(tmp_path, data, name="config.yml"):
    p = tmp_path / name
    p.write_text(yaml.safe_dump(data, sort_keys=False))
    return str(p)


def test_uses_full_address_when_present(tmp_path):
    cfg_path = write_yaml(
        tmp_path,
        {
            "full_address_field": "street_address",
            "address_fields": {"street": "addr_st", "city": "addr_city"},
        },
    )

    assert find_address_fields(cfg_path) == ["street_address"]


def test_falls_back_to_component_fields(tmp_path):
    cfg_path = write_yaml(
        tmp_path,
        {
            "full_address_field": None,
            "address_fields": {"street": "addr_st", "city": "addr_city"},
        },
    )

    assert find_address_fields(cfg_path) == ["addr_st", "addr_city"]


def test_raises_if_street_missing(tmp_path):
    cfg_path = write_yaml(
        tmp_path,
        {
            "full_address_field": None,
            "address_fields": {"city": "addr_city", "zip": "addr_zip"},
        },
    )

    with pytest.raises(ValueError, match="street"):
        find_address_fields(cfg_path)


def test_raises_if_street_null(tmp_path):
    cfg_path = write_yaml(
        tmp_path,
        {
            "full_address_field": None,
            "address_fields": {"street": None, "city": "addr_city"},
        },
    )

    with pytest.raises(ValueError, match="street"):
        find_address_fields(cfg_path)


def test_raises_when_both_null(tmp_path):
    cfg_path = write_yaml(
        tmp_path, {"full_address_field": None, "address_fields": {"street": None}}
    )

    with pytest.raises(ValueError, match="street"):
        find_address_fields(cfg_path)


def test_parse_real_address():
    parsed = parse("123 mkt")

    addr = parsed["output_address"]
    is_addr = parsed["is_addr"]
    is_philly_addr = parsed["is_philly_addr"]

    assert addr == "123 MARKET ST"
    assert is_addr == True
    assert is_philly_addr == True


def test_parse_non_philly_address():
    parsed = parse("123 fake st")

    addr = parsed["output_address"]
    is_addr = parsed["is_addr"]
    is_philly_addr = parsed["is_philly_addr"]

    assert addr == "123 FAKE ST"
    assert is_addr == True
    assert is_philly_addr == False


def test_parse_non_address():
    parsed = parse("not an address")
    addr = parsed["output_address"]
    is_addr = parsed["is_addr"]
    is_philly_addr = parsed["is_philly_addr"]

    assert addr == "NOT AN ADDRESS"
    assert is_addr == False
    assert is_philly_addr == False


def test_combine_fields_merges_correctly():
    record = {"id": 1, "street_address": "1234 market st", "city": "", "state": "PA"}

    fields = ["street_address", "city", "state"]

    result = combine_fields(fields, record)

    assert result == "1234 market st PA"


def test_combine_fields_handles_single_field():
    record = {"id": 1, "street_address": "1234 market st"}

    fields = ["street_address"]

    result = combine_fields(fields, record)

    assert result == "1234 market st"
