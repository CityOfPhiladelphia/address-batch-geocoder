import pytest
from geocoder import build_append_fields


def test_build_append_fields_returns_fields():
    config = {
        "append_fields": [
            "census_tract_2020",
            "census_block_group_2020",
            "census_block_2020",
        ]
    }

    expected = (
        ["census_tract_2020", "census_block_group_2020", "census_block_2020"],
        [
            "census_tract_2020",
            "census_block_group_2020",
            "census_block_2020",
            "street_address",
            "geocode_lat",
            "geocode_lon",
        ],
    )

    actual = build_append_fields(config)

    assert expected == actual


def test_build_append_fields_errors_if_invalid_field():
    config = {
        "append_fields": ["coordinates", "latitude", "longitude", "census_block_2020"]
    }

    with pytest.raises(ValueError):
        build_append_fields(config)
