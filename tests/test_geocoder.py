import pytest
from geocoder import build_enrichment_fields


def test_build_enrichment_fields_returns_fields_both_srids():
    config = {
        "enrichment_fields": [
            "census_tract_2020",
            "census_block_group_2020",
            "census_block_2020",
        ],
        "srid_4326": True,
        "srid_2272": True,
    }

    expected = (
        {"census_tract_2020", "census_block_group_2020", "census_block_2020"},
        {
            "census_tract_2020",
            "census_block_group_2020",
            "census_block_2020",
            "street_address",
            "geocode_lat",
            "geocode_lon",
            "geocode_x",
            "geocode_y"
        },
    )

    actual = build_enrichment_fields(config)

    assert expected == actual


def test_build_enrichment_fields_returns_fields_only_4326():
    config = {
        "enrichment_fields": [
            "census_tract_2020",
            "census_block_group_2020",
            "census_block_2020",
        ],
        "srid_4326": True,
        "srid_2272": False,
    }

    expected = (
        {"census_tract_2020", "census_block_group_2020", "census_block_2020"},
        {
            "census_tract_2020",
            "census_block_group_2020",
            "census_block_2020",
            "street_address",
            "geocode_lat",
            "geocode_lon",
        },
    )

    actual = build_enrichment_fields(config)

    assert expected == actual


def test_build_enrichment_fields_returns_fields_only_2272():
    config = {
        "enrichment_fields": [
            "census_tract_2020",
            "census_block_group_2020",
            "census_block_2020",
        ],
        "srid_4326": False,
        "srid_2272": True,
    }

    expected = (
        {"census_tract_2020", "census_block_group_2020", "census_block_2020"},
        {
            "census_tract_2020",
            "census_block_group_2020",
            "census_block_2020",
            "street_address",
            "geocode_x",
            "geocode_y",
        },
    )

    actual = build_enrichment_fields(config)

    assert expected == actual


def test_build_enrichment_fields_defaults_to_both_srids():
    """Test that when srid flags are not specified, both are included by default"""
    config = {
        "enrichment_fields": [
            "census_tract_2020",
            "census_block_group_2020",
            "census_block_2020",
        ],
        # No srid_4326 or srid_2272 specified - should default to True
    }

    expected = (
        {"census_tract_2020", "census_block_group_2020", "census_block_2020"},
        {
            "census_tract_2020",
            "census_block_group_2020",
            "census_block_2020",
            "street_address",
            "geocode_lat",
            "geocode_lon",
            "geocode_x",
            "geocode_y"
        },
    )

    actual = build_enrichment_fields(config)

    assert expected == actual


def test_build_enrichment_fields_errors_if_invalid_field():
    config = {
        "enrichment_fields": [
            "coordinates",
            "latitude",
            "longitude",
            "census_block_2020",
        ],
        "srid_4326": True,
        "srid_2272": True,
    }

    with pytest.raises(ValueError):
        build_enrichment_fields(config)