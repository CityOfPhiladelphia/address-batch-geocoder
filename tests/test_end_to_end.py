import pytest
import shutil
import polars as pl
import os
import socket
from click.testing import CliRunner
from geocoder import run_process_csv, Geocoder
from pathlib import Path
from unittest.mock import patch

TEST_DIR = Path(__file__).parent
TEST_CSV = TEST_DIR / "sample_file_input.csv"
CONFIG_FILE_PATH = TEST_DIR / "config_for_tests.yml"

import yaml


def tomtom_is_reachable():
    try:
        socket.getaddrinfo("citygeo-geocoder-aws.phila.city", 443)
        return True
    except socket.gaierror:
        return False


pytestmark = pytest.mark.skipif(
    not tomtom_is_reachable(), reason="Tomtom host unreachable"
)


@pytest.fixture(scope="session")
def geocoded_output(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("geocoder")

    input_path = tmp / TEST_CSV.name
    shutil.copy(TEST_CSV, input_path)

    with open(CONFIG_FILE_PATH) as f:
        config = yaml.safe_load(f)

    config["input_file"] = str(input_path)
    config["address_file"] = str(TEST_DIR / "test_address_file.parquet")

    config["AIS_API_KEY"] = os.getenv("AIS_API_KEY", "test_dummy_key")

    temp_config = tmp / "config_for_tests.yml"
    with open(temp_config, "w") as f:
        yaml.dump(config, f)

    runner = CliRunner()
    result = runner.invoke(run_process_csv, ["--config_path", str(temp_config)])

    assert result.exit_code == 0, result.output

    output_path = tmp / (TEST_CSV.stem + "_enriched.csv")

    return pl.read_csv(output_path)


@pytest.fixture()
def geocoded_output_resumed(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("geocoder")

    input_path = tmp / TEST_CSV.name
    shutil.copy(TEST_CSV, input_path)

    with open(CONFIG_FILE_PATH) as f:
        config = yaml.safe_load(f)

    config["input_file"] = str(input_path)
    config["address_file"] = str(TEST_DIR / "test_address_file.parquet")

    config["AIS_API_KEY"] = os.getenv("AIS_API_KEY", "test_dummy_key")

    temp_config = tmp / "config_for_tests.yml"
    with open(temp_config, "w") as f:
        yaml.dump(config, f)

    # Phase 1: partial run
    gc = Geocoder(config)
    gc.batch_size = 1

    original_write_batch = gc._write_batch
    call_count = 0

    def write_then_crash(batch, out_path):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            raise Exception("Simulated crash")
        return original_write_batch(batch, out_path)

    with patch.object(gc, "_write_batch", side_effect=write_then_crash):

        try:
            gc.geocode()

        except Exception:
            pass

    # Phase 2: resume
    config["resume"] = True
    gc2 = Geocoder(config)
    gc2.geocode()

    output_path = tmp / (TEST_CSV.stem + "_enriched.csv")

    return pl.read_csv(output_path)


def test_output_has_correct_row_count(geocoded_output):
    assert len(geocoded_output) == 9


def test_address_file_hit_has_coordinates(geocoded_output):
    addresses = ["1001 Loney Street"]

    for address in addresses:
        row = geocoded_output.filter(pl.col("street_address") == address)
        assert row["geocode_lat"].item() is not None, f"{address} has null geocode_lat"


def test_bad_address_has_no_coordinates(geocoded_output):
    addresses = ["dfdfa sdhl; dort@"]

    for address in addresses:
        row = geocoded_output.filter(pl.col("street_address") == address)

        assert row["geocode_lat"].item() is None


def test_out_of_phila_coded_by_tomtom(geocoded_output):
    cities = ["Mc Kees Rocks", "Lawnside"]

    for city in cities:
        row = geocoded_output.filter(pl.col("address_city") == city)

        assert row["geocoder_used"].item() == "tomtom"


def test_address_file_match(geocoded_output):
    addresses = ["1001 Loney Street"]

    for address in addresses:
        row = geocoded_output.filter(pl.col("street_address") == address)

        assert row["geocoder_used"].item() == "address_file"


def test_tomtom_address_returns_coordinates(geocoded_output):
    city = "Lawnside"
    row = geocoded_output.filter(pl.col("address_city") == city)

    assert row["geocode_lat"].item() == pytest.approx(39.8755899, rel=1e-3)
    assert row["geocode_lon"].item() == pytest.approx(-75.03612616, rel=1e-3)
    assert row["geocode_x"].item() == pytest.approx(2730093.07070462, rel=1e-3)
    assert row["geocode_y"].item() == pytest.approx(209237.2950039, rel=1e-3)


def test_api_address_has_right_coordinates(geocoded_output):
    address = "1100 W Godfrey Ave Bldg A ent @ 1100 W. Godfrey Ave"
    row = geocoded_output.filter(pl.col("street_address") == address)

    assert row["geocode_lat"].item() == pytest.approx(40.04610199, rel=1e-3)
    assert row["geocode_lon"].item() == pytest.approx(-75.13838509, rel=1e-3)
    assert row["geocode_x"].item() == pytest.approx(2699567.12316782, rel=1e-3)
    assert row["geocode_y"].item() == pytest.approx(270461.85786862, rel=1e-3)


class Test_Resume:
    def test_resumed_output_right_length(self, geocoded_output_resumed):
        assert len(geocoded_output_resumed) == 9

    def test_resumed_output_equals_full_output(
        self, geocoded_output, geocoded_output_resumed
    ):
        assert geocoded_output.equals(geocoded_output_resumed)


@pytest.mark.skipif(os.getenv("AIS_API_KEY") is None, reason="AIS_API_KEY not set")
def test_ais_match(geocoded_output):
    addresses = ["12th and mkt"]

    for address in addresses:
        row = geocoded_output.filter(pl.col("street_address") == address)

        assert row["geocoder_used"].item() == "ais-intersection"
