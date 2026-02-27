import pytest
import shutil
import polars as pl
import os
from click.testing import CliRunner
from geocoder import process_csv
from pathlib import Path

TEST_DIR = Path(__file__).parent
TEST_CSV = TEST_DIR / "sample_file_input.csv"
CONFIG_FILE_PATH = TEST_DIR / "config_for_tests.yml"

import yaml

@pytest.fixture(scope="session")
def geocoded_output(tmp_path_factory):
    if os.getenv("CI") == "true":
        pytest.skip("E2E tests not supported in CI")
        
    tmp = tmp_path_factory.mktemp("geocoder")

    input_path = tmp / TEST_CSV.name
    shutil.copy(TEST_CSV, input_path)

    with open(CONFIG_FILE_PATH) as f:
        config = yaml.safe_load(f)
    
    config["input_file"] = str(input_path)
    config["geography_file"] = str(TEST_DIR / "test_address_file.parquet")

    if os.getenv("AIS_API_KEY"):
        config["AIS_API_KEY"] = os.getenv("AIS_API_KEY")
    
    temp_config = tmp / "config_for_tests.yml"
    with open(temp_config, "w") as f:
        yaml.dump(config, f)

    runner = CliRunner()
    result = runner.invoke(process_csv, ["--config_path", str(temp_config)])

    assert result.exit_code == 0, result.output

    output_path = tmp / (TEST_CSV.stem + "_enriched.csv")

    return pl.read_csv(output_path)

def test_output_has_correct_row_count(geocoded_output):
    assert len(geocoded_output) == 9

def test_address_file_hit_has_coordinates(geocoded_output):
    addresses = ["1001 Loney Street", "1100 W Godfrey Ave Bldg A ent @ 1100 W. Godfrey Ave", "508 carver court", "market and broad street", "ENT @ 10945 E. KESWICK ROAD"]
    
    for address in addresses:
        row = geocoded_output.filter(pl.col("street_address") == address)
        
        assert row["geocode_lat"].item() is not None

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

@pytest.mark.skipif(
    os.getenv("AIS_API_KEY") is None, 
    reason="AIS_API_KEY not set"
)
def test_ais_match(geocoded_output):
    addresses = ["12th and mkt"]
    
    for address in addresses:
        row = geocoded_output.filter(pl.col("street_address") == address)
        
        assert row["geocoder_used"].item() == "ais"

