import os
from mapping.ais_properties_fields import POSSIBLE_FIELDS

ADDRESS_FILE = './geocoder_address_data/address_service_area_summary.parquet'
ENRICHMENT_FIELDS = sorted(POSSIBLE_FIELDS.keys())
HOW_TO_FILEPATH = './address-geocoder-main/ui/how_to.md'
