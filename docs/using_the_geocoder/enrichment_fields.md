---
description: A list of optional enrichment fields
icon: lucide/map-plus
---

# Enrichment Fields

Below is a list of fields that the geocoder can optionally enrich
an address with. 

Fields marked as `Full match only` will only be returned for addresses
that make a full address match to the address file, or AIS.

Fields marked as `Service area match` can be returned for both addresses
that make a full address match, or addresses that make a partial match
based on coordinates.

| **Field** | **Match Type** |
| --- | --- |
|`address_high`| Full match only |
|`address_low_frac`| Full match only |
|`address_low_suffix`| Full match only |
|`address_low`| Full match only |
|`bin`| Full match only |
|`census_block_2010`| Service area match |
|`census_block_2020`| Service area match |
|`census_block_group_2010`| Service area match |
|`census_block_group_2020`| Service area match |
|`census_tract_2010`| Service area match |
|`census_tract_2020`| Service area match |
|`center_city_district`| Service area match |
|`clean_philly_block_captain`| Service area match |
|`commercial_corridor`| Service area match |
|`council_district_2016`| Service area match |
|`council_district_2024`| Service area match |
|`cua_zone`| Service area match |
|`dor_parcel_id`| Full match only |
|`eclipse_location_id`| Full match only |
|`elementary_school`| Service area match |
|`engine_local`| Service area match |
|`h3_hex_grid_r7`| Service area match |
|`h3_hex_grid_r8`| Service area match |
|`h3_hex_grid_r9`| Service area match |
|`h3_hex_grid_r10`| Service area match |
|`high_school`| Service area match |
|`highway_district`| Service area match |
|`highway_section`| Service area match |
|`highway_subsection`| Service area match |
|`historic_district`| Service area match |
|`historic_site`| Service area match |
|`historic_street`| Service area match |
|`ladder_local`| Service area match |
|`lane_closure`| Service area match |
|`leaf_collection_area`| Service area match |
|`li_address_key`| Full match only |
|`li_district`| Service area match |
|`major_phila_watershed`| Service area match |
|`middle_school`| Service area match |
|`neighborhood_advisory_committee`| Service area match |
|`opa_account_num`| Full match only |
|`opa_address`| Full match only |
|`opa_owners`| Full match only |
|`philly_rising_area`| Service area match |
|`planning_district`| Service area match |
|`police_district`| Service area match |
|`police_division`| Service area match |
|`police_service_area`| Service area match |
|`political_division`| Service area match |
|`political_ward`| Service area match |
|`ppr_friends`| Service area match |
|`pwd_center_city_district`| Service area match |
|`pwd_maint_district`| Service area match |
|`pwd_parcel_id`| Full match only |
|`pwd_pressure_district`| Service area match |
|`pwd_treatment_plant`| Service area match | 
|`pwd_water_plate`| Service area match |
|`recycling_diversion_rate`| Service area match |
|`rubbish_recycle_day`| Service area match |
|`sanitation_area`| Service area match | 
|`sanitation_convenience_center`| Service area match |
|`sanitation_district`| Service area match |
|`secondary_rubbish_day`| Service area match |
|`seg_id`| Full match only |
|`state_house_rep_2012`| Service area match |
|`state_house_rep_2022`| Service area match |
|`state_senate_2012`| Service area match |
|`state_senate_2022`| Service area match |
|`street_code`| Full match only | 
|`street_light_route`| Service area match |
|`street_name`| Full match only |
|`street_postdir`| Full match only |
|`street_predir`| Full match only |
|`street_suffix`| Full match only |
|`traffic_district`| Service area match |
|`traffic_pm_district`| Service area match |
|`tobacco_free_school_zones`| Service area match |
|`tobacco_retailer_permit_capped`| Service area match |
|`unit_num`| Full match only |
|`unit_type`| Full match only |
|`us_congressional_2012`| Service area match |
|`us_congressional_2018`| Service area match |
|`us_congressional_2022`| Service area match |
|`zip_4`| Full match only |
|`zip_code`| Full match only |
|`zoning_document_ids`| Full match only |
|`zoning_rco`| Service area match |
|`zoning`| Service area match | 