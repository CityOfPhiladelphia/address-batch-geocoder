# Utils Reference

## Address Parsing
::: utils.parse_address
    options:
        show_root_heading: true
        members:
          - find_address_fields
          - parse_address
          - is_non_philly

## Geocoding
::: utils.ais_lookup
    options:
        show_root_heading: true
        members:
          - ais_lookup
          - fetch_service_area_enrichment_data

::: utils.tomtom_lookup
    options:
        show_root_heading: true
        members:
          - tomtom_lookup

## Infrastructure
::: utils.cache
    options:
        show_root_heading: true
        members:
          - LRUCache

::: utils.encoder
    options:
        show_root_heading: true
        members:
          - detect_file_encoding
          - recode_to_utf8

::: utils.rate_limiter
    options:
        show_root_heading: true
        members:
          - RateLimiter