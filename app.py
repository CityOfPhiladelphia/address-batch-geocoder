import streamlit as st
import pandas as pd
import os
import tempfile
import traceback
from utils.enrichment_fields import ENRICHMENT_FIELDS
from geocoder import process_data


AIS_API_KEY = os.environ.get("AIS_API_KEY")
GEOGRAPHY_FILE = './geocoder_address_data/address_service_area_summary.parquet'

# UI Configurations
st.set_page_config(page_title="Address Batch Geocoder", 
                   page_icon=":globe-with-meridians:",
                   layout="wide")

st.markdown(" # :blue[Address Batch Geocoder]")

def filtered_options(columns, exclude: set, none_option=False) -> list:
    if none_option:
        return ["(none)"] + [c for c in columns if c not in exclude]
    else:
        return [c for c in columns if c not in exclude]

def toggle_submit() -> None:
    pass


def enrichment_field_selector() -> set:
    enrichment_fields = st.multiselect(
        "Choose which fields to add to your data",
        ENRICHMENT_FIELDS
    )

    return enrichment_fields

def srid_selector() -> set:
    srids = st.multiselect(
        "Choose which SRIDs to append. Required.",
        [4326, 2272]
    )

    return srids


def call_geocoder_backend(data, full_address_field, address_fields, enrichment_fields, srid_4326, srid_2272):
    config = {
        "AIS_API_KEY": AIS_API_KEY,
        "geography_file": GEOGRAPHY_FILE,
        "full_address_field": full_address_field,
        "address_fields": address_fields,
        "enrichment_fields": list(enrichment_fields),
        "srid_4326": srid_4326,
        "srid_2272": srid_2272,
    }

    # Write uploaded file to a temp file so process_data can work with a filepath
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as tmp:
        tmp.write(data.read())
        tmp_path = tmp.name

    try:
        result, utf8_filepath = process_data(tmp_path, config)
        try:
            df = result.collect()
        finally:
            if utf8_filepath:
                os.remove(utf8_filepath)
    finally:
        os.remove(tmp_path)

    return df.write_csv().encode("utf-8")


def download_config():
    pass

def file_uploader() -> tuple[bytes, dict, set]:
    """
    Setup the file uploader element.
    """

    uploaded_file = st.file_uploader("Upload a CSV", type=["csv"])
    full_address_field = None
    address_fields = {}
    enrichment_fields = []
    srid_4326 = False
    srid_2272 = False

    if uploaded_file is not None:
        preview_df = pd.read_csv(uploaded_file, nrows=5, encoding="latin-1")
        st.subheader("Preview (first 5 rows)")
        st.dataframe(preview_df)
        uploaded_file.seek(0)

        columns = list(preview_df.columns)

        st.subheader("Map Address Fields")
        address_format = st.radio(
            "Address format",
            ["Single address field", "Separate address / city / state / zip"],
            horizontal=True,
        )

        if address_format == "Single address field":
            full_address_field = st.selectbox("Address field", columns)
            st.write(full_address_field)
            
        else:
            selected_cols = []
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                street_col = st.selectbox("Street Address", filtered_options(columns, selected_cols))
                selected_cols.append(street_col)
            with col2:
                city_col = st.selectbox("City", filtered_options(columns, selected_cols, none_option=True))
                selected_cols.append(city_col)
            with col3:
                state_col = st.selectbox("State", filtered_options(columns, selected_cols, none_option=True))
                selected_cols.append(state_col)
            with col4:
                zip_col = st.selectbox("Zip", filtered_options(columns, selected_cols, none_option=True))
                selected_cols.append(zip_col)
            
            # Only include fields user actually mapped
            address_fields = {
                k: v for k, v in {
                    "street_address": street_col,
                    "city": city_col,
                    "state": state_col,
                    "zip": zip_col,
                }.items() if v and v != '(none)'
            }

        enrichment_fields = enrichment_field_selector()
        srids = srid_selector()
        srid_4326 = 4326 in srids
        srid_2272 = 2272 in srids

    return uploaded_file, full_address_field, address_fields, enrichment_fields, srid_4326, srid_2272
            
def main():
    uploaded, full_address_field, address_fields, enrichment_fields, srid_4326, srid_2272 = file_uploader()

    has_address = full_address_field or address_fields.get("street_address")
    has_srid = srid_4326 or srid_2272
    ready_to_geocode = uploaded and has_srid and has_address

    if ready_to_geocode:
        if st.button("Geocode"):
            with st.spinner("Geocoding..."):
                try:
                    result_bytes = call_geocoder_backend(
                        uploaded,
                        full_address_field,
                        address_fields,
                        enrichment_fields,
                        srid_4326,
                        srid_2272,
                    )
                    st.session_state["geocode_result"] = result_bytes
                except ValueError as e:
                    st.session_state["geocode_error"] = f"Configuration error: {e}"
                except Exception as e:
                    st.session_state["geocode_error"] = f"Error: {e}"

    if "geocode_error" in st.session_state:
        st.error(st.session_state["geocode_error"])

    if "geocode_result" in st.session_state:
        st.success("Geocoding complete!")
        st.download_button(
            label="Download enriched file",
            data=st.session_state["geocode_result"],
            file_name="enriched_addresses.csv",
            mime="text/csv",
            icon=":material/download:",
            on_click="ignore",
        )

if __name__ == "__main__":
    main()