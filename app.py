import streamlit as st
import pandas as pd
import os
import tempfile
import yaml
from utils.enrichment_fields import ENRICHMENT_FIELDS
from geocoder import process_data


AIS_API_KEY = os.environ.get("AIS_API_KEY")
ADDRESS_FILE = './geocoder_address_data/address_service_area_summary.parquet'

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

def init_session_state():
    """Initialize all session state defaults on first run."""

    defaults = {
        "api_key_default": "",
        "address_format_default": "Single address field",
        "full_address_field_default": None,
        "street_col_default": None,
        "city_col_default": None,
        "state_col_default": None,
        "zip_col_default": None,
        "geocode_result": None,
        "geocode_error": None
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

def on_config_upload():
    """
    Parse uploaded config YML and populate default variables.
    """
    uploaded = st.session_state.get("config_upload")
    if not uploaded:
        return

    try:
        config = yaml.safe_load(uploaded)
    
    except yaml.YAMLError as e:
        st.session_state["config_load_error"] = f"Could not parse config file: {e}"
        return
    
    st.session_state["config_load_error"] = None

    st.session_state["api_key"] = config.get("AIS_API_KEY", "")

    srids = []
    if config.get("srid_4326"):
        srids.append(4326)
    if config.get("srid_2272"):
        srids.append(2272)
    st.session_state["srids"] = srids

    st.session_state["enrichment_fields"] = config.get("enrichment_fields", [])

    full_address_field = config.get("full_address_field")
    address_fields = config.get("address_fields") or {}

    if full_address_field:
        st.session_state["address_format_default"] = "Single address field"
        st.session_state["full_address_field_default"] = full_address_field
    elif address_fields:
        st.session_state["address_format_default"] = "Separate address / city / state / zip"
        st.session_state["street_col_default"] = address_fields.get("street_address")
        st.session_state["city_col_default"] = address_fields.get("city")
        st.session_state["state_col_default"] = address_fields.get("state")
        st.session_state["zip_col_default"] = address_fields.get("zip")
    
def call_geocoder_backend(data, config):

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

# Prevent app from rerunning when this is clicked
@st.fragment
def download_config(config):
    st.download_button(
        label="Download config",
        data=yaml.dump(config),
        file_name="streamlit_config.yml",
    )

def update_fields():
    if st.session_state.config_upload:
        
        config = yaml.safe_load(st.session_state.config_upload)

        st.session_state.api_key = config.get("AIS_API_KEY", "")
        st.session_state.srids = [k for k, v in {4326: "srid_4326", 2272: "srid_2272"}.items() if config.get(v)]
        st.session_state.full_address_field = config.get("full_address_field")
        st.session_state.address_fields = config.get("address_fields")
        st.session_state.enrichment_fields = config.get("enrichment_fields", [])
        st.session_state.config = config

def main():
    init_session_state()

    # --- API Key ---
    api_key = st.text_input(
        "Address Information System (AIS) API key. Required.",
        value=st.session_state["api_key_default"],
        key="api_key"
    )

    # --- CSV Upload & Preview ---
    uploaded_file = st.file_uploader("Upload a CSV", type=["csv"])

    full_address_field = None
    address_fields = {}

    if uploaded_file is not None:
        preview_df = pd.read_csv(uploaded_file, nrows=5, encoding="latin-1")
        st.subheader(":blue[Preview (first 5 rows)]")
        st.dataframe(preview_df)
        uploaded_file.seek(0)

        columns = list(preview_df.columns)
    
        # --- Address Format ---
        st.subheader(":blue[Map Address Fields]")

        format_options = ["Single address field", "Separate address / city / state / zip"]
        format_default_idx = format_options.index(st.session_state["address_format_default"])
        address_format = st.radio(
            "Address format",
            format_options,
            index=format_default_idx,
            horizontal=True
        )

        if address_format == "Single address field":
            default_col = st.session_state["full_address_field_default"]

            # If the mapped field is in the uploaded file, map it. Otherwise, do nothing. 
            # Prevents people from mapping fields that don't exist in the actual file
            default_idx = columns.index(default_col) if default_col in columns else 0
            if default_col and default_col not in columns:
                st.warning(f"Config field '{default_col}' not found in this CSV. Please re-map.")
            full_address_field = st.selectbox("Address field", columns, index=default_idx)
        
        else:
            selected_cols = []
            col1, col2, col3, col4 = st.columns(4)

            def col_index(opts, default):
                return opts.index(default) if default in opts else 0
            
            with col1:
                opts = filtered_options(columns, set(selected_cols))
                street_col = st.selectbox("Street Address", opts,
                                        index=col_index(opts, st.session_state["street_col_default"]))
                selected_cols.append(street_col)
            with col2:
                opts = filtered_options(columns, set(selected_cols), none_option=True)
                city_col = st.selectbox("City", opts,
                                        index=col_index(opts, st.session_state["city_col_default"]))
                selected_cols.append(city_col)
            with col3:
                opts = filtered_options(columns, set(selected_cols), none_option=True)
                state_col = st.selectbox("State", opts,
                                        index=col_index(opts, st.session_state["state_col_default"]))
                selected_cols.append(state_col)
            with col4:
                opts = filtered_options(columns, set(selected_cols), none_option=True)
                zip_col = st.selectbox("Zip", opts,
                                        index=col_index(opts, st.session_state["zip_col_default"]))
                selected_cols.append(zip_col)
            
            for label, default_key in [
                ("Street Address", "street_col_default"),
                ("City", "city_col_default"),
                ("State", "state_col_default"),
                ("Zip", "zip_col_default"),
            ]:
                saved = st.session_state[default_key]
                if saved and saved not in columns:
                    st.warning(f"Config field '{saved} ({label}) not found in this CSV. Please re-map.")

            address_fields = {
                k: v for k, v in {
                    "street_address": street_col,
                    "city": city_col,
                    "state": state_col,
                    "zip": zip_col
                }.items() if v and v != "(none)"
            }
    
    # --- SRID & Enrichment Fields ---
    srids = st.multiselect(
        "Choose which SRIDs to append. Required.",
        [4326, 2272],
        key="srids"
    )

    enrichment_fields = st.multiselect(
        "Choose which fields to add to your data",
        ENRICHMENT_FIELDS,
        key="enrichment_fields"
    )

    # --- Config Upload ---
    st.file_uploader(
        "Load a previously saved config file.",
        type=".yml",
        key="config_upload",
        on_change=on_config_upload
    )

    if st.session_state.get("config_load_error"):
        st.error(st.session_state["config_load_error"])
    
    # --- Build Config ---
    config = {
        "AIS_API_KEY": api_key,
        "input_file": uploaded_file.name if uploaded_file else None,
        "address_file": ADDRESS_FILE,
        "full_address_field": full_address_field,
        "address_fields": address_fields,
        "enrichment_fields": enrichment_fields,
        "srid_4326": 4326 in srids,
        "srid_2272": 2272 in srids,

    }

    # --- Geocode ---
    ready_to_geocode = (
        uploaded_file
        and api_key
        and any(srid in srids for srid in [4326, 2272])
        and (full_address_field or address_fields)
    )

    if ready_to_geocode:
        if st.button("Geocode"):
            with st.spinner("Geocoding..."):
                try:
                    result_bytes = call_geocoder_backend(uploaded_file, config)
                    st.session_state["geocode_result"] = result_bytes
                    st.session_state["geocode_error"] = None
                except ValueError as e:
                    st.session_state["geocode_error"] = f"Configuration error: {e}"
                except Exception as e:
                    st.session_state["geocode_error"] = f"Error: {e}"
        
        download_config(config)
    
    if st.session_state.get("geocode_error"):
        st.error(st.session_state["geocode_error"])
    
    if st.session_state.get("geocode_result"):
        st.success("Geocoding complete!")
        st.download_button(
            label="Download enriched file",
            data=st.session_state["geocode_result"],
            file_name=f"{uploaded_file.name}_enriched.csv",
            mime="text/csv",
            icon=":material/download:",
            on_click="ignore",
        )

if __name__ == "__main__":
    main()