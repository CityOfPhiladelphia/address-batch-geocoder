import streamlit as st
import pandas as pd
import os
import yaml
from mapping.ais_properties_fields import POSSIBLE_FIELDS
from geocoder import Geocoder

AIS_API_KEY = os.environ.get("AIS_API_KEY")
ADDRESS_FILE = './geocoder_address_data/address_service_area_summary.parquet'
ENRICHMENT_FIELDS = sorted(POSSIBLE_FIELDS.keys())

HOME_DIR = os.path.expanduser('~')
# UI Configurations
# --------- Header and Title -------- #
st.set_page_config(page_title="Address Batch Geocoder", 
                   page_icon=":globe-with-meridians:",
                   layout="wide")

st.markdown(" # :blue[Address Batch Geocoder]")

 
def filtered_options(columns, exclude: set, none_option=False) -> list:
    """
    Returns a filtered list of options, based on what options have already been
    selected
    """
    if none_option:
        return ["(none)"] + [c for c in columns if c not in exclude]
    else:
        return [c for c in columns if c not in exclude]

def init_session_state():
    """Initialize all session state defaults on first run."""

    defaults = {
        "yaml_upload": None,
        "yaml_config_path": None,
        "api_key_default": "",
        "address_format_default": "Single address field",
        "input_filepath": "",
        "input_loaded": False,
        "out_path": None,
        "full_address_field_default": None,
        "resume": False,
        "street_col_default": None,
        "city_col_default": None,
        "state_col_default": None,
        "zip_col_default": None,
        "geocode_result": None,
        "file_not_found_error": None,
        "geocode_error": None,
        "running": False
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ------- Event Handlers --------- #
def on_config_upload():
    """
    Parse uploaded config YML and populate default variables.
    """
    uploaded = st.session_state.get("yaml_config_path")
    if not uploaded:
        return

    try:
        config = yaml.safe_load(uploaded)
    
    except yaml.YAMLError as e:
        st.session_state["config_load_error"] = f"Could not parse config file: {e}"
        return
    
    st.session_state["input_filepath"] = config.get("input_file", "")

    st.session_state["config_load_error"] = None

    st.session_state["api_key_default"] = config.get("AIS_API_KEY", "")

    srids = []
    if config.get("srid_4326"):
        srids.append(4326)
    if config.get("srid_2272"):
        srids.append(2272)
    st.session_state["srids"] = srids

    st.session_state["enrichment_fields"] = config.get("enrichment_fields", [])

    st.session_state["resume"] = config.get("resume", False)

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

def on_filepath_change():
    st.session_state["input_filepath"] = st.session_state["input_filepath"].strip('"\'')
    st.session_state["input_loaded"] = False
    st.session_state["geocode_result"] = None

def on_file_load():
    st.session_state["input_loaded"] = True

def on_geocode_click():
    st.session_state["running"] = True

def on_resume_change():
    st.session_state["resume"] = st.session_state["resume_select"] == "Resume a partially geocoded file (address mapping must match the previous run)"


# Prevent app from rerunning when this is clicked
@st.fragment
def download_config(config):
    config_for_download = {**config} # Make input file none since streamlit cannot access full filepaths
    st.download_button(
        label="Download config",
        data=yaml.dump(config_for_download),
        file_name="geocoder_config.yml",
    )

def render_yaml_upload():
    # --- Config Upload ---
    st.file_uploader(
        "Load a previously saved config file.",
        type=".yml",
        key="yaml_config_path",
        on_change=on_config_upload
    )

    if st.session_state.get("config_load_error"):
        st.error(st.session_state["config_load_error"])


def render_config_form() -> dict:

    config = {}
    
    if st.session_state.get("yaml_config_path") or st.session_state.get("yaml_upload") == "Enter settings below":
        # --- Resume or not ---
        st.subheader(":blue[Choose a run type]")

        resume_opts = ["Geocode a new file", "Resume a partially geocoded file (address mapping must match the previous run)"]

        st.segmented_control(
            "Run type",
            options=resume_opts,
            key="resume_select",
            selection_mode="single",
            default=resume_opts[0] if not st.session_state.get("resume") else resume_opts[1],
            on_change=on_resume_change,
        )

        st.subheader(":blue[Configure your run below:]")

        # --- API Key ---
        api_key = st.text_input(
            "Address Information System (AIS) API key. Required.",
            value=st.session_state["api_key_default"],
            key="api_key"
        )

        # --- CSV Upload & Preview ---
        st.text_input(label="Input file: (paste the full filepath here)", 
                    help="Full file path to the file you wish to geocode.", 
                    value=st.session_state.get("input_filepath", ""),
                    key="input_filepath",
                    on_change=on_filepath_change
                    )

        if st.session_state.get("input_filepath").endswith(".csv"):
            if st.button("Load file", type="primary", on_click=on_file_load):                
                st.session_state["geocode_result"] = None

        full_address_field = None
        address_fields = {}

        if st.session_state.get("input_filepath") and st.session_state.get("input_loaded"):
            try:
                preview_df = pd.read_csv(st.session_state["input_filepath"], nrows=5, encoding="utf-8-sig")
                st.subheader(":blue[Preview (first 5 rows)]")
                st.dataframe(preview_df)
                st.session_state["input_loaded"]
            
            except FileNotFoundError as e:
                st.session_state["file_not_found_error"] = f"Error: {e}"
                st.error(st.session_state["file_not_found_error"])
                return {}

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
        
        enrichment_fields = []
        # --- SRID & Enrichment Fields ---
        if st.session_state["input_loaded"] and not st.session_state["resume"]:
            st.subheader(":blue[Select Enrichment Fields]")
            st.multiselect(
                "Choose which coordinate system (SRID) to use when geocoding. Required.",
                [4326, 2272],
                key="srids"
            )

            enrichment_fields = st.multiselect(
                "Choose which fields to add to your data",
                ENRICHMENT_FIELDS,
                key="enrichment_fields"
            )
        
        else:
            enrichment_fields = []

        config = {
            "AIS_API_KEY": api_key,
            "input_file": st.session_state["input_filepath"] if st.session_state["input_loaded"] else None,
            "address_file": ADDRESS_FILE,
            "full_address_field": full_address_field,
            "address_fields": address_fields,
            "resume": st.session_state["resume"], 
            "enrichment_fields": enrichment_fields,
            "srid_4326": 4326 in st.session_state.get("srids", []),
            "srid_2272": 2272 in st.session_state.get("srids", []),
        }

    return config

def render_geocode_button(config):
    # --- Geocode ---
    ready_to_geocode = (
    st.session_state["input_loaded"]
    and config.get("AIS_API_KEY")
    and (st.session_state["resume"] or config.get("srid_4326") or config.get("srid_2272"))
    and (config.get("full_address_field") or config.get("address_fields"))
)

    if ready_to_geocode:
        st.markdown(":blue[Geocoding large files could take a while. Please do not refresh the page.]")
        st.caption("To stop geocoding, close the application window — closing this browser tab will not stop the process.")
        if st.button("Geocode", on_click=on_geocode_click, disabled=st.session_state["running"], type="primary"):
            with st.status("Geocoding... this may take a while. Output file being written to", expanded=True) as status:
                try:
                    gc = Geocoder(config)
                    gc.geocode()
                    st.session_state["geocode_error"] = None
                    st.session_state["out_path"] = gc.out_path
                    status.update(label = "Geocoding complete!", state="complete")
                except ValueError as e:
                    st.session_state["geocode_error"] = f"Configuration error: {e}"
                    status.update(label="Configuration error", state="error")
                except Exception as e:
                    st.session_state["geocode_error"] = f"Error: {e}"
                    status.update(label="Geocoding failed", state="error")
                finally:
                    st.session_state["running"] = False

                    # Only show success message if there wasn't an error
                    if not st.session_state.get("geocode_error"):
                        st.session_state["geocode_result"] = True
            
            st.rerun()
        
        download_config(config)

    if st.session_state.get("geocode_error"):
        st.error(st.session_state["geocode_error"])

    if st.session_state.get("geocode_result"):
        st.success(f"Geocoding complete! File available at {st.session_state['out_path']}")

def main():
    init_session_state()
    config = {}

    # --- YAML Config Upload Prompt --- #
    st.segmented_control(
        "How would you like to configure this run?",
        options=["Load settings from a file", "Enter settings below"],
        selection_mode="single",
        default=None,
        key="yaml_upload"
    )

    if st.session_state["yaml_upload"] == "Load settings from a file":
        render_yaml_upload()
        config = render_config_form()
    
    if st.session_state["yaml_upload"] == "Enter settings below":
        config = render_config_form()
        
    render_geocode_button(config)

if __name__ == "__main__":
    main()