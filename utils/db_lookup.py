import yaml, polars as pl
from sqlalchemy import create_engine, Engine as SQLAlchemyEngine

def create_sqa_engine(config_path: str) -> SQLAlchemyEngine:
    """
    Creates a sql alchemy engine based on connection information
    in the config file.

    Args: config_path

    Returns: SQL Alchemy Engine
    """
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    user = cfg["DB_USERNAME"]
    pwd  = cfg["DB_PASSWORD"]
    host = cfg["DB_HOST"]
    db   = cfg["DB_NAME"]
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}/{db}"

    return create_engine(url, future=True)

def insert_to_tmp_table(
        conn, 
        data: pl.LazyFrame, 
        table_name: str = "tmp_geo_append_table",
        batch_size: int = 100_000) -> None:
    """
    Streams a lazy dataframe into a postgres temp table.
    """

    temp_name = f"pg_temp.{table_name}"

    first = True
    for batch_df in data.collect_batches(chunk_size=batch_size):
        
        batch_df.write_database(
            table_name=temp_name,
            connection=conn,
            engine="sqlalchemy",
            if_table_exists="replace" if first else "append",
        )

        first = False
        

def get_lat_lon(conn, table_name) -> pl.LazyFrame:
    """
    Appends latitude and longitude to the incoming file in databridge.

    Args:
        conn: A SQLAlchemy connection object
        table_name: The name of the temp table to append to

    """
    
    sql_query = f"""
    SELECT b.*,
    geocode_lat::float8 as latitude,
    geocode_lon::float8 as longitude
    FROM {table_name} b
    LEFT JOIN citygeo.address_summary addr on b.output_address = addr.street_address
    """

    lf = pl.read_database(
        query=sql_query,
        connection=conn,
    ).lazy()

    return lf

def append(data: pl.LazyFrame, config_path) -> pl.LazyFrame:
    """
    Loads a polars frame of data to a temp table, and appends
    latitude and longitude to it.

    Args:
        data: A polars lazyframe of data to append to
        config_path: A path to the config file 
    """
    engine = create_sqa_engine(config_path)

    with engine.begin() as conn:
        table_name = f"tmp_geo_append_table"

        insert_to_tmp_table(conn, data, table_name, 100_000)
        results = get_lat_lon(conn, table_name)
    
    return results
