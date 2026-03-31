import polars as pl
import click

@click.command()
@click.option(
    "--input_path"
)
@click.option(
    "--output_path"
)
def convert_to_parquet(input_path, output_path):

    # Scan CSV into a lazy dataframe to avoid loading it all
    # into memory
    pl.scan_csv(input_path, infer_schema=False).sink_parquet(output_path)


if __name__ == "__main__":
    convert_to_parquet()
