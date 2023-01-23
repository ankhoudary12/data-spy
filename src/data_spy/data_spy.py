"""Main console program for data-spy CLI tool."""

import click

from . import __version__
from .schema_compare import schema_compare


@click.command()
@click.argument("database")
@click.argument("schema")
@click.option("-r", "--regex", default=None)
@click.version_option(version=__version__)
def main(database: str, schema: str, regex: str) -> None:
    """Main function to run schema compare.

    Args:
        database: database of dev tables to run the schema compare against.
        schema: schema of dev tables to run the schema compare against.
        regex: regex pattern to select for dev tables to run schema compare.

    Returns:
        None
    """

    sc = schema_compare(database, schema, regex)

    sc.schema_compare_summary_diff()
    sc.schema_compare_column_level_diff()
    sc.schema_compare_row_level_diff()
