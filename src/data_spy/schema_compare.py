""" Module for evaluating diffs between schemas."""
import logging

from decouple import config

import pandas as pd

from snowflake_utils import snowflake_ctx

from tabulate import tabulate

from table_compare import table_compare

logging.basicConfig(level=logging.INFO)

prod_database = config("PROD_DATABASE")
prod_schema = config("PROD_SCHEMA")


class schema_compare:
    """Class for comparing two snowflake schemas. Assumes dev/prod separation."""

    def __init__(
        self, dev_database: str, dev_schema: str, table_regex: str = None
    ) -> None:
        """Initialize with schema names.

        Args:
            dev_schema: name of dev schema.
            table_regex: regex pattern to match dev tables to. IE only want to compare dim/fct tables.
        """
        self.dev_database = dev_database
        self.dev_schema = dev_schema
        self.prod_database = prod_database
        self.prod_schema = prod_schema

        if not table_regex:
            self.table_regex = "fct_.*|dim_.*"
        else:
            self.table_regex = table_regex

        self.sf_ctx = snowflake_ctx()

        self.tables_to_compare = self._set_tables_to_compare()

    def _set_tables_to_compare(self) -> list:
        """Gets list of tables to run comparisons on.
        Only tables that exist in both dev/prod should be considered.

        Args:
            None

        Returns:
            list: list of tables to compare.
        """

        # we only want to compare tables that exist in the prod db
        sql = f"""
        WITH prod_tables AS (
            SELECT
                LOWER(table_name) AS table_name
            FROM {self.prod_database}.information_schema.tables
            WHERE LOWER(table_schema) = '{self.prod_schema}'
            AND RLIKE(LOWER(table_name), '{self.table_regex}','i')
        )

            SELECT
                LOWER(table_name) AS table_name
            FROM {self.prod_database}.information_schema.tables
            WHERE LOWER(table_schema) = '{self.dev_schema}'
            AND RLIKE(LOWER(table_name), '{self.table_regex}','i')
            AND LOWER(table_name) IN (SELECT table_name FROM prod_tables);
        """

        table_df = self.sf_ctx.sql_to_df(sql)
        tables = table_df["TABLE_NAME"].values.tolist()
        return tables

    def schema_compare_summary_diff(self) -> pd.DataFrame:
        """Creates a table compare object for each table, runs the summary_diff() method.

        Args:
            None

        Returns:
            pd.DataFrame: dataframe object representing all of the table's summary diff.
        """

        if not self.tables_to_compare:
            logging.info("No tables to run schema compare on!")
            return

        print(f"Running comparison for the following tables: {self.tables_to_compare}")
        # create empty list to store all of the dataframes
        df_list = []
        for table in self.tables_to_compare:
            tc = table_compare(table)
            df = tc.summary_diff()
            df_list.append(df)

        # export single df
        summary_df = pd.concat(df_list)

        print("Summary Output: ")
        print(tabulate(summary_df, headers="keys", tablefmt="psql"))

        self.sf_ctx.write_df_to_snowflake(
            summary_df, self.dev_database, self.dev_schema, "data_diff_summary"
        )

    def schema_compare_row_level_diff(self) -> None:
        """Creates a table compare object for each table, runs the row__level_diff() method.

        Args:
            None

        Returns:
            None: Instead materailzies the results all within the Snowflake database since this can
            be a computationally intensive operation.
        """

        for table in self.tables_to_compare:
            tc = table_compare(table)
            logging.info(f"Running the row_level_diff() for {table}")
            tc.row_level_diff()


sc = schema_compare("analytics", "dev_akhoudary")

sc.schema_compare_row_level_diff()
