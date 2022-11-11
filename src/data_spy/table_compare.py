"""Module for table_compare class."""

import logging

from decouple import config

import pandas as pd

from snowflake_utils import snowflake_ctx


logging.basicConfig(level=logging.INFO)

prod_database = config("PROD_DATABASE")
prod_schema = config("PROD_SCHEMA")
model_meta_table = config("MODEL_META_TABLE")


class table_compare:
    """Class for comparing two Snowflake tables and materializing diff in the warehouse."""

    def __init__(self, dev_table: str, prod_table: str) -> None:
        """Initialize with table names."""
        self.dev_table = dev_table
        self.prod_table = prod_table

        self.dev_database = "analytics"
        self.dev_schema = "dev_akhoudary"
        self.prod_database = prod_database
        self.prod_schema = prod_schema
        self.model_meta_table = model_meta_table

        self.sf_ctx = snowflake_ctx()

    def _create_sf_ctx(self) -> None:
        """Create a snowflake cursor object."""
        sf = snowflake_ctx()._connect()
        return sf

    def _create_table_level_diff_object(self) -> None:
        """Creates a summary table to house the diff statistics."""

        sql = f"""
        CREATE OR REPLACE TABLE { self.dev_database}.{ self.dev_schema }.diff_summary 
        (
            table_name TEXT,
            rowcount_dev NUMBER,
            rowcount_prod NUMBER,
            column_count_dev NUMBER,
            column_count_prod NUMBER,
            distinct_pk_dev NUMBER,
            distinct_pk_prod NUMBER
        );
        """

        self.sf_ctx.execute_sql(sql)

    def _get_primary_key(self, table: str) -> str:
        """Returns the primary key associated with a table as found in the model meta table.

        Args:
            table: table you want to retrieve a primary key for.

        Returns:
            str: the primary key of the table.
        """

        sql = f"""
        SELECT 
            model_meta['primary-key'] :: string
        FROM { self.prod_database }.{ self.prod_schema }.{ self.model_meta_table }
        WHERE model_name = '{ table }'
        """
        return self.sf_ctx.execute_sql_and_return_single_value(sql)

    def _get_freshness_key(self, table: str) -> str:
        """Returns a freshness key associated with a table as found in the model meta table.

        Args:
            table: table you want to retrieve a freshness key for.

        Returns:
            str: the freshness key of the table.
        """

        sql = f"""
        SELECT 
            model_meta['freshness-key'] :: string
        FROM { self.prod_database }.{ self.prod_schema }.{ self.model_meta_table }
        WHERE model_name = '{ table }'
        """

        return self.sf_ctx.execute_sql_and_return_single_value(sql)

    def summary_diff(self) -> None:
        """Runs a high level diff on the dev/prod tables and materializes output in dev database."""
        # get primary keys for each table
        dev_pkey = self._get_primary_key(self.dev_table)
        print(f"Dev key: {dev_pkey}")
        prod_pkey = self._get_primary_key(self.prod_table)
        print(f"Prod key: {prod_pkey}")
        # get freshness keys for each table.
        dev_freshness_key = self._get_freshness_key(self.dev_table)
        print(f"Dev freshness key: {dev_freshness_key}")
        prod_freshness_key = self._get_freshness_key(self.prod_table)
        print(f"Prod freshness key: {prod_freshness_key}")

        sql = f"""
        
        WITH dev AS (
            SELECT
                *
            FROM { self.dev_database}.{ self.dev_schema }.{ self.dev_table }
            WHERE { dev_freshness_key } < CURRENT_DATE()
        ),

        prod AS (
            SELECT
                *
            FROM { self.prod_database}.{ self.prod_schema }.{ self.prod_table }
            WHERE { prod_freshness_key } < CURRENT_DATE()
        )

        SELECT
            '{ self.dev_table }' AS table_name,
            (SELECT COUNT(*) FROM dev ) AS rowcount_dev,
            (SELECT COUNT(*) FROM prod ) AS rowcount_prod,
            (SELECT COUNT(*) FROM { self.dev_database }.information_schema.columns WHERE LOWER(table_schema) = '{ self.dev_schema }' AND LOWER(table_name) = '{ self.dev_table }') AS column_count_dev,
            (SELECT COUNT(*) FROM { self.prod_database }.information_schema.columns WHERE LOWER(table_schema) = '{ self.prod_schema }' AND LOWER(table_name) = '{ self.prod_table }') AS column_count_prod,
            (SELECT COUNT(DISTINCT { dev_pkey }) FROM dev ) AS distinct_pkey_dev,
            (SELECT COUNT(DISTINCT { prod_pkey }) FROM prod ) AS distinct_pkey_prod 
            ;
        """

        df = self.sf_ctx.sql_to_df(sql)

        self.sf_ctx.write_df_to_snowflake(
            df, self.dev_database, self.dev_schema, f"{self.dev_table}_summary_diff"
        )
