"""Module for table_compare class."""

import logging

from decouple import config

import jinja2

import pandas as pd

from snowflake_utils import snowflake_ctx


logging.basicConfig(level=logging.INFO)

prod_database = config("PROD_DATABASE")
prod_schema = config("PROD_SCHEMA")
model_meta_table = config("MODEL_META_TABLE")


class table_compare:
    """Class for comparing two Snowflake tables and materializing diff in the warehouse.
    Assumes this is a dbt implementation where dev/prod tables are the same name, different schemas.
    """

    def __init__(self, table: str) -> None:
        """Initialize with table names."""
        self.dev_table = table
        self.prod_table = table

        self.dev_database = "analytics"
        self.dev_schema = "dev_akhoudary"
        self.prod_database = prod_database
        self.prod_schema = prod_schema
        self.model_meta_table = model_meta_table

        self.sf_ctx = snowflake_ctx()

        # set primary/freshness keys for each table
        self.dev_pkey = self.set_primary_key(self.dev_table)
        self.prod_pkey = self.set_primary_key(self.prod_table)

        self.dev_freshness_key = self.set_freshness_key(self.dev_table)
        self.prod_freshness_key = self.set_freshness_key(self.prod_table)

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

    def set_primary_key(self, table: str) -> str:
        """Sets internal params for primary key.

        Args:
            table: table you want to retrieve pkey for.

        Returns:
            str: pkey for the table.
        """

        pkey = self._get_primary_key(table)
        return pkey

    def set_freshness_key(self, table: str) -> str:
        """Sets internal param for freshness key.

        Args:
            table: table you want to retrieve freshness key for.

        Returns:
            str: freshness key for the table.
        """

        freshness_key = self._get_freshness_key(table)
        return freshness_key

    def get_columns_in_table(self, database: str, schema: str, table: str) -> list:
        """Gets list of column names for a table.

        Args:
            database: name of database where information schema lives.
            schema: name of schema of table.
            table: name of table to query.

        Returns:
            list: list of column names for that table.
        """

        sql = f"""
            SELECT
                column_name
            FROM {database}.information_schema.columns
            WHERE LOWER(table_schema) = '{schema}'
            AND LOWER(table_name) = '{table}'
        """
        # get column names as a df and convert to list
        column_df = self.sf_ctx.sql_to_df(sql)
        columns = column_df["COLUMN_NAME"].values.tolist()

        return columns

    def summary_diff(self) -> pd.DataFrame:
        """Runs a high level diff on the dev/prod tables and materializes output in dev database.

        Args:
            None

        Returns: df: DataFrame object representing the summary diff.
        """

        sql = f"""
        
        WITH dev AS (
            SELECT
                *
            FROM { self.dev_database}.{ self.dev_schema }.{ self.dev_table }
            WHERE { self.dev_freshness_key } < CURRENT_DATE()

        ),
        prod AS (
            SELECT
                *
            FROM { self.prod_database}.{ self.prod_schema }.{ self.prod_table }
            WHERE { self.prod_freshness_key } < CURRENT_DATE()
        )

        SELECT
            '{ self.dev_table }' AS table_name,
            (SELECT COUNT(*) FROM dev ) AS rowcount_dev,
            (SELECT COUNT(*) FROM prod ) AS rowcount_prod,
            (rowcount_dev - rowcount_prod) / rowcount_prod AS rowcount_diff,
            (SELECT COUNT(*) FROM { self.dev_database }.information_schema.columns WHERE LOWER(table_schema) = '{ self.dev_schema }' AND LOWER(table_name) = '{ self.dev_table }') AS column_count_dev,
            (SELECT COUNT(*) FROM { self.prod_database }.information_schema.columns WHERE LOWER(table_schema) = '{ self.prod_schema }' AND LOWER(table_name) = '{ self.prod_table }') AS column_count_prod,
            (column_count_dev - column_count_prod) / column_count_prod AS column_count_diff,
            (SELECT COUNT(DISTINCT { self.dev_pkey }) FROM dev ) AS distinct_pkey_dev,
            (SELECT COUNT(DISTINCT { self.prod_pkey }) FROM prod ) AS distinct_pkey_prod,
            (distinct_pkey_dev - distinct_pkey_prod) / distinct_pkey_prod AS distinct_pkey_diff
            ;
        """

        return self.sf_ctx.sql_to_df(sql)

    def row_level_diff(self) -> None:
        """Executes a row level diff of the two tables and outputs a new table in the dev schema."""

        dev_columns = self.get_columns_in_table(
            self.dev_database, self.dev_schema, self.dev_table
        )

        prod_columns = self.get_columns_in_table(
            self.prod_database, self.prod_schema, self.prod_table
        )

        # we want to materialize this output all intra-database since it could be a large dataset
        # using jinja2 formatting to prefix all column names with dev/prod to avoid duplicate column error
        sql = """
        CREATE OR REPLACE TABLE {{ dev_database }}.{{ dev_schema }}.{{ dev_table }}_raw_diff AS (

            WITH dev AS (
                SELECT
                    {{ dev_pkey }} AS dev_primary_key,
                    {% for column in dev_columns %}
                    {{ column }} AS dev_{{ column }} {% if not loop.last %},{% endif %}
                    {% endfor %}
                FROM {{ dev_database }}.{{ dev_schema }}.{{ dev_table }}
                WHERE {{ dev_freshness_key }} < CURRENT_DATE()
            ),

            prod AS (
                SELECT
                    {{ prod_pkey }} AS prod_primary_key,
                    {% for column in prod_columns %}
                    {{ column }} AS prod_{{ column }} {% if not loop.last %},{% endif %}
                    {% endfor %}
                FROM {{ prod_database }}.{{ prod_schema }}.{{ prod_table }}
                WHERE {{ prod_freshness_key }} < CURRENT_DATE()
            ),

            joined AS (
                SELECT
                    CASE 
                        WHEN a.dev_primary_key IS NOT NULL AND b.prod_primary_key IS NULL THEN 'addition'
                        WHEN a.dev_primary_key IS NULL AND b.prod_primary_key IS NOT NULL THEN 'deletion'
                    ELSE 'equal' END AS diff_type,
                    a.*,
                    b.*
                FROM dev a
                FULL OUTER JOIN prod b ON a.dev_primary_key = b.prod_primary_key
            )

            SELECT
                *
            FROM joined
            WHERE diff_type != 'equal'
            );
        """

        context = {
            "dev_database": self.dev_database,
            "dev_schema": self.dev_schema,
            "dev_table": self.dev_table,
            "dev_pkey": self.dev_pkey,
            "dev_freshness_key": self.dev_freshness_key,
            "dev_columns": dev_columns,
            "prod_database": self.prod_database,
            "prod_schema": self.prod_schema,
            "prod_table": self.prod_table,
            "prod_pkey": self.prod_pkey,
            "prod_freshness_key": self.prod_freshness_key,
            "prod_columns": prod_columns,
        }

        environment = jinja2.Environment()
        template = environment.from_string(sql)
        sql_formatted = template.render(context)
        print(sql_formatted)
        self.sf_ctx.execute_sql(sql_formatted)
