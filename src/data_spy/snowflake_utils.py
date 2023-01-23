"""Library for various utils around connecting to and executing queries against a snowflake database."""

import logging

from decouple import config

import pandas as pd

import snowflake.connector

from sqlalchemy import create_engine

from snowflake.connector.pandas_tools import pd_writer

logging.basicConfig(level=logging.INFO)

user = config("SNOWFLAKE_USER")
password = config("SNOWFLAKE_PASSWORD")
account = config("SNOWFLAKE_ACCOUNT")
role = config("SNOWFLAKE_ROLE")
warehouse = config("SNOWFLAKE_WAREHOUSE")


class snowflake_ctx:
    """Class for connecting to snowflake account and executing SQL."""

    def __init__(self):
        """Initialize with snowflake params."""
        self.user = user
        self.password = password
        self.account = account
        self.role = role
        self.warehouse = warehouse

    def _connect(self):
        """Connect to Snowflake, return cursor."""
        ctx = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            role=self.role,
            warehouse=self.warehouse,
        )

        logging.info("Connected Successfully!")
        return ctx

    def execute_sql(self, sql: str):
        """Execute a sql comnmand."""

        cursor = self._connect().cursor()
        cur = cursor.execute(sql)
        return cur

    def execute_sql_and_return_single_value(self, sql: str) -> str:
        """Executes a sql command that returns a single value...returns value.

        Args:
            sql: sql to execute

        Returns:
            str: sigle value returned as a string.
        """

        cursor = self._connect().cursor()
        cur = cursor.execute(sql)

        return str(cur.fetchone()[0])

    def sql_to_df(self, sql: str) -> pd.DataFrame:
        """Execute sql and return a pandas dataframe."""

        ctx = self._connect().cursor()
        logging.info(ctx)

        cur = ctx.execute(sql)
        df = cur.fetch_pandas_all()
        return df

    def write_df_to_snowflake(
        self,
        df: pd.DataFrame,
        database: str,
        schema: str,
        table_name: str,
    ) -> None:
        """Writes a pandas dataframe to snowflake.

        Uses the sqlalchemy package because the snowflake-connector's write_pandas requires
        the table already exist...which is a hindrance.

        Args:
            df: pandas dataframe to be written to snowflake.
            database: name of database to write to.
            schema: name of schema to write to.
            table_name: table name to write df to.

        Returns:
            None
        """

        account = self.account
        user = self.user
        password = self.password
        database = database
        schema = schema

        conn_string = f"snowflake://{user}:{password}@{account}/{database}/{schema}"

        engine = create_engine(conn_string)
        con = engine.connect()

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
            method=pd_writer,
        )

        con.close()
        engine.dispose()
