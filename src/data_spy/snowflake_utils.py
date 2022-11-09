"Library for various utils around connecting to and executing queries against a snowflake database."

import logging

from decouple import config

import snowflake.connector
import pandas as pd

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
        return ctx.cursor()

    def execute_sql(self, sql: str):
        """Execute a sql comnmand."""

        cursor = self._connect()
        cur = cursor.execute(sql)
        return cur

    def sql_to_df(self, sql: str) -> pd.DataFrame:
        """Execute sql and return a pandas dataframe."""

        ctx = self._connect()
        logging.info(ctx)

        cur = ctx.execute(sql)
        df = cur.fetch_pandas_all()
        return df
