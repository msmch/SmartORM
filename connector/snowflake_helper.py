import logging
import os
import pandas as pd

from snowflake.snowpark import Session


class SnowflakeConnectorError(Exception):
    """Custom exception for Snowflake connector."""
    pass


def generate_snowflake_config(use_sso: bool, user: str|None) -> dict:
    """
    Function generates a config dictionary based on the env variables
    
    It allows developer to use Single Sign On via Web browser, especially in case
    when Snowflake is hosted by the client who doesn't want to share technical account
    credentials.

    For SSO purpose developer needs to provide user name that will match the name used for
    authenticating access by SSO provider.
    """
    if use_sso and user is None:
        raise SnowflakeConnectorError("Argument user must be provided when use_sso flag is set to True.")
    
    snowflake_config = {
        "account": os.getenv("SNFL_ACCOUNT"),
        "user": user or os.getenv("SNFL_USER"),
        "password": None if use_sso else os.getenv("SNFL_PASSWORD"),
        "database": os.getenv("SNFL_DATABASE"),
        "schema": os.getenv("SNFL_SCHEMA"),
        "warehouse": os.getenv("SNFL_WAREHOUSE"),
        "role": os.getenv("SNFL_ROLE"),
    }
    return snowflake_config

def open_snowpark_session(use_sso: bool = False, user: str|None = None) -> Session:
    snowflake_config = generate_snowflake_config(use_sso, user)
    if use_sso:
        snowflake_config.pop("password")
        snowflake_config["authenticator"] = "externalbrowser"
    return Session.builder.configs(snowflake_config).create()


class SnowparkConnector:
    def __init__(self, use_sso: bool = False, user: str|None = None):
        self.logger = logging.getLogger(__name__)
        self.session = open_snowpark_session(use_sso, user)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()

    def bulk_insert(self, df: pd.DataFrame, table: str) -> None:
        """ SQLAlchemy do not support Cursor.copy_from() method for Snowflake
        and this is why we need this method"
        """
        self.logger.debug("Running bulk insert using Snowpark.")
        df = df.where(pd.notnull(df), None)
        df.columns = [col.upper() for col in df.columns]
        snowpark_df = self.session.create_dataframe(df)
        snowpark_df.write.mode("overwrite").save_as_table(table)