import os
from logging import getLogger

from sqlalchemy import MetaData, Engine, create_engine 
from sqlalchemy.orm import sessionmaker


class PostgresConnectorError(Exception):
    """Custom exception for Postgres connector."""
    pass

def generate_postgres_config(user: str | None = None) -> dict:
    postgres_config = {
        "user": user or os.getenv("PG_USER"),
        "password": os.getenv("PG_PWD"),
        "host": os.getenv("PG_HOST", "localhost"),
        "port": os.getenv("PG_PORT", "5432"),
        "database": os.getenv("PG_DATABASE", "core_db"),
    }
    return postgres_config

def build_pg_engine() -> Engine:
    pg_config = generate_postgres_config()
    engine = create_engine(
        "postgresql+psycopg2://", 
        connect_args={
            "user": pg_config["user"],
            "password": pg_config["password"],
            "host": pg_config["host"],
            "port": pg_config["port"],
            "database": pg_config["database"],
        }
    )
    return engine


class PostgresConnector:
    def __init__(self, use_sso: bool = False, user: str | None = None) -> None:
        if use_sso:
            raise NotImplementedError("Single Sign On is currently implemented for Snowflake connector only.")
        self.logger = getLogger(self.__class__.__name__)
        self.engine = build_pg_engine()
        self.Session = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        self.session = self.Session()
        self.metadata = MetaData()
        self.metadata.bind = self.engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()

