import atexit
import logging
from importlib.resources import files
from typing import Literal, NotRequired, Required, TypedDict, cast

import click
from flask import current_app, g
from flask.cli import with_appcontext

from lufa.awx import ApiAwxClient, AwxClient, NoneAwxClient
from lufa.database import DatabaseManager, PostgresDatabaseManager, SqliteDatabaseManager
from lufa.repository.api_repository import ApiRepository, PostgresApiRepository, SqliteApiRepository
from lufa.repository.backend_repository import BackendRepository, PostgresBackendRepository, SqliteBackendRepository
from lufa.repository.user_repository import PostgresUserRepository, SqliteUserRepository, UserRepository

logger = logging.getLogger(__name__)

# Reference to the current process's Postgres connection-pool manager,
# so the pool can be closed via atexit when the process ends.
_postgres_db_manager: PostgresDatabaseManager | None = None


class PostgresConfig(TypedDict, total=True):
    DB_TYPE: Literal["POSTGRES"]
    DB_HOST: str
    DB_DATABASE: str
    DB_USER: str
    DB_PASSWORD: str
    DB_MAX_POOL_CONNECTIONS: int
    DB_PORT: NotRequired[int]


class SqliteConfig(TypedDict, total=True):
    DB_TYPE: Literal["SQLITE"]
    DB_DATABASE: str


DbConfig = PostgresConfig | SqliteConfig


class AppConfig(TypedDict, total=True):
    """
    Represent the expected structure of config.py (and config.py.example).
    """

    DB_TYPE: Required[Literal["SQLITE"] | Literal["POSTGRES"]]
    DB_HOST: str
    DB_DATABASE: str
    DB_USER: str
    DB_PASSWORD: str
    DB_MAX_POOL_CONNECTIONS: int
    DB_PORT: int
    # Logging
    LOG_LEVEL: str
    LOG_FILE_PATH: str

    AUTH: Literal["LOCAL"]
    # only if auth is "LOCAL"
    AUTH_USER: str
    AUTH_PASSWORD: str
    # flask-ldap3-login
    LDAP_PORT: int
    LDAP_USE_SSL: bool
    LDAP_HOST: str
    LDAP_BASE_DN: str
    LDAP_GROUP_SEARCH_SCOPE: str
    LDAP_USER_LOGIN_ATTR: str
    LDAP_USER_SEARCH_SCOPE: str
    # login group in memberOf (is recursive)
    LDAP_USER_OBJECT_FILTER: str
    LDAP_BIND_USER_DN: str
    # AWX
    AWX_BASE_URL: str  # for linking to awx


def _create_database_manager(app_config: AppConfig) -> DatabaseManager:
    """
    Create the DatabaseManager. Will initialize the database if it is empty.

    Part of get_database_manager that doesn't require flask global state.
    Separated from get_database_manager for integration testing purpuses.
    """
    global _postgres_db_manager
    db_type = app_config["DB_TYPE"].upper()
    db_manager: DatabaseManager
    if db_type == "POSTGRES":
        init_script = str(files("lufa").joinpath("schema.sql"))
        db_manager = PostgresDatabaseManager(
            host=app_config["DB_HOST"],
            port=app_config.get("DB_PORT", 5432),
            database=app_config["DB_DATABASE"],
            user=app_config["DB_USER"],
            password=app_config["DB_PASSWORD"],
            init_script=init_script,  # makes result rows dicts
            max_pool_connections=app_config["DB_MAX_POOL_CONNECTIONS"],
        )
        _postgres_db_manager = db_manager
    elif db_type == "SQLITE":
        db_name = app_config["DB_DATABASE"]
        if "." not in db_name:
            db_name = db_name + ".db"
        init_script = str(files("lufa").joinpath("schema_sqlite.sql"))
        db_manager = SqliteDatabaseManager(db_name, init_script)
    else:
        raise ValueError(f"Unknown DB_TYPE '{db_type}'")
    if not db_manager.is_not_empty():
        logger.info("Database empty. Initializing.")
        db_manager.init_db()
    return db_manager


def get_database_manager():
    if "db_manager" not in g:
        g.db_manager = _create_database_manager(cast(AppConfig, current_app.config))
    return g.db_manager


def get_user_repository() -> UserRepository:
    if "user_repository" not in g:
        app_config: AppConfig = cast(AppConfig, current_app.config)
        if app_config["DB_TYPE"].upper() == "POSTGRES":
            g.user_repository = PostgresUserRepository(get_database_manager())
        elif app_config["DB_TYPE"].upper() == "SQLITE":
            g.user_repository = SqliteUserRepository(get_database_manager())
    return g.user_repository


def get_api_repository() -> ApiRepository:
    if "api_repository" not in g:
        if current_app.config["DB_TYPE"].upper() == "POSTGRES":
            g.api_repository = PostgresApiRepository(get_database_manager())
        elif current_app.config["DB_TYPE"].upper() == "SQLITE":
            g.api_repository = SqliteApiRepository(get_database_manager())
    return g.api_repository


def get_backend_repository() -> BackendRepository:
    if "backend_repository" not in g:
        if current_app.config["DB_TYPE"].upper() == "POSTGRES":
            g.backend_repository = PostgresBackendRepository(get_database_manager())
        elif current_app.config["DB_TYPE"].upper() == "SQLITE":
            g.backend_repository = SqliteBackendRepository(get_database_manager())
    return g.backend_repository


def get_awx_client() -> AwxClient:
    if "awx_client" not in g:
        if (
            current_app.config.get("AWX_BASE_URL")
            and current_app.config.get("AWX_API_TOKEN")
            and current_app.config.get("AWX_API_TOKEN") != ""
        ):
            ssl_verify = current_app.config.get("AWX_SSL_VERIFY", True)
            g.awx_client = ApiAwxClient(
                current_app.config["AWX_BASE_URL"], current_app.config["AWX_API_TOKEN"], ssl_verify
            )
        else:
            g.awx_client = NoneAwxClient()
    return g.awx_client


@click.command("init-db")  # add init-db command to cli
@with_appcontext
def init_db_command():
    """Clear the existing data and create new tables."""
    if click.confirm(init_db_command.__doc__ + " Continue?"):
        get_database_manager().init_db()
        click.echo("database initialized")
    else:
        click.echo("aborted")


@click.command("job-cleanup")
@with_appcontext
def job_cleanup_command():
    """Cleanup jobs older than 89 days."""
    get_api_repository().job_cleanup(89)
    click.echo("Jobs older than 89 days cleaned up.")


def close_db_conn(e=None):
    get_database_manager().close_db()


def _close_db_pools_on_exit():
    """Close the Postgres connection pool at process end (Postgres only)."""
    if _postgres_db_manager is not None:
        _postgres_db_manager.shutdown()


def init_app(app):
    app.teardown_appcontext(close_db_conn)  # close DB after request
    app.cli.add_command(init_db_command)
    app.cli.add_command(job_cleanup_command)
    atexit.register(_close_db_pools_on_exit)  # close db pool on process end
