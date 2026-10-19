import os
import tempfile
from typing import Iterable

import pytest
from flask.testing import FlaskClient

from lufa import create_app, provider

API_DEV_KEY = "1234"
API_RO_KEY = "6789"
AUTH_USER = "admin"
AUTH_PASS = "pass"

SECRET_KEY = "geheim"
TEST_ROOT_REL_PATH = "../.."


# Sonst werden die init-Skripte nicht gefunden
@pytest.fixture
def change_test_dir(request, monkeypatch):
    monkeypatch.chdir(TEST_ROOT_REL_PATH)


@pytest.fixture
def app_sqlite(request):
    db_tmp_file = tempfile.NamedTemporaryFile().name

    app = create_app(
        {
            "TESTING": True,
            "WTF_CSRF_ENABLED": False,
            "DB_TYPE": "SQLITE",
            "DB_DATABASE": db_tmp_file,
            "AUTH": "LOCAL",
            "AUTH_USER": AUTH_USER,
            "AUTH_PASSWORD": AUTH_PASS,
            "API_KEYS": [API_DEV_KEY],
            "API_KEYS_RO": [API_RO_KEY],
            "SECRET_KEY": SECRET_KEY,
        }
    )

    os.chdir(TEST_ROOT_REL_PATH)
    with app.app_context():
        provider.get_database_manager().init_db()

    os.chdir(request.config.invocation_params.dir)

    yield app

    try:
        os.remove(db_tmp_file)
    except FileNotFoundError:
        pass


@pytest.fixture
def app_postgres(request):
    app = create_app(
        {
            "TESTING": True,
            "WTF_CSRF_ENABLED": False,
            "DB_TYPE": "POSTGRES",
            "DB_HOST": os.environ.get("POSTGRES_HOST"),
            "DB_DATABASE": os.environ.get("POSTGRES_DB"),
            "DB_USER": os.environ.get("POSTGRES_USER"),
            "DB_PASSWORD": os.environ.get("POSTGRES_PASSWORD"),
            "DB_MAX_POOL_CONNECTIONS": 4,
            "AUTH_USER": AUTH_USER,
            "AUTH_PASSWORD": AUTH_PASS,
            "AUTH": "LOCAL",
            "API_KEYS": [API_DEV_KEY],
            "API_KEYS_RO": [API_RO_KEY],
            "SECRET_KEY": SECRET_KEY,
        }
    )

    os.chdir(TEST_ROOT_REL_PATH)
    with app.app_context():
        provider.get_database_manager().init_db()

    os.chdir(request.config.invocation_params.dir)

    yield app


@pytest.fixture(
    params=[pytest.param("sqlite", marks=pytest.mark.sqlite3), pytest.param("postgres", marks=pytest.mark.postgres)]
)
def app(request):
    if request.param == "sqlite":
        return request.getfixturevalue("app_sqlite")
    else:
        return request.getfixturevalue("app_postgres")


@pytest.fixture
def client(app) -> Iterable[FlaskClient]:
    app_client = app.test_client()

    # Login to get /data Endpoints
    app_client.post("/login?next=/", data={"username": AUTH_USER, "password": AUTH_PASS})

    yield app_client

    app_client.get("/logout")
