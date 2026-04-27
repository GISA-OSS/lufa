from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from importlib.resources import files
from typing import Callable, Iterable, Literal, NamedTuple, TypeAlias, TypedDict, cast

import pytest

from lufa.database import DatabaseManager, NumDays, PostgresDatabaseManager, SqliteDatabaseManager
from lufa.provider import DbConfig, PostgresConfig, SqliteConfig
from lufa.repository.api_repository import ApiRepository, PostgresApiRepository, SqliteApiRepository, TowerJobStats
from lufa.repository.backend_repository import BackendRepository, PostgresBackendRepository, SqliteBackendRepository
from lufa.repository.user_repository import PostgresUserRepository, SqliteUserRepository, UserRepository

SCOPE: Literal["function", "module"] = "function"  # Use "module" to keep DB-file on sqlite, like postgres does


def pytest_generate_tests(metafunc: pytest.Metafunc):
    for key in ["mark_db_backend", "mark_db_to_backend"]:
        if key not in metafunc.fixturenames:
            continue
        relevant_marks = [pytest.mark.sqlite3, pytest.mark.postgres]
        found_mark_names = [
            m.name for m in metafunc.definition.iter_markers() if m.name in [r.name for r in relevant_marks]
        ]
        metafunc.parametrize(
            key,
            (
                # test that not marked with supported db backends get all
                [pytest.param(r.name, marks=r) for r in relevant_marks]
                if len(found_mark_names) == 0
                # tests that have been explitily marked which db backends they support
                # (e.g. pytest.mark.postgres or pytest.mark.sqlite3) only get those backends.
                else found_mark_names
            ),
            scope=SCOPE,
        )
    num_stat_fixtures = len([k in metafunc.fixturenames for k in ["success_stat", "failed_stat", "any_stat"]])
    if num_stat_fixtures == 0:
        return

    empty = dict.fromkeys(["ok", "changed", "failed", "ignored", "rescued", "skipped", "unreachable"], 0)

    success_stats: list[StatsParam] = [
        StatsParam("only_ok", {"ok": 1, **empty}),
        StatsParam(
            "all_except_failed_unreachable_nonzero",
            {**{t: i + 1 for (i, t) in enumerate(empty.keys()) if t not in ["failed", "unreachable"]}, **empty},
        ),
        *(
            StatsParam(f"only_ok_{k}", {"ok": 1, k: 2, **empty})
            for k in empty.keys()
            if k not in ["ok", "failed", "unreachable"]
        ),
    ]
    failed_stats: list[StatsParam] = [
        StatsParam("all_stats_non_zero", {t: i + 1 for (i, t) in enumerate(empty.keys())}),
        StatsParam("only_failed", {"failed": 1, **empty}),
        StatsParam("only_skipped", {"skipped": 1, **empty}),
        StatsParam("only_unreachable", {"unreachable": 1, **empty}),
        StatsParam("all_empty", empty),
        *(StatsParam(f"failed+{p.title}", {"failed": 1, **p.param}) for p in success_stats),
        *(StatsParam(f"rescued+{p.title}", {"rescued": 1, **p.param}) for p in success_stats),
    ]
    if "success_stat" in metafunc.fixturenames:
        metafunc.parametrize(
            "success_stat",
            limit_length_if(num_stat_fixtures > 1, [pytest.param(p.param, id=p.title) for p in success_stats]),
        )
    if "failed_stat" in metafunc.fixturenames:
        metafunc.parametrize(
            "failed_stat",
            limit_length_if(num_stat_fixtures > 1, [pytest.param(p.param, id=p.title) for p in failed_stats]),
        )
    if "any_stat" in metafunc.fixturenames:
        metafunc.parametrize(
            "any_stat",
            limit_length_if(
                num_stat_fixtures > 1, [pytest.param(p.param, id=p.title) for p in success_stats + failed_stats]
            ),
        )

    if "single_any_stat" in metafunc.fixturenames:
        metafunc.parametrize("single_any_stat", [pytest.param(p.param, id=p.title) for p in success_stats[:1]])


@pytest.fixture(scope=SCOPE, name="db_config")
def db_config_fixture(mark_db_backend: str) -> Iterable[DbConfig]:
    yield from db_config(mark_db_backend)


def db_config(mark_db_backend: str) -> Iterable[DbConfig]:
    match mark_db_backend:
        case pytest.mark.sqlite3.name:
            db_tmp_file = tempfile.NamedTemporaryFile().name
            yield SqliteConfig(DB_TYPE="SQLITE", DB_DATABASE=db_tmp_file)
            try:
                os.remove(db_tmp_file)
            except FileNotFoundError:
                pass

        case pytest.mark.postgres.name:
            db_host = os.environ["POSTGRES_HOST"]
            db_database = os.environ["POSTGRES_DB"]
            db_user = os.environ["POSTGRES_USER"]
            db_password = os.environ["POSTGRES_PASSWORD"]
            assert db_database is None or "test" in db_database or db_user is None or "test" in db_user, (
                f'db_user or db_database must contain "test", to prevent running on production DB: {db_user}@{db_database}'
            )
            yield PostgresConfig(
                DB_TYPE="POSTGRES",
                DB_HOST=db_host,
                DB_DATABASE=db_database,
                DB_USER=db_user,
                DB_PASSWORD=db_password,
            )

        case _:
            raise NotImplementedError(f"Unknown DB backend marker: pytest.mark.{mark_db_backend}")


@pytest.fixture(name="db_manager")
def db_manager_fixture(empty_db: DatabaseManager) -> DatabaseManager:
    return db_manager(empty_db)


def db_manager(empty_db: DatabaseManager) -> DatabaseManager:
    empty_db.init_db()
    return empty_db


@pytest.fixture(name="empty_db")
def empty_db_fixture(mark_db_backend: str, db_config: DbConfig) -> Iterable[DatabaseManager]:
    yield from empty_db(mark_db_backend, db_config)


def empty_db(mark_db_backend: str, db_config: DbConfig) -> Iterable[DatabaseManager]:
    match mark_db_backend:
        case pytest.mark.sqlite3.name:
            sqlite = SqliteDatabaseManager(db_config["DB_DATABASE"], str((files("lufa").joinpath("schema_sqlite.sql"))))

            with open(
                str(files("tests").joinpath("integration").joinpath("lufa").joinpath("drop_tables_sqlite.sql")), "r"
            ) as f:
                sqlite.get_db_connection().cursor().executescript(f.read())

            yield sqlite

            sqlite.close_db()
        case pytest.mark.postgres.name:
            postgres_config = cast(PostgresConfig, db_config)
            postgres = PostgresDatabaseManager(
                host=postgres_config["DB_HOST"],
                user=postgres_config["DB_USER"],
                database=postgres_config["DB_DATABASE"],
                password=postgres_config["DB_PASSWORD"],
                init_script=str(files("lufa").joinpath("schema.sql")),
            )
            with open(
                str(files("tests").joinpath("integration").joinpath("lufa").joinpath("drop_tables.sql")), "r"
            ) as f:
                postgres.get_db_connection().cursor().execute(f.read())
            yield postgres

            postgres.close_db()


@pytest.fixture(name="api_repository")
def api_repository_fixture(mark_db_backend: str, db_manager: DatabaseManager) -> ApiRepository:
    return api_repository(mark_db_backend, db_manager)


def api_repository(mark_db_backend: str, db_manager: DatabaseManager) -> ApiRepository:
    if mark_db_backend == pytest.mark.sqlite3.name:
        return SqliteApiRepository(db_manager)
    if mark_db_backend == pytest.mark.postgres.name:
        return PostgresApiRepository(db_manager)
    raise NotImplementedError(f"Unknown DB backend marker: pytest.mark.{mark_db_backend}")


ApiRepositoryToBackend: TypeAlias = Callable[[], Iterable[ApiRepository]]


@pytest.fixture
def api_repository_to_backend(mark_db_to_backend: str) -> Iterable[ApiRepositoryToBackend]:
    for config in db_config(mark_db_to_backend):
        yield partial(reset_api_repository, mark_db_to_backend, config)


def reset_api_repository(mark_db_to_backend: str, config: DbConfig) -> Iterable[ApiRepository]:
    # to be able to export then import to same Postgres DB
    for empty in empty_db(mark_db_to_backend, config):
        yield api_repository(mark_db_to_backend, db_manager(empty))


@pytest.fixture
def user_repository(mark_db_backend: str, db_manager: DatabaseManager) -> UserRepository:
    if mark_db_backend == pytest.mark.sqlite3.name:
        return SqliteUserRepository(db_manager)
    if mark_db_backend == pytest.mark.postgres.name:
        return PostgresUserRepository(db_manager)
    raise NotImplementedError(f"Unknown DB backend marker: pytest.mark.{mark_db_backend}")


@pytest.fixture
def backend_repository(mark_db_backend: str, db_manager: DatabaseManager) -> BackendRepository:
    if mark_db_backend == pytest.mark.sqlite3.name:
        return SqliteBackendRepository(db_manager)
    if mark_db_backend == pytest.mark.postgres.name:
        return PostgresBackendRepository(db_manager)
    raise NotImplementedError(f"Unknown DB backend marker: pytest.mark.{mark_db_backend}")


class HostIntependantTowerJobStats(TypedDict):
    ok: int
    failed: int
    unreachable: int
    changed: int
    skipped: int
    rescued: int
    ignored: int


@dataclass
class WithJobFactory:
    tower_job_id: int
    _lufa_factory: LufaFactory

    def with_stats(self, ansible_host: str, tower_job_stats: HostIntependantTowerJobStats):
        with_ansible_host = [cast(TowerJobStats, {"ansible_host": ansible_host, **tower_job_stats})]
        self._lufa_factory.api_repository.add_stats(self.tower_job_id, with_ansible_host)
        return self

    def with_end_time_days_ago(self, end: NumDays):
        return self.with_end_time(timedelta(days=end))

    def with_end_time(self, ago: timedelta = timedelta()):
        self._lufa_factory.api_repository.update_job(self.tower_job_id, (datetime.now() - ago).isoformat())
        return self


@dataclass
class WithTowerJobTemplate:
    tower_job_template_id: int
    _lufa_factory: LufaFactory

    @property
    def tower_job_template_name(self) -> str:
        return f"TowerJobTemplate{self.tower_job_template_id}"

    @property
    def awx_organisation(self) -> str:
        return f"Organisation{self.tower_job_template_id}"

    @property
    def playbook_path(self) -> str:
        return f"playbook{self.tower_job_template_id}.yml"

    def add_job(self, started_ago: timedelta = timedelta()) -> WithJobFactory:
        return self.add_job_with_compliance_interval(0, started_ago)

    def add_job_with_compliance_interval(
        self, compliance_interval: int, started_ago: timedelta = timedelta()
    ) -> WithJobFactory:
        tower_job_id = self._lufa_factory.new_tower_job_id()
        start_time = (datetime.now() - started_ago).isoformat()
        irelevant_parameters = {
            "playbook_path": self.playbook_path,
            "tower_job_template_name": self.tower_job_template_name,
            "awx_organisation": self.awx_organisation,
            "ansible_limit": f"AnsibleLimit{tower_job_id}",
            "tower_user_name": f"TowerUserName{tower_job_id}",
            "awx_tags": ["tag"],
            "extra_vars": "{}",
            "artifacts": "{}",
            "tower_schedule_id": 70000 + tower_job_id,
            "tower_schedule_name": f"TowerScheduleName{tower_job_id}",
            "tower_workflow_job_id": 80000 + tower_job_id,
            "tower_workflow_job_name": f"TowerWorkflowJobName{tower_job_id}",
        }
        self._lufa_factory.api_repository.add_job(
            tower_job_id,
            self.tower_job_template_id,
            compliance_interval=compliance_interval,
            start_time=start_time,
            **irelevant_parameters,
        )
        return WithJobFactory(tower_job_id, self._lufa_factory)


@dataclass
class LufaFactory:
    _next_tower_job_template_id: int
    _next_tower_job_id: int

    api_repository: ApiRepository

    def new_tower_job_id(self):
        ret = self._next_tower_job_id
        self._next_tower_job_id += 11
        return ret

    def new_tower_job_template_id(self):
        ret = self._next_tower_job_template_id
        self._next_tower_job_template_id += 11
        return ret

    def add_tower_template(self) -> WithTowerJobTemplate:
        return WithTowerJobTemplate(self.new_tower_job_template_id(), self)


@pytest.fixture
def lufa_factory(api_repository: ApiRepository) -> LufaFactory:
    return LufaFactory(100, 2000, api_repository)


class StatsParam(NamedTuple):
    title: str
    param: dict


def limit_length_if[T](limit: bool, array: list[T]):
    return array[:1] if limit else array
