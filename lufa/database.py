import sqlite3
from abc import ABC, abstractmethod
from typing import Any, Literal, NotRequired, Optional, TypeAlias, TypedDict

import psycopg2
from psycopg2.extras import RealDictCursor

JobState = Literal["Not found", "error", "ok", "failed", "started"]
JSon: TypeAlias = str
TimeStamp: TypeAlias = str
NumDays: TypeAlias = int
Path: TypeAlias = str


class NewJob(TypedDict):
    tower_job_id: int
    tower_job_template_id: int
    tower_job_template_name: str
    ansible_limit: str
    tower_user_name: str
    awx_tags: list[str]
    extra_vars: str
    artifacts: JSon
    tower_schedule_id: NotRequired[Optional[int]]
    tower_schedule_name: NotRequired[Optional[str]]
    tower_workflow_job_id: NotRequired[Optional[int]]
    tower_workflow_job_name: NotRequired[Optional[str]]
    compliance_interval: NumDays
    template_infos: NotRequired[Optional[str]]
    playbook_path: NotRequired[Optional[Path]]
    awx_organisation: NotRequired[Optional[str]]
    start_time: NotRequired[Optional[TimeStamp]]


class Job(NewJob):
    state: JobState
    end_time: NotRequired[Optional[TimeStamp]]


class DatabaseManager(ABC):
    @abstractmethod
    def is_not_empty(self) -> bool:
        """Checks if is initialised."""
        pass

    @abstractmethod
    def init_db(self) -> None:
        """Initialize the database."""
        pass

    @abstractmethod
    def get_db_connection(self) -> Any:
        pass

    @abstractmethod
    def close_db(self) -> None:
        """Closes the db connection."""
        pass

    @abstractmethod
    def get_db_now(self) -> str:
        """Get current timestamp from the database."""
        pass


class SqliteDatabaseManager(DatabaseManager):
    def __init__(self, db_path: str, init_script: str):
        self.db_path = db_path
        self.init_script = init_script
        self._conn: sqlite3.Connection | None = None

    def is_not_empty(self) -> bool:
        cur = self.get_db_connection().cursor()
        cur.execute("select * from dbstat;")
        rows = cur.fetchall().__len__()
        return rows > 0  # no tables

    def close_db(self) -> None:
        self.get_db_connection().close()
        self._conn = None

    def get_db_connection(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = dict_factory
        return self._conn

    def init_db(self) -> None:
        cur = self.get_db_connection().cursor()
        with open(self.init_script, "r") as sql_file:
            sql = sql_file.read()
            cur.executescript(sql)
            self.get_db_connection().commit()

    def get_db_now(self) -> str:
        cur = self.get_db_connection().cursor()
        cur.execute("select strftime('%Y-%m-%d %H:%M', datetime('now')) as now;")
        return cur.fetchone()["now"]


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class PostgresDatabaseManager(DatabaseManager):
    def get_db_now(self) -> str:
        with self.get_db_connection() as db_conn:
            cur = db_conn.cursor()
            cur.execute("SELECT current_timestamp as now;")
            return cur.fetchone()["now"].strftime(r"%Y-%m-%d %H:%M")

    def __init__(self, host: str, database: str, user: str, password: str, init_script: str):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.init_script = init_script
        self._conn: Any | None = None

    def is_not_empty(self) -> bool:
        with self.get_db_connection() as db_conn:
            cur = db_conn.cursor()
            cur.execute("SELECT * FROM information_schema.tables WHERE table_schema='public';")
            rows = cur.fetchall().__len__()
            return rows > 0  # no tables

    def init_db(self) -> None:
        with self.get_db_connection() as conn:
            with open(self.init_script, "r") as f:
                conn.cursor().execute(f.read())

    def get_db_connection(self) -> Any:
        if self._conn is None:
            self._conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor,  # makes result rows dicts
            )
        return self._conn

    def close_db(self) -> None:
        self.get_db_connection().close()
        self._conn = None
