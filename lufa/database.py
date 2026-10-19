import sqlite3
import threading
from abc import ABC, abstractmethod
from typing import Any, Literal, NotRequired, Optional, TypeAlias, TypedDict

from psycopg2.extras import RealDictCursor
from psycopg2.pool import PoolError, ThreadedConnectionPool

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
        conn = self.get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT current_timestamp as now;")
            return cur.fetchone()["now"].strftime(r"%Y-%m-%d %H:%M")
        finally:
            self._release(conn)

    def __init__(self, host: str, database: str, user: str, password: str, init_script: str, max_pool_connections: int):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.init_script = init_script
        self.max_pool_connections = max_pool_connections

        self._pool: ThreadedConnectionPool | None = None
        self._pool_lock = threading.Lock()

        # A thread holds exactly one checked-out pool connection,
        # so the repositories can keep their "get_db_connection()" pattern.
        self._local = threading.local()

    def _get_pool(self) -> ThreadedConnectionPool:
        if self._pool is None:
            with self._pool_lock:
                if self._pool is None:
                    self._pool = ThreadedConnectionPool(
                        minconn=1,
                        maxconn=self.max_pool_connections,
                        host=self.host,
                        database=self.database,
                        user=self.user,
                        password=self.password,
                        cursor_factory=RealDictCursor,  # makes result rows dicts
                    )

        return self._pool

    def _release(self, conn: Any):
        """Return a connection to the pool and clear the thread's handle."""
        if getattr(self._local, "conn", None) is conn:
            self._local.conn = None
        self._get_pool().putconn(conn)

    def is_not_empty(self) -> bool:
        conn = self.get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM information_schema.tables WHERE table_schema='public';")
            rows = cur.fetchall().__len__()
            return rows > 0  # no tables
        finally:
            self._release(conn)

    def init_db(self) -> None:
        conn = self.get_db_connection()
        try:
            with open(self.init_script, "r") as f:
                conn.cursor().execute(f.read())
                conn.commit()
        finally:
            self._release(conn)

    def get_db_connection(self) -> Any:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._get_pool().getconn()
            self._local.conn = conn
        return conn

    def close_db(self) -> None:
        # return the connection to the pool (do not hard-close it)
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                self._release(conn)
            except PoolError:
                pass  # pool may already be closed

    def shutdown(self) -> None:
        """Close all pooled connections (e.g. at process end)."""
        with self._pool_lock:
            if self._pool is not None:
                self._pool.closeall()
                self._pool = None
