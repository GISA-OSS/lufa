import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from typing import Optional, TypeAlias, TypedDict, cast

from psycopg2.errors import ForeignKeyViolation, InvalidDatetimeFormat, InvalidTextRepresentation

from lufa.database import DatabaseManager, JobState, JSon, TimeStamp
from lufa.repository.backend_repository import ResourceNotFoundError

logger = logging.getLogger(__name__)


UnixTimeStamp: TypeAlias = int


class LufaKeyError(KeyError):
    def __init__(self, key, value):
        super().__init__("{} is not a known {}", value, key)

    @property
    def msg(self) -> str:
        return self.args[0].format(self.args[1], self.args[2])


class Callback(TypedDict):
    task_ansible_uuid: str
    ansible_host: str
    state: JobState
    module: str
    result_dump: str


class CallbackExport(Callback):
    timestamp: TimeStamp


class Task(TypedDict):
    ansible_uuid: str
    tower_job_id: int
    task_name: str


class TaskExport(TypedDict):
    ansible_uuid: str
    task_name: str
    callbacks: list[CallbackExport]


class TowerJobStats(TypedDict):
    ansible_host: str
    ok: int
    failed: int
    unreachable: int
    changed: int
    skipped: int
    rescued: int
    ignored: int


class JobTemplateComplianceStates(TypedDict):
    last_successful: UnixTimeStamp
    last_job_run: int
    compliance_interval_in_days: int
    playbook: str
    template_name: str
    tower_job_template_id: int
    organisation: str


class FullJob(TypedDict):
    tower_job_id: int
    tower_job_template_id: int
    tower_job_template_name: str
    ansible_limit: str
    tower_user_name: str
    awx_tags: list[str]
    extra_vars: JSon
    artifacts: JSon
    tower_schedule_id: int
    tower_schedule_name: str
    tower_workflow_job_id: int
    tower_workflow_job_name: str
    start_time: TimeStamp
    end_time: TimeStamp | None
    state: JobState


class TowerJobTemplate(TypedDict):
    tower_job_template_id: int
    tower_job_template_name: str
    playbook_path: str
    compliance_interval: int
    awx_organisation: str
    template_infos: str | None


class JobExport(TypedDict):
    exported_at: TimeStamp
    job: FullJob
    job_template: TowerJobTemplate
    stats: list[TowerJobStats]
    tasks: list[TaskExport]


class ApiRepository(ABC):
    @abstractmethod
    def get_all_noncompliant_hosts(self) -> dict[str, list[JobTemplateComplianceStates]]:
        """ "
        json structure:

        { hostname_fqdm: [{
            'last_successful': unix_timestamp ? 0
            'last_job_run': int_job_id
            'compliance_interval_in_days': int_num_days
            'playbook': str
            'template_name': str
            'tower_job_template_id': int
            'oranisation': str_tower
        }]}
        """
        pass

    @abstractmethod
    def update_job(
        self, tower_job_id: int, end_time: Optional[TimeStamp] = None, artifacts: Optional[JSon] = None
    ) -> None:
        pass

    @abstractmethod
    def job_exists(self, tower_job_id: int) -> bool:
        """Checks if a job exists."""
        pass

    @abstractmethod
    def add_job(
        self,
        tower_job_id: int,
        tower_job_template_id: int,
        tower_job_template_name: str,
        awx_tags: list[str],
        extra_vars: JSon,
        artifacts: JSon,
        ansible_limit: Optional[str] = None,
        tower_user_name: Optional[str] = None,
        tower_schedule_id: Optional[int] = None,
        tower_schedule_name: Optional[str] = None,
        tower_workflow_job_id: Optional[int] = None,
        tower_workflow_job_name: Optional[str] = None,
        compliance_interval: int = 0,
        template_infos: Optional[str] = None,
        playbook_path: Optional[str] = None,
        awx_organisation: Optional[str] = None,
        start_time: Optional[TimeStamp] = None,
    ) -> None:
        pass

    @abstractmethod
    def add_callback(
        self,
        task_ansible_uuid: str,
        ansible_host: str,
        state: JobState,
        result_dump: JSon,
        module: str,
        timestamp: Optional[str] = None,
    ) -> None:
        pass

    @abstractmethod
    def tasks_exists(self, task_uuid: str) -> bool:
        pass

    @abstractmethod
    def add_task(self, ansible_uuid: str, tower_job_id: int, task_name: str) -> None:
        pass

    @abstractmethod
    def add_stats(self, tower_job_id: int, stats: list[TowerJobStats]) -> None:
        pass

    @abstractmethod
    def export_job(self, tower_job_id: int) -> JobExport:
        """Exports complete job data with tasks and callbacks"""
        pass

    @abstractmethod
    def import_job(self, export_data: JobExport) -> int:
        """Imports a job from a dict
        Returns the tower_job_id of the imported job.
        """
        pass


class SqliteApiRepository(ApiRepository):
    def __init__(self, db_manager: DatabaseManager) -> None:
        self.db_manager = db_manager
        self.callbacks: list = []

    def get_all_noncompliant_hosts(self) -> dict[str, list[JobTemplateComplianceStates]]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
                    SELECT
                        ansible_host,
                        noncompliant
                    FROM v_host_noncompliance
                """)
        ret: dict[str, list[JobTemplateComplianceStates]] = {}
        for line in cursor.fetchall():
            ret[line["ansible_host"]] = json.loads(line["noncompliant"])
        return ret

    def add_stats(self, tower_job_id: int, stats: list[TowerJobStats]) -> None:
        conn: sqlite3.Connection = self.db_manager.get_db_connection()
        cursor = conn.cursor()

        for stat in stats:
            cursor.execute(
                """
                            INSERT INTO stats (
                                tower_job_id, 
                                ansible_host, 
                                ok, 
                                failed, 
                                unreachable, 
                                changed,
                                skipped,
                                rescued,
                                ignored
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT DO NOTHING
                        """,
                (
                    tower_job_id,
                    stat["ansible_host"],
                    stat["ok"],
                    stat["failed"],
                    stat["unreachable"],
                    stat["changed"],
                    stat["skipped"],
                    stat["rescued"],
                    stat["ignored"],
                ),
            )

        conn.commit()

    def add_callback(
        self,
        task_ansible_uuid: str,
        ansible_host: str,
        state: JobState,
        result_dump: JSon,
        module: str,
        timestamp: Optional[str] = None,
    ) -> None:
        conn: sqlite3.Connection = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                            INSERT INTO task_callbacks (task_ansible_uuid, ansible_host, state, module, result_dump)
                            VALUES (?, ?, ?, ?, ?)
                        """,
            (
                task_ansible_uuid,
                ansible_host,
                state,
                module,
                result_dump,
            ),
        )
        # set timestamp if given and not None
        if timestamp is not None:
            cursor.execute(
                """
                                UPDATE task_callbacks
                                SET timestamp = ?
                                WHERE task_ansible_uuid = ?
                            """,
                (timestamp, task_ansible_uuid),
            )

        conn.commit()

    def job_exists(self, tower_job_id: int) -> bool:
        conn: sqlite3.Connection = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT *
                        FROM jobs
                        WHERE tower_job_id = ?
                        """,
            (tower_job_id,),
        )

        return cursor.fetchone() is not None

    def add_job(
        self,
        tower_job_id: int,
        tower_job_template_id: int,
        tower_job_template_name: str,
        awx_tags: list[str],
        extra_vars: JSon,
        artifacts: JSon,
        ansible_limit: Optional[str] = None,
        tower_user_name: Optional[str] = None,
        tower_schedule_id: Optional[int] = None,
        tower_schedule_name: Optional[str] = None,
        tower_workflow_job_id: Optional[int] = None,
        tower_workflow_job_name: Optional[str] = None,
        compliance_interval: int = 0,
        template_infos: Optional[str] = None,
        playbook_path: Optional[str] = None,
        awx_organisation: Optional[str] = None,
        start_time: Optional[TimeStamp] = None,
    ) -> None:
        conn: sqlite3.Connection = self.db_manager.get_db_connection()
        cursor = conn.cursor()

        # upsert job_template
        cursor.execute(
            """
                        INSERT INTO job_templates
                            (tower_job_template_id, tower_job_template_name, template_infos, playbook_path, awx_organisation, compliance_interval)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT (tower_job_template_id) DO UPDATE
                        SET (tower_job_template_name, template_infos, playbook_path, awx_organisation, compliance_interval) = (?, ?, ?, ?, ?)
                        """,
            (
                tower_job_template_id,
                tower_job_template_name,
                template_infos,
                playbook_path,
                awx_organisation,
                compliance_interval,
                tower_job_template_name,
                template_infos,
                playbook_path,
                awx_organisation,
                compliance_interval,
            ),
        )

        # insert job
        cursor.execute(
            """
                        INSERT INTO jobs (
                            tower_job_id,
                            tower_job_template_id,
                            ansible_limit,
                            tower_user_name,
                            awx_tags,
                            extra_vars,
                            artifacts,
                            tower_schedule_id,
                            tower_schedule_name,
                            tower_workflow_job_id,
                            tower_workflow_job_name
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
            (
                tower_job_id,
                tower_job_template_id,
                ansible_limit,
                tower_user_name,
                ",".join(awx_tags),
                extra_vars,
                artifacts,
                tower_schedule_id,
                tower_schedule_name,
                tower_workflow_job_id,
                tower_workflow_job_name,
            ),
        )
        # change start time if given and not None
        if start_time is not None:
            cursor.execute(
                """UPDATE jobs
                        SET start_time = ?
                    WHERE tower_job_id = ?
                """,
                (start_time, tower_job_id),
            )

        conn.commit()

    def add_task(self, ansible_uuid: str, tower_job_id: int, task_name: str) -> None:
        conn: sqlite3.Connection = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                            INSERT INTO tasks (ansible_uuid, tower_job_id, task_name)
                            VALUES (?, ?, ?)
                        """,
            (ansible_uuid, tower_job_id, task_name),
        )
        conn.commit()

    def tasks_exists(self, task_uuid: str) -> bool:
        conn: sqlite3.Connection = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT *
                    FROM tasks
                    WHERE ansible_uuid = ?
                """,
            (task_uuid,),
        )
        return cursor.fetchone() is not None

    def update_job(
        self, tower_job_id: int, end_time: Optional[TimeStamp] = None, artifacts: Optional[JSon] = None
    ) -> None:
        conn: sqlite3.Connection = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    UPDATE jobs
                    SET
                        end_time = COALESCE(?, datetime('now','localtime')),
                        artifacts = COALESCE(?, '{}')
                    WHERE tower_job_id = ?
                    """,
            (
                end_time,
                artifacts,
                tower_job_id,
            ),
        )

        conn.commit()

    def export_job(self, tower_job_id: int) -> JobExport:
        """Exports complete job data with tasks and callbacks"""
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()

        # get job data with template info
        cursor.execute(
            """
            SELECT v_job_status.*,
                   job_templates.tower_job_template_name,
                   job_templates.playbook_path,
                   job_templates.compliance_interval,
                   job_templates.awx_organisation,
                   job_templates.template_infos,
                   strftime('%Y-%m-%dT%H:%M:%f', v_job_status.start_time) as start_time,
                   strftime('%Y-%m-%dT%H:%M:%f', v_job_status.end_time)   as end_time
            FROM v_job_status
                     JOIN job_templates
                          ON v_job_status.tower_job_template_id = job_templates.tower_job_template_id
            WHERE v_job_status.tower_job_id = ?
            """,
            (tower_job_id,),
        )

        job = cursor.fetchone()
        if not job:
            raise ResourceNotFoundError(f"Job with id {tower_job_id} not found")

        awx_tags = job["awx_tags"].split(",") if job["awx_tags"] else []
        template_infos = json.loads(job["template_infos"]) if job["template_infos"] else None

        # get stats
        cursor.execute(
            """
            SELECT ansible_host,
                   ok,
                   failed,
                   unreachable,
                   changed,
                   skipped,
                   rescued,
                   ignored
            FROM stats
            WHERE tower_job_id = ?
            ORDER BY ansible_host
            """,
            (tower_job_id,),
        )
        stats = cursor.fetchall()

        # get tasks with their callbacks
        # SQLite doesn't have json_agg, so we need to fetch and group manually
        cursor.execute(
            """
            SELECT tasks.ansible_uuid,
                   tasks.task_name,
                   task_callbacks.ansible_host,
                   task_callbacks.state,
                   task_callbacks.module,
                   strftime('%Y-%m-%dT%H:%M:%f', task_callbacks.timestamp) as timestamp,
                   task_callbacks.result_dump
            FROM tasks
                     LEFT JOIN task_callbacks
                               ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
            WHERE tasks.tower_job_id = ?
            ORDER BY tasks.task_name, task_callbacks.timestamp
            """,
            (tower_job_id,),
        )

        # group callbacks by task
        tasks_dict = {}
        for row in cursor.fetchall():
            task_uuid = row["ansible_uuid"]

            if task_uuid not in tasks_dict:
                tasks_dict[task_uuid] = {
                    "ansible_uuid": task_uuid,
                    "task_name": row["task_name"],
                    "callbacks": [],
                }

            # Only add callback if there is one (LEFT JOIN might return NULL)
            if row["ansible_host"] is not None:
                tasks_dict[task_uuid]["callbacks"].append(
                    {
                        "ansible_host": row["ansible_host"],
                        "state": row["state"],
                        "module": row["module"],
                        "timestamp": row["timestamp"],
                        "result_dump": row["result_dump"],
                    }
                )

        tasks_with_callbacks = cast(list[TaskExport], list(tasks_dict.values()))

        # build export structure
        export_data: JobExport = {
            "exported_at": self.db_manager.get_db_now(),
            "job": {
                "tower_job_id": job["tower_job_id"],
                "tower_job_template_id": job["tower_job_template_id"],
                "tower_job_template_name": job["tower_job_template_name"],
                "ansible_limit": job["ansible_limit"],
                "tower_user_name": job["tower_user_name"],
                "awx_tags": awx_tags,
                "extra_vars": job["extra_vars"],
                "artifacts": job["artifacts"],
                "tower_schedule_id": job["tower_schedule_id"],
                "tower_schedule_name": job["tower_schedule_name"],
                "tower_workflow_job_id": job["tower_workflow_job_id"],
                "tower_workflow_job_name": job["tower_workflow_job_name"],
                "start_time": job["start_time"],
                "end_time": job["end_time"],
                "state": job["state"],
            },
            "job_template": {
                "tower_job_template_id": job["tower_job_template_id"],
                "tower_job_template_name": job["tower_job_template_name"],
                "playbook_path": job["playbook_path"],
                "compliance_interval": job["compliance_interval"],
                "awx_organisation": job["awx_organisation"],
                "template_infos": template_infos,
            },
            "stats": stats,
            "tasks": tasks_with_callbacks,
        }

        return export_data

    def import_job(self, export_data: JobExport) -> int:
        """
        Imports a job from export data.

        Returns the tower_job_id of the imported job.
        Raises ValueError if export_data is invalid or job already exists.
        """
        conn: sqlite3.Connection = self.db_manager.get_db_connection()
        cursor = conn.cursor()

        job_data = export_data.get("job")
        template_data = export_data.get("job_template")
        stats_data = export_data.get("stats", [])
        tasks_data = export_data.get("tasks", [])

        if not job_data or not template_data:
            raise ValueError("Invalid export data: missing job or job_template")

        tower_job_id = job_data["tower_job_id"]

        # check if job already exists
        if self.job_exists(tower_job_id):
            raise ValueError(f"Job with id {tower_job_id} already exists")

        try:
            # insert/update job_template
            template_infos_value = cast(str, template_data.get("template_infos", {}))
            if template_infos_value is not None:
                template_infos_json = json.dumps(template_infos_value)
            else:
                template_infos_json = None

            cursor.execute(
                """
                INSERT INTO job_templates (tower_job_template_id,
                                           tower_job_template_name,
                                           playbook_path,
                                           compliance_interval,
                                           awx_organisation,
                                           template_infos)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (tower_job_template_id) DO UPDATE
                    SET tower_job_template_name = excluded.tower_job_template_name,
                        playbook_path           = excluded.playbook_path,
                        compliance_interval     = excluded.compliance_interval,
                        awx_organisation        = excluded.awx_organisation,
                        template_infos          = excluded.template_infos
                """,
                (
                    template_data["tower_job_template_id"],
                    template_data["tower_job_template_name"],
                    template_data.get("playbook_path"),
                    template_data.get("compliance_interval", 0),
                    template_data.get("awx_organisation"),
                    template_infos_json,
                ),
            )

            # insert job
            # convert awx_tags list to comma-separated string for SQLite
            awx_tags_str = ",".join(job_data.get("awx_tags", []))

            # ensure extra_vars and artifacts are strings
            extra_vars = job_data.get("extra_vars", "{}")
            if isinstance(extra_vars, dict):
                extra_vars = json.dumps(extra_vars)

            artifacts = job_data.get("artifacts", "{}")
            if isinstance(artifacts, dict):
                artifacts = json.dumps(artifacts)

            cursor.execute(
                """
                INSERT INTO jobs (tower_job_id,
                                  tower_job_template_id,
                                  ansible_limit,
                                  tower_user_name,
                                  awx_tags,
                                  extra_vars,
                                  artifacts,
                                  tower_schedule_id,
                                  tower_schedule_name,
                                  tower_workflow_job_id,
                                  tower_workflow_job_name,
                                  start_time,
                                  end_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tower_job_id,
                    job_data["tower_job_template_id"],
                    job_data.get("ansible_limit"),
                    job_data.get("tower_user_name"),
                    awx_tags_str,
                    extra_vars,
                    artifacts,
                    job_data.get("tower_schedule_id"),
                    job_data.get("tower_schedule_name"),
                    job_data.get("tower_workflow_job_id"),
                    job_data.get("tower_workflow_job_name"),
                    job_data.get("start_time"),
                    job_data.get("end_time"),
                ),
            )

            # insert stats
            for stat in stats_data:
                cursor.execute(
                    """
                    INSERT INTO stats (tower_job_id,
                                       ansible_host,
                                       ok,
                                       failed,
                                       unreachable,
                                       changed,
                                       skipped,
                                       rescued,
                                       ignored)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        tower_job_id,
                        stat["ansible_host"],
                        stat["ok"],
                        stat["failed"],
                        stat["unreachable"],
                        stat["changed"],
                        stat["skipped"],
                        stat["rescued"],
                        stat["ignored"],
                    ),
                )

            # insert tasks and callbacks
            for task in tasks_data:
                # Insert task
                cursor.execute(
                    """
                    INSERT INTO tasks (ansible_uuid,
                                       tower_job_id,
                                       task_name)
                    VALUES (?, ?, ?)
                    """,
                    (
                        task["ansible_uuid"],
                        tower_job_id,
                        task["task_name"],
                    ),
                )

                # insert callbacks for this task
                for callback in task.get("callbacks", []):
                    result_dump = callback["result_dump"]

                    cursor.execute(
                        """
                        INSERT INTO task_callbacks (task_ansible_uuid,
                                                    ansible_host,
                                                    state,
                                                    module,
                                                    timestamp,
                                                    result_dump)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            task["ansible_uuid"],
                            callback["ansible_host"],
                            callback["state"],
                            callback["module"],
                            callback["timestamp"],
                            result_dump,
                        ),
                    )

            conn.commit()

            return tower_job_id

        except ValueError as e:
            conn.rollback()
            raise ValueError(f"Failed to import job: {str(e)}") from e


class PostgresApiRepository(ApiRepository):
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def get_all_noncompliant_hosts(self) -> dict[str, list[JobTemplateComplianceStates]]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
                SELECT
                    *
                FROM v_host_noncompliance
                """)
        ret = {}
        for line in cursor.fetchall():
            ret[line["ansible_host"]] = line["noncompliant"]
        return ret

    def job_exists(self, tower_job_id) -> bool:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT *
                        FROM jobs
                        WHERE tower_job_id = %s
                        """,
            (tower_job_id,),
        )

        return cursor.rowcount != 0

    def add_job(
        self,
        tower_job_id: int,
        tower_job_template_id: int,
        tower_job_template_name: str,
        awx_tags: list[str],
        extra_vars: JSon,
        artifacts: JSon,
        ansible_limit: Optional[str] = None,
        tower_user_name: Optional[str] = None,
        tower_schedule_id: Optional[int] = None,
        tower_schedule_name: Optional[str] = None,
        tower_workflow_job_id: Optional[int] = None,
        tower_workflow_job_name: Optional[str] = None,
        compliance_interval: int = 0,
        template_infos: Optional[str] = None,
        playbook_path: Optional[str] = None,
        awx_organisation: Optional[str] = None,
        start_time: Optional[TimeStamp] = None,
    ) -> None:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        INSERT INTO job_templates
                            (tower_job_template_id, tower_job_template_name, template_infos, playbook_path, awx_organisation, compliance_interval)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tower_job_template_id) DO UPDATE
                            SET (tower_job_template_name, template_infos, playbook_path, awx_organisation, compliance_interval) = (%s, %s, %s, %s, %s)
                        """,
            (
                tower_job_template_id,
                tower_job_template_name,
                template_infos,
                playbook_path,
                awx_organisation,
                compliance_interval,
                tower_job_template_name,
                template_infos,
                playbook_path,
                awx_organisation,
                compliance_interval,
            ),
        )

        # insert job
        cursor.execute(
            """
                        INSERT INTO jobs (
                            tower_job_id,
                            tower_job_template_id,
                            ansible_limit,
                            tower_user_name,
                            awx_tags,
                            extra_vars,
                            artifacts,
                            tower_schedule_id,
                            tower_schedule_name,
                            tower_workflow_job_id,
                            tower_workflow_job_name
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
            (
                tower_job_id,
                tower_job_template_id,
                ansible_limit,
                tower_user_name,
                json.dumps(awx_tags),
                extra_vars,
                artifacts,
                tower_schedule_id,
                tower_schedule_name,
                tower_workflow_job_id,
                tower_workflow_job_name,
            ),
        )

        # set start_time if given and not null
        if start_time is not None:
            cursor.execute(
                """UPDATE jobs
                        SET start_time = %s
                    WHERE tower_job_id = %s
                """,
                (start_time, tower_job_id),
            )

        conn.commit()

    def add_callback(
        self,
        task_ansible_uuid: str,
        ansible_host: str,
        state: JobState,
        result_dump: JSon,
        module: str,
        timestamp: Optional[str] = None,
    ) -> None:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                    INSERT INTO task_callbacks (task_ansible_uuid, ansible_host, state, module, result_dump)
                    VALUES (%s, %s, %s, %s, %s)
                                    """,
                (task_ansible_uuid, ansible_host, state, module, result_dump),
            )
        except ForeignKeyViolation as ex:
            raise LufaKeyError("ansible_uuid", task_ansible_uuid) from ex

        # set timestamp if given and not None
        if timestamp is not None:
            cursor.execute(
                """
                                UPDATE task_callbacks
                                SET timestamp = ?
                                WHERE task_ansible_uuid = ?
                            """,
                (timestamp, task_ansible_uuid),
            )
        conn.commit()

    def tasks_exists(self, task_uuid: str) -> bool:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT *
                    FROM tasks
                    WHERE ansible_uuid = %s
                """,
            (task_uuid,),
        )

        return cursor.rowcount > 0

    def add_task(self, ansible_uuid: str, tower_job_id: int, task_name: str) -> None:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                        INSERT INTO tasks (ansible_uuid, tower_job_id, task_name)
                        VALUES (%s, %s, %s)
                        """,
                (ansible_uuid, tower_job_id, task_name),
            )
        except ForeignKeyViolation as ex:
            raise LufaKeyError("tower_job_id", tower_job_id) from ex

        conn.commit()

    def add_stats(self, tower_job_id: int, stats: list[TowerJobStats]) -> None:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()

        try:
            for stat in stats:
                cursor.execute(
                    """
                                        INSERT INTO stats (
                                            tower_job_id,
                                            ansible_host,
                                            ok,
                                            failed,
                                            unreachable,
                                            changed,
                                            skipped,
                                            rescued,
                                            ignored
                                        )
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT DO NOTHING
                                    """,
                    (
                        tower_job_id,
                        stat["ansible_host"],
                        stat["ok"],
                        stat["failed"],
                        stat["unreachable"],
                        stat["changed"],
                        stat["skipped"],
                        stat["rescued"],
                        stat["ignored"],
                    ),
                )
            cursor.execute("REFRESH MATERIALIZED VIEW v_host_templates;")
        except ForeignKeyViolation as ex:
            raise LufaKeyError("tower_job_id", tower_job_id) from ex

        conn.commit()

    def update_job(
        self, tower_job_id: int, end_time: Optional[TimeStamp] = None, artifacts: Optional[JSon] = None
    ) -> None:
        with self.db_manager.get_db_connection() as db_conn:
            cur = db_conn.cursor()
            try:
                cur.execute(
                    """
                            UPDATE jobs
                            SET
                                end_time = COALESCE(%s, now()),
                                artifacts = COALESCE(%s, '{}'::jsonb)
                            WHERE tower_job_id = %s
                            """,
                    (
                        end_time,
                        artifacts,
                        tower_job_id,
                    ),
                )
            except InvalidTextRepresentation as ex:
                raise LufaKeyError("artifacts", artifacts) from ex
            except InvalidDatetimeFormat as ex:
                raise LufaKeyError("end_time", end_time) from ex
            except ForeignKeyViolation as ex:
                raise LufaKeyError("tower_job_id", tower_job_id) from ex
            db_conn.commit()

    def export_job(self, tower_job_id: int) -> JobExport:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()

        # get job data with template info
        cursor.execute(
            """
            SELECT v_job_status.*,
                   job_templates.tower_job_template_name,
                   job_templates.playbook_path,
                   job_templates.compliance_interval,
                   job_templates.awx_organisation,
                   job_templates.template_infos,
                   to_char(v_job_status.start_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as start_time,
                   to_char(v_job_status.end_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS')   as end_time
            FROM v_job_status
                     JOIN job_templates
                          ON v_job_status.tower_job_template_id = job_templates.tower_job_template_id
            WHERE v_job_status.tower_job_id = %s
            """,
            (tower_job_id,),
        )

        job = cursor.fetchone()
        if not job:
            raise ResourceNotFoundError(f"Job with id {tower_job_id} not found")

        # get stats
        cursor.execute(
            """
            SELECT ansible_host,
                   ok,
                   failed,
                   unreachable,
                   changed,
                   skipped,
                   rescued,
                   ignored
            FROM stats
            WHERE tower_job_id = %s
            ORDER BY ansible_host
            """,
            (tower_job_id,),
        )
        stats = cursor.fetchall()

        # get tasks with their callbacks
        cursor.execute(
            """
            SELECT tasks.ansible_uuid,
                   tasks.task_name,
                   json_agg(
                           json_build_object(
                                   'ansible_host', task_callbacks.ansible_host,
                                   'state', task_callbacks.state,
                                   'module', task_callbacks.module,
                                   'timestamp', to_char(task_callbacks.timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
                                   'result_dump', task_callbacks.result_dump
                           ) ORDER BY task_callbacks.timestamp
                   ) as callbacks
            FROM tasks
                     LEFT JOIN task_callbacks
                               ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
            WHERE tasks.tower_job_id = %s
            GROUP BY tasks.ansible_uuid, tasks.task_name
            ORDER BY tasks.task_name
            """,
            (tower_job_id,),
        )

        tasks_with_callbacks: list[TaskExport] = cursor.fetchall()

        # build export structure
        export_data: JobExport = {
            "exported_at": self.db_manager.get_db_now(),
            "job": {
                "tower_job_id": job["tower_job_id"],
                "tower_job_template_id": job["tower_job_template_id"],
                "tower_job_template_name": job["tower_job_template_name"],
                "ansible_limit": job["ansible_limit"],
                "tower_user_name": job["tower_user_name"],
                "awx_tags": job["awx_tags"],
                "extra_vars": job["extra_vars"],
                "artifacts": job["artifacts"],
                "tower_schedule_id": job["tower_schedule_id"],
                "tower_schedule_name": job["tower_schedule_name"],
                "tower_workflow_job_id": job["tower_workflow_job_id"],
                "tower_workflow_job_name": job["tower_workflow_job_name"],
                "start_time": job["start_time"],
                "end_time": job["end_time"],
                "state": job["state"],
            },
            "job_template": {
                "tower_job_template_id": job["tower_job_template_id"],
                "tower_job_template_name": job["tower_job_template_name"],
                "playbook_path": job["playbook_path"],
                "compliance_interval": job["compliance_interval"],
                "awx_organisation": job["awx_organisation"],
                "template_infos": job["template_infos"],
            },
            "stats": stats,
            "tasks": [
                {
                    "ansible_uuid": task["ansible_uuid"],
                    "task_name": task["task_name"],
                    "callbacks": task["callbacks"] if task["callbacks"] else [],
                }
                for task in tasks_with_callbacks
            ],
        }
        return export_data

    def import_job(self, export_data: JobExport) -> int:
        """Imports a job from a dict
        Returns the tower_job_id of the imported job.
        """
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()

        job_data = export_data.get("job")
        template_data = export_data.get("job_template")
        stats_data = export_data.get("stats", [])
        tasks_data = export_data.get("tasks", [])

        if not job_data or not template_data:
            raise ValueError("Invalid export data: missing job or job_template")

        tower_job_id = job_data["tower_job_id"]

        # check if job already exists
        if self.job_exists(tower_job_id):
            raise ValueError(f"Job with id {tower_job_id} already exists")

        try:
            # insert/update job_template
            cursor.execute(
                """
                INSERT INTO job_templates (tower_job_template_id,
                                           tower_job_template_name,
                                           playbook_path,
                                           compliance_interval,
                                           awx_organisation,
                                           template_infos)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tower_job_template_id) DO UPDATE
                    SET tower_job_template_name = EXCLUDED.tower_job_template_name,
                        playbook_path           = EXCLUDED.playbook_path,
                        compliance_interval     = EXCLUDED.compliance_interval,
                        awx_organisation        = EXCLUDED.awx_organisation,
                        template_infos          = EXCLUDED.template_infos
                """,
                (
                    template_data["tower_job_template_id"],
                    template_data["tower_job_template_name"],
                    template_data.get("playbook_path"),
                    template_data.get("compliance_interval", 0),
                    template_data.get("awx_organisation"),
                    json.dumps(template_data.get("template_infos")) if template_data.get("template_infos") else None,
                ),
            )

            # insert job
            # ensure extra_vars and artifacts are strings
            extra_vars = json.dumps(cast(dict, job_data.get("extra_vars", {})))
            artifacts = json.dumps(cast(dict, job_data.get("artifacts", {})))

            cursor.execute(
                """
                INSERT INTO jobs (tower_job_id,
                                  tower_job_template_id,
                                  ansible_limit,
                                  tower_user_name,
                                  awx_tags,
                                  extra_vars,
                                  artifacts,
                                  tower_schedule_id,
                                  tower_schedule_name,
                                  tower_workflow_job_id,
                                  tower_workflow_job_name,
                                  start_time,
                                  end_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tower_job_id,
                    job_data["tower_job_template_id"],
                    job_data.get("ansible_limit"),
                    job_data.get("tower_user_name"),
                    json.dumps(job_data.get("awx_tags", [])),
                    extra_vars,
                    artifacts,
                    job_data.get("tower_schedule_id"),
                    job_data.get("tower_schedule_name"),
                    job_data.get("tower_workflow_job_id"),
                    job_data.get("tower_workflow_job_name"),
                    job_data.get("start_time"),
                    job_data.get("end_time"),
                ),
            )

            # insert stats
            for stat in stats_data:
                cursor.execute(
                    """
                    INSERT INTO stats (tower_job_id,
                                       ansible_host,
                                       ok,
                                       failed,
                                       unreachable,
                                       changed,
                                       skipped,
                                       rescued,
                                       ignored)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        tower_job_id,
                        stat["ansible_host"],
                        stat["ok"],
                        stat["failed"],
                        stat["unreachable"],
                        stat["changed"],
                        stat["skipped"],
                        stat["rescued"],
                        stat["ignored"],
                    ),
                )

            # insert tasks and callbacks
            for task in tasks_data:
                # Insert task
                cursor.execute(
                    """
                    INSERT INTO tasks (ansible_uuid,
                                       tower_job_id,
                                       task_name)
                    VALUES (%s, %s, %s)
                    """,
                    (
                        task["ansible_uuid"],
                        tower_job_id,
                        task["task_name"],
                    ),
                )

                # insert callbacks for this task
                for callback in task.get("callbacks", []):
                    result_dump = json.dumps(callback["result_dump"])
                    cursor.execute(
                        """
                        INSERT INTO task_callbacks (task_ansible_uuid,
                                                    ansible_host,
                                                    state,
                                                    module,
                                                    timestamp,
                                                    result_dump)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            task["ansible_uuid"],
                            callback["ansible_host"],
                            callback["state"],
                            callback["module"],
                            callback.get("timestamp"),
                            result_dump,
                        ),
                    )

            # refresh materialized view for compliance calculations
            cursor.execute("REFRESH MATERIALIZED VIEW v_host_templates;")

            conn.commit()

            return tower_job_id

        except Exception as e:
            conn.rollback()
            raise ValueError(f"Failed to import job: {str(e)}") from e
