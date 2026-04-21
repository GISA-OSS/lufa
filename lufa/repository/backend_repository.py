import json
import sqlite3
from abc import ABC, abstractmethod
from typing import Final, Optional, TypedDict

from lufa.database import DatabaseManager, Job, JobState, JSon, NumDays, Path, TimeStamp


class CompliantHostStats(TypedDict):
    compliant: int
    not_compliant: int


class JobTaskCallbacks(TypedDict):
    ansible_host: str
    id: int
    state: JobState
    task_name: str
    timestamp: TimeStamp


class JobCallbackData(TypedDict):
    ansible_host: str
    result_dump: JSon
    state: JobState
    task_name: str
    timestamp: TimeStamp
    tower_job_id: int
    tower_job_template_id: int
    tower_job_template_name: str


class JobTemplate(TypedDict):
    tower_job_template_id: int
    tower_job_template_name: str
    playbook_path: Path
    awx_organisation: str
    compliance_interval: NumDays
    compliant: bool
    start_time: TimeStamp


class HostCallback(TypedDict):
    ansible_host: str
    id: int
    state: JobState
    task_name: str
    timestamp: TimeStamp
    tower_job_id: int
    tower_job_template_name: str


class JobStatus(TypedDict):
    ansible_limit: str
    awx_organisation: str
    awx_tags: list[str]
    end_time: Optional[TimeStamp]
    compliance_interval: NumDays
    start_time: Optional[TimeStamp]
    state: JobState
    template_infos: str
    tower_job_id: int
    tower_job_template_id: int
    tower_job_template_name: str
    tower_schedule_id: int
    tower_schedule_name: str
    tower_user_name: str
    tower_workflow_job_id: int
    tower_workflow_job_name: str


class TemplateInfo(TypedDict): ...


class Template(TypedDict):
    template_infos: TemplateInfo


class JobStats(TypedDict):
    tower_job_id: int
    ansible_host: str
    ok: int
    failed: int
    unreachable: int
    changed: int
    skipped: int
    rescued: int
    ignored: int


class HostCallbackCounts(JobStats):
    awx_tags: list[str]
    last_callback_id: int
    success: int
    compliance_interval: NumDays
    start_time: TimeStamp
    tower_job_template_name: str


class HostTemplates(TypedDict):
    awx_tags: list[str]
    compliant: bool
    compliance_interval: NumDays
    start_time: TimeStamp
    successful: bool
    tower_job_id: int
    tower_job_template_id: int
    tower_job_template_name: str


class HostComplianceStates(TypedDict):
    ansible_host: str
    compliant: bool


class HostSummary(TypedDict):
    ansible_host: str
    awx_tags: list[str]
    compliant: bool
    compliance_interval: NumDays
    start_time: TimeStamp
    tower_job_id: int


class TemplateJobData(TypedDict):
    ansible_limit: str
    awx_tags: list[str]
    end_time: Optional[TimeStamp]
    start_time: TimeStamp
    state: str
    tower_job_id: int
    tower_job_template_id: int
    tower_schedule_id: int
    tower_schedule_name: str
    tower_user_name: str
    tower_workflow_job_id: int
    tower_workflow_job_name: str


class ResourceNotFoundError(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)
        self.msg = msg


class BackendRepository(ABC):
    ERROR: Final = "error"
    SUCCESS: Final = "success"
    FAILED: Final = "failed"
    STARTED: Final = "started"

    @abstractmethod
    def get_job_template_name_by_job_id(self, tower_job_id: int) -> str:
        """Get the tower job template name by the job id"""
        pass

    @abstractmethod
    def get_job_template_name_by_template_id(self, tower_job_template_id: int) -> str:
        """Get the tower job template name by the template id"""
        pass

    @abstractmethod
    def get_workflow_job_name(self, tower_workflow_job_id: int) -> str:
        """Get the tower workflow name by the workflow job id"""
        pass

    @abstractmethod
    def get_job_stats(self, tower_job_id: int) -> JobStats:
        pass

    @abstractmethod
    def get_job_status(self, tower_job_id: int) -> JobStatus:
        """Get the job status using the id. The status can be started, success or error"""
        pass

    @abstractmethod
    def get_job_task_callbacks(self, tower_job_id: int) -> list[JobTaskCallbacks]:
        pass

    @abstractmethod
    def get_last_host_callback(self, tower_job_id: int) -> list[HostCallback]:
        """Get the last Callback for each Host"""
        pass

    @abstractmethod
    def get_last_host_callback_count(self, tower_job_id: int) -> dict[str, int]:
        """Get state and count grouped by state"""
        pass

    @abstractmethod
    def get_last_host_callback_task_count(self, tower_job_id: int) -> dict[str, int]:
        """Get state and count grouped by state for callback status"""
        pass

    @abstractmethod
    def get_job_info(self, tower_job_id: int) -> Optional[Job]:
        """Get infos about the job"""
        pass

    @abstractmethod
    def get_workflow_job_info(self, tower_workflow_job_id: int) -> list[JobStatus]:
        """Get info about a workflow job"""
        pass

    @abstractmethod
    def get_workflow_callbacks_count(self, tower_workflow_job_id: int) -> int:
        """Get state and count grouped by state for workflow callback status"""
        pass

    @abstractmethod
    def get_template(self, tower_job_template_id: int) -> Optional[Template]:
        """Get infos about a single template"""
        pass

    @abstractmethod
    def get_callback_data(self, task_callback_id: int) -> Optional[JobCallbackData]:
        pass

    @abstractmethod
    def get_all_host_compliance_state(self) -> list[HostComplianceStates]:
        pass

    @abstractmethod
    def get_host_templates(self, ansible_host: str) -> list[HostTemplates]:
        pass

    @abstractmethod
    def get_host_jobs(self, ansible_host: str) -> list[dict]:
        pass

    @abstractmethod
    def get_host_last_callback(self, ansible_host: str) -> list[HostCallbackCounts]:
        """Get the last callback of a host"""
        pass

    @abstractmethod
    def get_all_job_templates(self) -> list[JobTemplate]:
        pass

    @abstractmethod
    def get_template_job_data(self, tower_job_template_id: int) -> list[TemplateJobData]:
        pass

    @abstractmethod
    def get_template_hosts_summary(self, tower_job_template_id: int) -> list[HostSummary]:
        pass

    @abstractmethod
    def get_last_host_callbacks_by_workflow_id(self, tower_workflow_job_id: int) -> list[HostCallback]:
        pass

    @abstractmethod
    def get_last_host_callbacks_count_by_workflow_id(self, tower_workflow_job_id: int) -> dict:
        pass

    @abstractmethod
    def get_all_workflow_jobs(self) -> list[dict]:
        pass

    @abstractmethod
    def get_workflow_job_stats(self, tower_workflow_job_id: int) -> dict:
        pass

    @abstractmethod
    def get_workflow_callbacks(self, tower_workflow_job_id: int) -> dict:
        pass

    @abstractmethod
    def get_last_jobs_by_days(self, days: int) -> list[Job]:
        pass

    @abstractmethod
    def get_compliant_non_compliant_stats(self) -> CompliantHostStats:
        """Get the stats for the welcome page."""
        pass


class SqliteBackendRepository(BackendRepository):
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def get_job_template_name_by_job_id(self, tower_job_id: int) -> str:
        # Get Template Name and Check if Job exists
        conn = self.db_manager.get_db_connection()
        cursor: sqlite3.Cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT tower_job_template_name
                        FROM job_templates
                        JOIN jobs
                        ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                        WHERE jobs.tower_job_id = ?
                        """,
            (tower_job_id,),
        )

        fetched = cursor.fetchone()
        if fetched is not None:
            return fetched["tower_job_template_name"]

        raise ResourceNotFoundError(f"Job with id {tower_job_id} not found")

    def get_job_template_name_by_template_id(self, tower_job_template_id: int) -> str:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT tower_job_template_name
                        FROM job_templates
                        WHERE tower_job_template_id = ?
                       """,
            (tower_job_template_id,),
        )

        fetched = cursor.fetchone()
        if fetched is not None:
            return fetched["tower_job_template_name"]

        raise ResourceNotFoundError(f"Job Template with id {tower_job_template_id} not found")

    def get_workflow_job_name(self, tower_workflow_job_id: int) -> str:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT tower_workflow_job_name
                        FROM jobs
                        WHERE tower_workflow_job_id = ?
                        LIMIT 1
                       """,
            (tower_workflow_job_id,),
        )

        fetched = cursor.fetchone()
        if fetched is not None:
            return fetched["tower_workflow_job_name"]

        raise ResourceNotFoundError(f"Workflow with id {tower_workflow_job_id} not found")

    def get_job_stats(self, tower_job_id: int) -> JobStats:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT * FROM stats
                        WHERE tower_job_id = ?
                        """,
            (tower_job_id,),
        )

        return cursor.fetchall()

    def get_job_status(self, tower_job_id: int) -> JobStatus:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT
                            v_job_status.*,
                            job_templates.*,
                            strftime('%Y-%m-%dT%H:%M:%f', end_time) as end_time,
                            strftime('%Y-%m-%dT%H:%M:%f', start_time) as start_time
                        FROM v_job_status
                        JOIN job_templates
                        ON v_job_status.tower_job_template_id = job_templates.tower_job_template_id
                        WHERE tower_job_id = ?
                        """,
            (tower_job_id,),
        )

        line = cursor.fetchone()
        line["awx_tags"] = line["awx_tags"].split(",")
        line["template_infos"] = line["template_infos"] = (
            json.loads(line["template_infos"]) if line["template_infos"] else None
        )
        return line

    def get_job_task_callbacks(self, tower_job_id: int) -> list[JobTaskCallbacks]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT
                            id, task_name, ansible_host, state,
                            strftime('%Y-%m-%dT%H:%M:%f', timestamp) as timestamp
                        FROM task_callbacks
                        JOIN tasks
                        ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                        WHERE tower_job_id = ?
                        """,
            (tower_job_id,),
        )

        return cursor.fetchall()

    def get_last_host_callback(self, tower_job_id: int) -> list[HostCallback]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT
                            tc1.id,
                            task_name,
                            ansible_host,
                            state,
                            strftime('%Y-%m-%dT%H:%M:%f', tc2.timestamp) as timestamp
                        FROM (SELECT max(id) as id
                                    FROM task_callbacks
                                    JOIN tasks
                                    ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                                    WHERE tasks.tower_job_id = ?
                                    GROUP BY ansible_host
                                ) as tc1
                        JOIN task_callbacks as tc2
                          ON tc1.id = tc2.id
                        JOIN tasks
                          ON tasks.ansible_uuid = tc2.task_ansible_uuid
                        WHERE tasks.tower_job_id = ?
                        ORDER BY ansible_host;
                        """,
            (tower_job_id, tower_job_id),
        )
        return cursor.fetchall()

    def get_last_host_callback_count(self, tower_job_id: int) -> dict[str, int]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                SELECT state, count(state) as "count"
                        FROM (
                            SELECT
                                tc1.id,
                                task_name,
                                ansible_host,
                                state
                            FROM (SELECT max(id) as id
                                    FROM task_callbacks
                                    JOIN tasks
                                    ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                                    WHERE tasks.tower_job_id = ?
                                    GROUP BY ansible_host
                                ) as tc1
                            JOIN task_callbacks as tc2
                            ON tc1.id = tc2.id
                            JOIN tasks
                            ON tasks.ansible_uuid = tc2.task_ansible_uuid
                            WHERE tasks.tower_job_id = ?
                            ORDER BY ansible_host
                        ) as host_states
                        GROUP BY state;
            """,
            (tower_job_id, tower_job_id),
        )
        return cursor.fetchall()

    def get_last_host_callback_task_count(self, tower_job_id: int) -> dict[str, int]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                 SELECT state, count(state) as "count"
                        FROM task_callbacks
                        JOIN tasks
                        ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                        WHERE tower_job_id = ?
                        GROUP BY state;
                    """,
            (tower_job_id,),
        )
        return cursor.fetchall()

    def get_job_info(self, tower_job_id: int) -> Optional[Job]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        jobs.tower_job_template_id,
                        jobs.awx_tags,
                        jobs.extra_vars,
                        jobs.artifacts,
                        jobs.tower_user_name,
                        jobs.tower_schedule_name,
                        jobs.tower_workflow_job_name,
                        jobs.ansible_limit,
                        jobs.start_time,
                        jobs.end_time,
                        job_templates.playbook_path
                    FROM jobs
                    JOIN job_templates ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                    WHERE tower_job_id = ?
                """,
            (tower_job_id,),
        )
        line = cursor.fetchone()
        line["awx_tags"] = line["awx_tags"].split(",")
        return line

    def get_workflow_job_info(self, tower_workflow_job_id: int) -> list[JobStatus]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT
                            v_job_status.*,
                            job_templates.*,
                            strftime('%Y-%m-%dT%H:%M:%f', start_time) as start_time
                        FROM v_job_status
                        JOIN job_templates
                        ON v_job_status.tower_job_template_id = job_templates.tower_job_template_id
                        WHERE tower_workflow_job_id = ?
                        ORDER BY v_job_status.start_time asc
                        """,
            (tower_workflow_job_id,),
        )

        ret = cursor.fetchall()
        for line in ret:
            line["awx_tags"] = line["awx_tags"].split(",")
            line["template_infos"] = json.loads(line["template_infos"])
        return ret

    def get_template(self, tower_job_template_id: int) -> Optional[Template]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT *
                    FROM job_templates
                    WHERE tower_job_template_id = ?
                """,
            (tower_job_template_id,),
        )
        fetched = cursor.fetchone()
        # template_infos can be None
        if fetched["template_infos"]:
            fetched["template_infos"] = json.loads(fetched["template_infos"])

        return fetched

    def get_callback_data(self, task_callback_id: int) -> Optional[JobCallbackData]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        job_templates.tower_job_template_id,
                        tower_job_template_name,
                        jobs.tower_job_id,
                        job_templates.playbook_path,
                        task_name,
                        ansible_host,
                        state,
                        module,
                        strftime('%Y-%m-%dT%H:%M:%f', timestamp) as timestamp,
                        result_dump
                    FROM task_callbacks
                    JOIN tasks
                    ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                    JOIN jobs
                    ON jobs.tower_job_id = tasks.tower_job_id
                    JOIN job_templates
                    ON job_templates.tower_job_template_id = jobs.tower_job_template_id
                    WHERE task_callbacks.id = ?
                """,
            (task_callback_id,),
        )
        ret = cursor.fetchone()
        ret["result_dump"] = json.loads(ret["result_dump"])
        return ret

    def get_all_host_compliance_state(self) -> list[HostComplianceStates]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
                    SELECT
                        *
                    FROM v_host_compliance
                """)
        ret = []
        for line in cursor.fetchall():
            line["compliant"] = line["compliant"] == 1
            ret.append(line)
        return ret

    def get_host_templates(self, ansible_host: str) -> list[HostTemplates]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        tower_job_template_id,
                        tower_job_template_name,
                        awx_tags,
                        MAX(compliant) AS compliant,
                        successful,
                        strftime('%Y-%m-%dT%H:%M:%f', start_time) as start_time,
                        compliance_interval,
                        tower_job_id
                    FROM v_host_templates
                    WHERE ansible_host = ?
                	GROUP BY tower_job_template_id
                    HAVING MAX(start_time)
                    ORDER BY start_time desc, tower_job_template_id, awx_tags;
                """,
            (ansible_host,),
        )
        ret = []
        for line in cursor.fetchall():
            line["awx_tags"] = line["awx_tags"].split(",")
            line["compliant"] = line["compliant"] == 1
            line["successful"] = line["successful"] == 1
            ret.append(line)
        return ret

    def get_host_jobs(self, ansible_host: str) -> list[dict]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        stats.*,
                        tower_job_template_name,
                        awx_tags,
                        strftime('%Y-%m-%dT%H:%M:%f', start_time) as start_time,
                        compliance_interval
                    FROM stats
                    JOIN jobs
                    ON stats.tower_job_id = jobs.tower_job_id
                    JOIN job_templates
                    ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                    WHERE ansible_host = ?
                """,
            (ansible_host,),
        )
        ret = []
        for line in cursor.fetchall():
            line["awx_tags"] = line["awx_tags"].split(",")
            ret.append(line)
        return ret

    def get_host_last_callback(self, ansible_host: str) -> list[HostCallbackCounts]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        job_stats.*,
                        strftime('%Y-%m-%dT%H:%M:%f', start_time) as start_time,
                        job_callbacks.id as last_callback_id
                    FROM v_tower_job_stats as job_stats
                    JOIN v_last_tower_job_callbacks as job_callbacks
                      ON job_stats.tower_job_id = job_callbacks.tower_job_id
                    WHERE job_callbacks.ansible_host = :1
                      AND job_stats.ansible_host = :1
                """,
            (ansible_host,),
        )
        ret = []
        for line in cursor.fetchall():
            line["awx_tags"] = line["awx_tags"].split(",")
            ret.append(line)
        return ret

    def get_all_job_templates(self) -> list[JobTemplate]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
                    SELECT
                        job_templates.tower_job_template_id,
                        tower_job_template_name,
                        playbook_path,
                        awx_organisation,
                        strftime('%Y-%m-%dT%H:%M:%f', jobs.start_time) as start_time,
                        compliance_interval,
                        (count(v_template_compliance.noncompliant) = 0) AS compliant
                    FROM job_templates
                    JOIN jobs
                    ON job_templates.tower_job_template_id = jobs.tower_job_template_id
                    LEFT JOIN v_template_compliance
                    ON v_template_compliance.tower_job_template_id = job_templates.tower_job_template_id
                    GROUP BY job_templates.tower_job_template_id
                    HAVING MAX(jobs.start_time)
                    ORDER BY job_templates.tower_job_template_id, start_time DESC
                """)

        ret = []
        for line in cursor.fetchall():
            line["compliant"] = line["compliant"] == 1
            ret.append(line)
        return ret

    def get_template_job_data(self, tower_job_template_id: int) -> list[TemplateJobData]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        v_job_status.*,
                        strftime('%Y-%m-%dT%H:%M:%f', end_time) as end_time,
                        strftime('%Y-%m-%dT%H:%M:%f', start_time) as start_time
                    FROM v_job_status
                    WHERE tower_job_template_id = ?
                """,
            (tower_job_template_id,),
        )
        ret = []
        for line in cursor.fetchall():
            line["awx_tags"] = line["awx_tags"].split(",")
            ret.append(line)
        return ret

    def get_template_hosts_summary(self, tower_job_template_id: int) -> list[HostSummary]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                	    ansible_host,
                	    awx_tags,
                	    compliance_interval,
                	    compliant,
                        strftime('%Y-%m-%dT%H:%M:%f', start_time) as start_time,
                	    tower_job_id
                    FROM v_template_hosts_summary
                    WHERE tower_job_template_id = ?
                """,
            (tower_job_template_id,),
        )
        line = cursor.fetchall()
        for item in line:
            item["awx_tags"] = item["awx_tags"].split(",")
            item["compliant"] = item["compliant"] == 1
        return line

    def get_last_host_callbacks_by_workflow_id(self, tower_workflow_job_id: int) -> list[HostCallback]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        id,
                        jobs.tower_job_id,
                        tower_job_template_name,
                        task_name,
                        ansible_host,
                        state,
                        strftime('%Y-%m-%dT%H:%M:%f', max(timestamp)) as timestamp
                    FROM task_callbacks
                    JOIN tasks
                      ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                    JOIN jobs
                      ON jobs.tower_job_id = tasks.tower_job_id
                    JOIN job_templates
                      ON job_templates.tower_job_template_id = jobs.tower_job_template_id
                    WHERE tower_workflow_job_id = ?
                    GROUP BY ansible_host
                    ORDER BY ansible_host, task_callbacks.timestamp DESC
                """,
            (tower_workflow_job_id,),
        )
        return cursor.fetchall()

    def get_last_host_callbacks_count_by_workflow_id(self, tower_workflow_job_id: int) -> dict:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT state, count(state) as "count"
                    FROM (
                        SELECT
                            id,
                            task_name,
                            ansible_host,
                            state,
                            strftime('%Y-%m-%dT%H:%M:%f', max(timestamp)) as timestamp
                        FROM task_callbacks
                        JOIN tasks
                        ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                        JOIN jobs
                        ON jobs.tower_job_id = tasks.tower_job_id
                        WHERE tower_workflow_job_id = ?
                        GROUP BY ansible_host
                        ORDER BY ansible_host DESC
                    ) as host_states
                    GROUP BY state
                """,
            (tower_workflow_job_id,),
        )
        return cursor.fetchall()

    def get_all_workflow_jobs(self) -> list[dict]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
                    SELECT
                        tower_workflow_job_id,
                        tower_workflow_job_name,
                        count(*) as count_jobs,
                        strftime('%Y-%m-%dT%H:%M:%f', MIN(start_time)) as start_time,
                        group_concat(DISTINCT awx_tags) as awx_tags,
                        group_concat(DISTINCT ansible_limit) as ansible_limit
                    FROM jobs
                    WHERE tower_workflow_job_id IS NOT NULL
                    GROUP BY
                        tower_workflow_job_id,
                        tower_workflow_job_name
                    ORDER BY MIN(start_time) desc
                """)
        ret = cursor.fetchall()
        for line in ret:
            line["awx_tags"] = line["awx_tags"].split(",")
            line["ansible_limit"] = line["ansible_limit"].split(",")
        return ret

    def get_workflow_job_stats(self, tower_workflow_job_id: int) -> dict:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT stats.*, tower_job_template_name 
                    FROM stats
                    JOIN jobs
                    ON stats.tower_job_id = jobs.tower_job_id
                    JOIN job_templates
                    ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                    WHERE tower_workflow_job_id = ?
                """,
            (tower_workflow_job_id,),
        )
        return cursor.fetchall()

    def get_workflow_callbacks(self, tower_workflow_job_id: int) -> dict:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        id,
                        jobs.tower_job_id,
                        tower_job_template_name,
                        task_name,
                        ansible_host,
                        state,
                        strftime('%Y-%m-%dT%H:%M:%f', timestamp) as timestamp
                    FROM task_callbacks
                    JOIN tasks
                    ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                    JOIN jobs
                    ON tasks.tower_job_id = jobs.tower_job_id
                    JOIN job_templates
                    ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                    WHERE tower_workflow_job_id = ?
                    """,
            (tower_workflow_job_id,),
        )
        return cursor.fetchall()

    def get_workflow_callbacks_count(self, tower_workflow_job_id: int) -> int:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        state,
                        count(state) as "count"
                    FROM task_callbacks
                    JOIN tasks
                    ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                    JOIN jobs
                    ON tasks.tower_job_id = jobs.tower_job_id
                    JOIN job_templates
                    ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                    WHERE tower_workflow_job_id = ?
                    GROUP BY state
                    """,
            (tower_workflow_job_id,),
        )
        return cursor.fetchall()

    def get_last_jobs_by_days(self, days: int) -> list[Job]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        if type(days) is not int:
            raise TypeError
        cursor.execute(
            """
                            SELECT
                                v_job_status.*,
                                job_templates.*,
                                strftime('%Y-%m-%dT%H:%M:%f', end_time) as end_time,
                                strftime('%Y-%m-%dT%H:%M:%f', start_time) as start_time
                            FROM v_job_status
                            JOIN job_templates
                            ON v_job_status.tower_job_template_id = job_templates.tower_job_template_id
                            WHERE start_time > datetime(current_timestamp, concat('-', ?, ' DAYS'))
                        """,
            (days,),
        )
        ret = []
        for line in cursor.fetchall():
            line["awx_tags"] = line["awx_tags"].split(",")
            line["template_infos"] = json.loads(line["template_infos"]) if line["template_infos"] else None
            ret.append(line)
        return ret

    def get_compliant_non_compliant_stats(self) -> CompliantHostStats:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
                    SELECT
                        count(*) FILTER (WHERE compliant = True) as compliant,
                        count(*) FILTER (WHERE compliant = False) as not_compliant
                    FROM v_host_compliance;
                """)
        return cursor.fetchone()


class PostgresBackendRepository(BackendRepository):
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def get_job_template_name_by_job_id(self, tower_job_id: int) -> str:
        # Get Template Name and Check if Job exists
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT tower_job_template_name
                        FROM job_templates
                        JOIN jobs
                        ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                        WHERE jobs.tower_job_id = %s
                    """,
            (tower_job_id,),
        )

        fetched = cursor.fetchone()
        if fetched is not None:
            return fetched["tower_job_template_name"]

        raise ResourceNotFoundError(f"Job with id {tower_job_id} not found")

    def get_job_template_name_by_template_id(self, tower_job_template_id: int) -> str:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                        SELECT tower_job_template_name
                        FROM job_templates
                        WHERE tower_job_template_id = %s
                    """,
            (tower_job_template_id,),
        )

        fetched = cursor.fetchone()
        if fetched is not None:
            return fetched["tower_job_template_name"]

        raise ResourceNotFoundError(f"Job Template with id {tower_job_template_id} not found")

    def get_workflow_job_name(self, tower_workflow_job_id: int) -> str:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT tower_workflow_job_name
                    FROM jobs
                    WHERE tower_workflow_job_id = %s
                    LIMIT 1
                """,
            (tower_workflow_job_id,),
        )

        fetched = cursor.fetchone()
        if fetched is not None:
            return fetched["tower_workflow_job_name"]

        raise ResourceNotFoundError(f"Workflow Job with id {tower_workflow_job_id} not found")

    def get_job_stats(self, tower_job_id: int) -> JobStats:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT * 
                    FROM stats
                    WHERE tower_job_id = %s
                """,
            (tower_job_id,),
        )

        return cursor.fetchall()

    def get_job_status(self, tower_job_id: int) -> JobStatus:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                v_job_status.*,
                job_templates.*,
                to_char(end_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as end_time,
                to_char(start_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as start_time
            FROM v_job_status
            JOIN job_templates
            ON v_job_status.tower_job_template_id = job_templates.tower_job_template_id
            WHERE tower_job_id = %s
        """,
            (tower_job_id,),
        )

        return cursor.fetchone()

    def get_job_task_callbacks(self, tower_job_id: int) -> list[JobTaskCallbacks]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT id, task_name, ansible_host, state, to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as timestamp
                    FROM task_callbacks
                    JOIN tasks
                    ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                    WHERE tower_job_id = %s
                """,
            (tower_job_id,),
        )

        return cursor.fetchall()

    def get_last_host_callback(self, tower_job_id: int) -> list[HostCallback]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT ON (ansible_host)
                id,
                task_name,
                ansible_host,
                state,
                to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as timestamp
            FROM task_callbacks
            JOIN tasks
            ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
            WHERE tower_job_id = %s
            ORDER BY ansible_host, task_callbacks.timestamp desc
        """,
            (tower_job_id,),
        )
        return cursor.fetchall()

    def get_last_host_callback_count(self, tower_job_id: int) -> dict[str, int]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT state, count(state) as "count"
            FROM (
                SELECT DISTINCT ON (ansible_host)
                    id,
                    task_name,
                    ansible_host,
                    state,
                    to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as timestamp
                FROM task_callbacks
                JOIN tasks
                ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                WHERE tower_job_id = %s
                ORDER BY ansible_host, task_callbacks.timestamp desc
            ) as host_states
            GROUP BY state
        """,
            (tower_job_id,),
        )
        return cursor.fetchall()

    def get_last_host_callback_task_count(self, tower_job_id: int) -> dict[str, int]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                 SELECT state, count(state) as "count"
                        FROM task_callbacks
                        JOIN tasks
                        ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                        WHERE tower_job_id = %s
                        GROUP BY state;
                    """,
            (tower_job_id,),
        )
        return cursor.fetchall()

    def get_job_info(self, tower_job_id: int) -> Optional[Job]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        jobs.tower_job_template_id,
                        jobs.awx_tags,
                        jobs.extra_vars::text,
                        jobs.artifacts::text,
                        jobs.tower_user_name,
                        jobs.tower_schedule_name,
                        jobs.tower_workflow_job_name,
                        jobs.ansible_limit,
                        jobs.start_time,
                        jobs.end_time,
                        job_templates.playbook_path
                    FROM jobs
                    JOIN job_templates ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                    WHERE tower_job_id = %s
                """,
            (tower_job_id,),
        )
        return cursor.fetchone()

    def get_workflow_job_info(self, tower_workflow_job_id: int) -> list[JobStatus]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        v_job_status.*,
                        job_templates.*,
                        to_char(start_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as start_time
                    FROM v_job_status
                    JOIN job_templates
                    ON v_job_status.tower_job_template_id = job_templates.tower_job_template_id
                    WHERE tower_workflow_job_id = %s
                    ORDER BY v_job_status.start_time asc
                """,
            (tower_workflow_job_id,),
        )

        return cursor.fetchall()

    def get_template(self, tower_job_template_id: int) -> Optional[Template]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT *
                    FROM job_templates
                    WHERE tower_job_template_id = %s
                """,
            (tower_job_template_id,),
        )
        return cursor.fetchone()

    def get_callback_data(self, task_callback_id: int) -> Optional[JobCallbackData]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        job_templates.tower_job_template_id,
                        tower_job_template_name,
                        jobs.tower_job_id,
                        job_templates.playbook_path,
                        task_name,
                        ansible_host,
                        state,
                        module,
                        to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as timestamp,
                        result_dump
                    FROM task_callbacks
                    JOIN tasks
                    ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                    JOIN jobs
                    ON jobs.tower_job_id = tasks.tower_job_id
                    JOIN job_templates
                    ON job_templates.tower_job_template_id = jobs.tower_job_template_id
                    WHERE task_callbacks.id = %s
                """,
            (task_callback_id,),
        )
        return cursor.fetchone()

    def get_all_host_compliance_state(self) -> list[HostComplianceStates]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
                    SELECT
                        *
                    FROM v_host_compliance
                """)

        return cursor.fetchall()

    def get_host_templates(self, ansible_host: str) -> list[HostTemplates]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT DISTINCT ON (o.tower_job_template_id)
                        o.tower_job_template_id,
                        o.tower_job_template_name,
                        awx_tags,
                        i.template_compliant AS compliant,
                        successful,
                        to_char(start_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS start_time,
                        o.compliance_interval,
                        tower_job_id
                    FROM v_host_templates AS o
                    join v_host_template_compliance AS i
                    ON o.tower_job_template_id = i.tower_job_template_id
                    AND o.ansible_host = i.ansible_host
                    WHERE o.ansible_host = %s
                    ORDER BY o.tower_job_template_id, awx_tags, start_time DESC
                """,
            (ansible_host,),
        )
        return cursor.fetchall()

    def get_host_jobs(self, ansible_host: str) -> list[dict]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        stats.*,
                        tower_job_template_name,
                        awx_tags,
                        to_char(start_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as start_time,
                        compliance_interval
                    FROM stats
                    JOIN jobs
                    ON stats.tower_job_id = jobs.tower_job_id
                    JOIN job_templates
                    ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                    WHERE ansible_host = %s
                """,
            (ansible_host,),
        )
        return cursor.fetchall()

    def get_host_last_callback(self, ansible_host: str) -> list[HostCallbackCounts]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT 
                        Tab1.*,
                        Tab2.id as last_callback_id
                    FROM
                        (
                            SELECT
                                stats.*,
                                tower_job_template_name,
                                awx_tags,
                                to_char(start_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as start_time,
                                compliance_interval
                            FROM 
                                stats
                                JOIN jobs
                                ON stats.tower_job_id = jobs.tower_job_id
                                JOIN job_templates
                                ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                            WHERE ansible_host = %s
                        ) as Tab1
                        JOIN
                        ( 
                            SELECT DISTINCT ON (tasks.tower_job_id)
                                id,
                                task_name,
                                ansible_host,
                                state,
                                to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as timestamp,
                                tower_job_id
                            FROM task_callbacks
                            JOIN tasks
                            ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                            WHERE task_callbacks.ansible_host = %s
                            ORDER BY tower_job_id, task_callbacks.timestamp desc
                        ) as Tab2
                        ON Tab1.tower_job_id = Tab2.tower_job_id
                """,
            (
                ansible_host,
                ansible_host,
            ),
        )
        return cursor.fetchall()

    def get_all_job_templates(self) -> list[JobTemplate]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
                    SELECT DISTINCT ON (job_templates.tower_job_template_id)
                        job_templates.tower_job_template_id,
                        tower_job_template_name,
                        playbook_path,
                        awx_organisation,
                        to_char(jobs.start_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as start_time,
                        compliance_interval,
                        (coalesce(array_length(v_template_compliance.noncompliant, 1), 0) = 0) AS compliant
                    FROM job_templates
                    JOIN jobs
                    ON job_templates.tower_job_template_id = jobs.tower_job_template_id
                    LEFT JOIN v_template_compliance
                    ON v_template_compliance.tower_job_template_id = job_templates.tower_job_template_id
                    ORDER BY job_templates.tower_job_template_id, start_time DESC
                """)

        return cursor.fetchall()

    def get_template_job_data(self, tower_job_template_id: int) -> list[TemplateJobData]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        *,
                        to_char(end_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as end_time,
                        to_char(start_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as start_time
                    FROM v_job_status
                    WHERE tower_job_template_id = %s
                """,
            (tower_job_template_id,),
        )
        return cursor.fetchall()

    def get_template_hosts_summary(self, tower_job_template_id: int) -> list[HostSummary]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                	    ansible_host,
                	    awx_tags,
                	    compliance_interval,
                	    compliant,
                        to_char(start_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as start_time,
                	    tower_job_id
                    FROM v_template_hosts_summary
                    WHERE tower_job_template_id = %s
                """,
            (tower_job_template_id,),
        )
        return cursor.fetchall()

    def get_last_host_callbacks_by_workflow_id(self, tower_workflow_job_id: int) -> list[HostCallback]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT DISTINCT ON (ansible_host)
                        id,
                        jobs.tower_job_id,
                        tower_job_template_name,
                        task_name,
                        ansible_host,
                        state,
                        to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as timestamp
                    FROM task_callbacks
                    JOIN tasks
                    ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                    JOIN jobs
                    ON jobs.tower_job_id = tasks.tower_job_id
                    JOIN job_templates
                    ON job_templates.tower_job_template_id = jobs.tower_job_template_id
                    WHERE tower_workflow_job_id = %s
                    ORDER BY ansible_host, task_callbacks.timestamp desc
                """,
            (tower_workflow_job_id,),
        )
        return cursor.fetchall()

    def get_last_host_callbacks_count_by_workflow_id(self, tower_workflow_job_id: int) -> dict:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT state, count(state)
                    FROM (
                        SELECT DISTINCT ON (ansible_host)
                            id,
                            task_name,
                            ansible_host,
                            state,
                            to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as timestamp
                        FROM task_callbacks
                        JOIN tasks
                        ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                        JOIN jobs
                        ON jobs.tower_job_id = tasks.tower_job_id
                        WHERE tower_workflow_job_id = %s
                        ORDER BY ansible_host, task_callbacks.timestamp desc
                    ) as host_states
                    GROUP BY state
                """,
            (tower_workflow_job_id,),
        )
        return cursor.fetchall()

    def get_all_workflow_jobs(self) -> list[dict]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
                    SELECT
                        tower_workflow_job_id,
                        tower_workflow_job_name,
                        count(*) as count_jobs,
                        to_char(MIN(start_time), 'YYYY-MM-DD"T"HH24:MI:SS.MS') as start_time,
                        array_agg(DISTINCT awx_tags) as awx_tags,
                        array_agg(DISTINCT ansible_limit) as ansible_limit
                    FROM jobs
                    WHERE tower_workflow_job_id IS NOT NULL
                    GROUP BY
                        tower_workflow_job_id,
                        tower_workflow_job_name
                    ORDER BY MIN(start_time) desc
                """)
        ret = cursor.fetchall()
        for line in ret:
            line["awx_tags"] = [t for tags in line["awx_tags"] for t in tags]
        return ret

    def get_workflow_job_stats(self, tower_workflow_job_id: int) -> dict:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT stats.*, tower_job_template_name 
                    FROM stats
                    JOIN jobs
                    ON stats.tower_job_id = jobs.tower_job_id
                    JOIN job_templates
                    ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                    WHERE tower_workflow_job_id = %s
                """,
            (tower_workflow_job_id,),
        )
        return cursor.fetchall()

    def get_workflow_callbacks(self, tower_workflow_job_id: int) -> dict:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT 
                        id,
                        jobs.tower_job_id,
                        tower_job_template_name,
                        task_name,
                        ansible_host,
                        state,
                        to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as timestamp
                    FROM task_callbacks
                    JOIN tasks
                    ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                    JOIN jobs
                    ON tasks.tower_job_id = jobs.tower_job_id
                    JOIN job_templates
                    ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                    WHERE tower_workflow_job_id = %s
                """,
            (tower_workflow_job_id,),
        )
        return cursor.fetchall()

    def get_workflow_callbacks_count(self, tower_workflow_job_id: int) -> int:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                    SELECT
                        state,
                        count(state) as "count"
                    FROM task_callbacks
                    JOIN tasks
                    ON tasks.ansible_uuid = task_callbacks.task_ansible_uuid
                    JOIN jobs
                    ON tasks.tower_job_id = jobs.tower_job_id
                    JOIN job_templates
                    ON jobs.tower_job_template_id = job_templates.tower_job_template_id
                    WHERE tower_workflow_job_id = %s
                    GROUP BY state
                    """,
            (tower_workflow_job_id,),
        )
        return cursor.fetchall()

    def get_last_jobs_by_days(self, days: int) -> list[Job]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                            SELECT
                                v_job_status.*,
                                job_templates.*,
                                to_char(end_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as end_time,
                                to_char(start_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as start_time
                            FROM v_job_status
                            JOIN job_templates
                            ON v_job_status.tower_job_template_id = job_templates.tower_job_template_id
                            WHERE start_time > NOW() - INTERVAL '%s DAYS'
                        """,
            (days,),
        )
        return cursor.fetchall()

    def get_compliant_non_compliant_stats(self) -> CompliantHostStats:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
                    SELECT
                        count(*) FILTER (WHERE compliant = True) as compliant,
                        count(*) FILTER (WHERE compliant = False) as not_compliant
                    FROM v_host_compliance;
                """)
        return cursor.fetchone()
