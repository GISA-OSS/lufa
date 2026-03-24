import pytest

from lufa.repository.api_repository import ApiRepository, JobExport
from lufa.repository.backend_repository import ResourceNotFoundError
from tests.integration.conftest import HostIntependantTowerJobStats, LufaFactory

HOST1 = "host1.example.com"
HOST2 = "host2.example.com"


class TestExportJob:
    """Test exporting a job from API repository"""

    def test_export_nonexistent_job_raises_error(self, api_repository: ApiRepository):
        """Test that exporting a non-existent job raises ResourceNotFoundError"""
        with pytest.raises(ResourceNotFoundError, match="Job with id 99999 not found"):
            api_repository.export_job(99999)

    def test_export_job_with_tasks_and_callbacks(
        self,
        api_repository: ApiRepository,
        lufa_factory: LufaFactory,
        single_any_stat: HostIntependantTowerJobStats,
    ):
        """Test export of a complete job with tasks and callbacks"""

        job = lufa_factory.add_tower_template().add_job().with_stats(HOST1, single_any_stat).with_end_time()

        # add tasks
        task1_uuid = "aaaaaaaa-1111-1111-1111-111111111111"
        task2_uuid = "bbbbbbbb-2222-2222-2222-222222222222"
        api_repository.add_task(task1_uuid, job.tower_job_id, "Install packages")
        api_repository.add_task(task2_uuid, job.tower_job_id, "Configure service")

        # add callbacks for task1
        api_repository.add_callback(
            task_ansible_uuid=task1_uuid,
            ansible_host=HOST1,
            state="ok",
            module="apt",
            result_dump='{"changed": true}',
        )

        # add callbacks for task2
        api_repository.add_callback(
            task_ansible_uuid=task2_uuid,
            ansible_host=HOST1,
            state="ok",
            module="template",
            result_dump='{"changed": false}',
        )

        result = api_repository.export_job(job.tower_job_id)

        # verify tasks
        assert len(result["tasks"]) == 2
        task_names = [t["task_name"] for t in result["tasks"]]
        assert "Install packages" in task_names
        assert "Configure service" in task_names

        # verify callbacks
        for task in result["tasks"]:
            if task["task_name"] == "Install packages":
                assert len(task["callbacks"]) == 1
                assert task["callbacks"][0]["ansible_host"] == HOST1
                assert task["callbacks"][0]["state"] == "ok"
                assert task["callbacks"][0]["module"] == "apt"
                assert "timestamp" in task["callbacks"][0]


class TestImportJob:
    """Test importing a job from API repository"""

    @pytest.fixture
    def import_data(self):
        new_job_id = 99000
        new_template_id = 88000
        task1_uuid = "aaaaaaaa-1111-2222-3333-aaaaaaaaaaaa"
        task2_uuid = "bbbbbbbb-1111-2222-3333-bbbbbbbbbbbb"
        task3_uuid = "cccccccc-1111-2222-3333-cccccccccccc"

        return {
            "exported_at": "2026-03-23T10:00:00",
            "job": {
                "tower_job_id": new_job_id,
                "tower_job_template_id": new_template_id,
                "tower_job_template_name": "Import Test Template",
                "ansible_limit": "*.example.com",
                "tower_user_name": "importuser",
                "awx_tags": ["import", "test"],
                "extra_vars": '{"var1": "value1"}',
                "artifacts": '{"key": "value"}',
                "tower_schedule_id": 12345,
                "tower_schedule_name": "Daily Schedule",
                "tower_workflow_job_id": 67890,
                "tower_workflow_job_name": "Import Workflow",
                "start_time": "2026-03-23T09:00:00.123",
                "end_time": "2026-03-23T09:30:00.123",
                "state": "success",
            },
            "job_template": {
                "tower_job_template_id": new_template_id,
                "tower_job_template_name": "Import Test Template",
                "playbook_path": "import_playbook.yml",
                "compliance_interval": 7,
                "awx_organisation": "ImportOrg",
                "template_infos": None,
            },
            "stats": [
                {
                    "ansible_host": HOST1,
                    "ok": 10,
                    "failed": 0,
                    "unreachable": 0,
                    "changed": 5,
                    "skipped": 2,
                    "rescued": 0,
                    "ignored": 0,
                },
                {
                    "ansible_host": HOST2,
                    "ok": 8,
                    "failed": 0,
                    "unreachable": 0,
                    "changed": 3,
                    "skipped": 1,
                    "rescued": 0,
                    "ignored": 0,
                },
            ],
            "tasks": [
                {
                    "ansible_uuid": task1_uuid,
                    "task_name": "Setup environment",
                    "callbacks": [
                        {
                            "ansible_host": HOST1,
                            "state": "ok",
                            "module": "setup",
                            "timestamp": "2026-03-23T09:05:00.123",
                            "result_dump": '{"ansible_facts": {"os_family": "Debian"}}',
                        },
                        {
                            "ansible_host": HOST2,
                            "state": "ok",
                            "module": "setup",
                            "timestamp": "2026-03-23T09:05:01.123",
                            "result_dump": '{"ansible_facts": {"os_family": "RedHat"}}',
                        },
                    ],
                },
                {
                    "ansible_uuid": task2_uuid,
                    "task_name": "Install packages",
                    "callbacks": [
                        {
                            "ansible_host": HOST1,
                            "state": "ok",
                            "module": "apt",
                            "timestamp": "2026-03-23T09:10:00.123",
                            "result_dump": '{"changed": true, "packages": ["nginx", "postgresql"]}',
                        },
                        {
                            "ansible_host": HOST2,
                            "state": "ok",
                            "module": "yum",
                            "timestamp": "2026-03-23T09:10:01.123",
                            "result_dump": '{"changed": true, "packages": ["nginx", "postgresql"]}',
                        },
                    ],
                },
                {
                    "ansible_uuid": task3_uuid,
                    "task_name": "Configure services",
                    "callbacks": [
                        {
                            "ansible_host": HOST1,
                            "state": "ok",
                            "module": "systemd",
                            "timestamp": "2026-03-23T09:15:00.123",
                            "result_dump": '{"changed": false, "status": {"running": true}}',
                        },
                        {
                            "ansible_host": HOST2,
                            "state": "changed",
                            "module": "systemd",
                            "timestamp": "2026-03-23T09:15:01.123",
                            "result_dump": '{"changed": true, "status": {"running": true}}',
                        },
                    ],
                },
            ],
        }

    def test_reexporting_is_mostly_invariant(self, api_repository: ApiRepository, import_data: JobExport):
        """Test exporting a complete job with tasks and callbacks an imported returns the original json with only specified possible changes."""

        # import the job
        imported_job_id = api_repository.import_job(import_data)
        reexport = api_repository.export_job(imported_job_id)

        assert import_data["exported_at"] < reexport["exported_at"]
        import_data["tasks"].sort(key=lambda x: x["ansible_uuid"])
        reexport["tasks"].sort(key=lambda x: x["ansible_uuid"])
        masked = {"exported_at": "masked"}
        assert {**reexport, **masked} == {**import_data, **masked}

    def test_import_existing_job_fails(
        self,
        api_repository: ApiRepository,
        lufa_factory: LufaFactory,
    ):
        """Test that importing a job with an ID that already exists raises an error."""
        # create an existing job
        existing_job = lufa_factory.add_tower_template().add_job()

        # manually build import data with the same job ID
        import_data: JobExport = {
            "exported_at": "2026-03-23T10:00:00",
            "job": {
                "tower_job_id": existing_job.tower_job_id,  # Same ID as existing job
                "tower_job_template_id": 100,
                "tower_job_template_name": "Test Template",
                "ansible_limit": "*.example.com",
                "tower_user_name": "testuser",
                "awx_tags": ["tag1"],
                "extra_vars": "{}",
                "artifacts": "{}",
                "tower_schedule_id": 42,
                "tower_schedule_name": "abc",
                "tower_workflow_job_id": 42,
                "tower_workflow_job_name": "abc",
                "start_time": "2026-03-23T09:00:00",
                "end_time": None,
                "state": "started",
            },
            "job_template": {
                "tower_job_template_id": 100,
                "tower_job_template_name": "Test Template",
                "playbook_path": "test.yml",
                "compliance_interval": 0,
                "awx_organisation": "Default",
                "template_infos": None,
            },
            "stats": [],
            "tasks": [],
        }

        # try to import with the same tower_job_id - should fail
        with pytest.raises((ValueError, Exception)) as exc_info:
            api_repository.import_job(import_data)

        # verify error message mentions the job already exists
        assert "already exists" in str(exc_info.value).lower()

    def test_import_job_with_tasks_and_callbacks(
        self,
        api_repository: ApiRepository,
        import_data: JobExport,
    ):
        """Test importing a complete job with tasks and callbacks."""

        # import the job
        imported_job_id = api_repository.import_job(import_data)

        assert imported_job_id == import_data["job"]["tower_job_id"]

        # verify job exists
        assert api_repository.job_exists(imported_job_id)

        # verify tasks were imported
        assert api_repository.tasks_exists(import_data["tasks"][0]["ansible_uuid"])
        assert api_repository.tasks_exists(import_data["tasks"][1]["ansible_uuid"])
        assert api_repository.tasks_exists(import_data["tasks"][2]["ansible_uuid"])

    def test_export_job_with_tasks_and_callbacks(
        self,
        api_repository: ApiRepository,
        api_repository_to_backend: ApiRepository,
        lufa_factory: LufaFactory,
        single_any_stat: HostIntependantTowerJobStats,
    ):
        """Test export of a complete job with tasks and callbacks"""

        job = lufa_factory.add_tower_template().add_job().with_stats(HOST1, single_any_stat).with_end_time()

        # add tasks
        task1_uuid = "aaaaaaaa-1111-1111-1111-111111111111"
        task2_uuid = "bbbbbbbb-2222-2222-2222-222222222222"
        api_repository.add_task(task1_uuid, job.tower_job_id, "Install packages")
        api_repository.add_task(task2_uuid, job.tower_job_id, "Configure service")

        # add callbacks for task1
        api_repository.add_callback(
            task_ansible_uuid=task1_uuid,
            ansible_host=HOST1,
            state="ok",
            module="apt",
            result_dump='{"changed": true}',
        )

        # add callbacks for task2
        api_repository.add_callback(
            task_ansible_uuid=task2_uuid,
            ansible_host=HOST1,
            state="ok",
            module="template",
            result_dump='{"changed": false}',
        )

        initial_export = api_repository.export_job(job.tower_job_id)
        api_repository_to_backend.import_job(initial_export)
        reexport = api_repository_to_backend.export_job(job.tower_job_id)

        assert initial_export["exported_at"] <= reexport["exported_at"]
        initial_export["tasks"].sort(key=lambda x: x["ansible_uuid"])
        reexport["tasks"].sort(key=lambda x: x["ansible_uuid"])
        masked = {"exported_at": "masked"}
        assert {**reexport, **masked} == {**initial_export, **masked}
