import json
from datetime import datetime, timedelta
from re import findall
from time import sleep
from typing import Any, Dict, Optional

import pytest

from lufa.database import TimeStamp
from lufa.repository.api_repository import ApiRepository
from lufa.repository.backend_repository import BackendRepository, HostCallback


class TestBackendRepository:
    # Testdata:
    tower_job_template_id = 1001
    tower_job_id = 2001
    tower_schedule_id = 3001
    tower_workflow_job_id = 4001
    task_callbacks_id = 1
    ansible_host = "AnsHost1"
    task_ansible_uuid = "873ea0ee-b9d7-49f2-b658-bb5aa8de501b"
    task_ansible_uuid2 = "bb5aa8de501b-b9d7-49f2-b658-873ea0ee"

    template_infos = {"test": 123, "test2": "test"}

    @pytest.fixture(autouse=True)
    def simple_api_repository(self, api_repository: ApiRepository) -> None:
        api_repository.add_job(
            self.tower_job_id,
            self.tower_job_template_id,
            "template 1 Name",
            ["tag1", "tag2"],
            r"{}",
            r"{}",
            "ans Limit",
            "towerUser",
            self.tower_schedule_id,
            "SchedName",
            self.tower_workflow_job_id,
            "WorkflowJobName",
            0,
            awx_organisation="awxOrg1",
            template_infos=json.dumps(self.template_infos),
        )
        api_repository.add_stats(
            self.tower_job_id,
            [
                {
                    "ansible_host": "AnsHost1",
                    "ok": 200,
                    "failed": 0,
                    "unreachable": 0,
                    "changed": 202,
                    "skipped": 410,
                    "rescued": 201,
                    "ignored": 418,
                }
            ],
        )
        api_repository.add_task(self.task_ansible_uuid, self.tower_job_id, "TaskName")
        api_repository.add_callback(self.task_ansible_uuid, "AnsHost1", "ok", "{}", "module")
        api_repository.add_job(
            2002,
            1002,
            "template 2 Name",
            ["tag3", "tag4"],
            r"{}",
            r"{}",
            "ans Limit",
            "towerUser",
            3001,
            "SchedName",
            4001,
            "WorkflowJobName",
            7,
            awx_organisation="awxOrg1",
            template_infos=json.dumps("TemplInfos 2"),
        )
        api_repository.add_stats(
            2002,
            [
                {
                    "ansible_host": "AnsHost1",
                    "ok": 200,
                    "failed": 400,
                    "unreachable": 404,
                    "changed": 202,
                    "skipped": 410,
                    "rescued": 201,
                    "ignored": 418,
                }
            ],
        )
        api_repository.add_task(self.task_ansible_uuid2, 2002, "TaskName")
        api_repository.add_callback(self.task_ansible_uuid, "AnsHost1", "ok", "{}", "module")

    @pytest.fixture
    def task_api_repository(
        self, api_repository: ApiRepository, backend_repository: BackendRepository
    ) -> BackendRepository:
        api_repository.add_task("833b47ac-07f0-4229-904b-c4ef788f1fca", 2001, "TaskName 2")
        api_repository.add_task("e3e7b1bf-1021-4f75-99bf-c635689c875e", 2001, "TaskName 3")
        api_repository.add_callback("e3e7b1bf-1021-4f75-99bf-c635689c875e", "AnsHost2", "failed", "{}", "module")
        # Damit es einen zetilichen Unterschied zwischen den Daten gibt
        sleep(1)
        api_repository.add_callback("833b47ac-07f0-4229-904b-c4ef788f1fca", "AnsHost1", "ok", "{}", "module")
        return backend_repository

    def test_get_job_template_name_by_job_id(self, backend_repository: BackendRepository):
        ret = backend_repository.get_job_template_name_by_job_id(self.tower_job_id)
        assert ret == "template 1 Name"

    def test_get_job_template_name_by_template_id(self, backend_repository: BackendRepository):
        ret = backend_repository.get_job_template_name_by_template_id(self.tower_job_template_id)
        assert ret == "template 1 Name"

    def test_get_workflow_job_name(self, backend_repository: BackendRepository):
        ret = backend_repository.get_workflow_job_name(self.tower_workflow_job_id)
        assert ret == "WorkflowJobName"

    def test_get_job_stats(self, backend_repository: BackendRepository):
        ret = backend_repository.get_job_stats(self.tower_job_id)
        d: Dict[str, Any] = {}
        d["ansible_host"] = "AnsHost1"
        d["changed"] = 202
        d["failed"] = 0
        d["ok"] = 200
        d["tower_job_id"] = self.tower_job_id
        d["unreachable"] = 0
        d["skipped"] = 410
        d["rescued"] = 201
        d["ignored"] = 418
        assert ret == [d]

    def test_get_job_status(self, backend_repository: BackendRepository):
        ret = backend_repository.get_job_status(self.tower_job_id)
        assert ret["ansible_limit"] == "ans Limit"
        assert ret["awx_organisation"] == "awxOrg1"
        assert ret["awx_tags"] == ["tag1", "tag2"]
        assert ret["end_time"] is None
        assert ret["compliance_interval"] == 0
        assert check_delta(ret["start_time"])

        assert ret["state"] == "started"
        assert ret["template_infos"] == self.template_infos
        assert ret["tower_job_id"] == 2001
        assert ret["tower_job_template_id"] == 1001
        assert ret["tower_job_template_name"] == "template 1 Name"
        assert ret["tower_schedule_id"] == 3001
        assert ret["tower_schedule_name"] == "SchedName"
        assert ret["tower_user_name"] == "towerUser"
        assert ret["tower_workflow_job_id"] == 4001
        assert ret["tower_workflow_job_name"] == "WorkflowJobName"

    def test_get_job_task_callbacks(self, task_api_repository: BackendRepository):
        ret = task_api_repository.get_job_task_callbacks(self.tower_job_id)

        line = ret[0]
        assert len(line) == 5
        assert line["ansible_host"] == "AnsHost1"
        assert line["id"] == 1
        assert line["state"] == "ok"
        assert line["task_name"] == "TaskName"
        assert check_delta(line["timestamp"])

    def test_get_last_host_callback(self, task_api_repository: BackendRepository):
        ret = task_api_repository.get_last_host_callback(self.tower_job_id)

        line = ret[0]
        assert len(ret) == 2
        assert line["ansible_host"] == "AnsHost1"
        assert line["id"] == 4
        assert line["state"] == "ok"
        assert line["task_name"] == "TaskName 2"
        assert check_delta(line["timestamp"])

    def test_get_last_host_callback_count(self, task_api_repository: BackendRepository):
        ret = task_api_repository.get_last_host_callback_count(self.tower_job_id)
        li = []
        d1: dict[str, Any] = {}
        d1["count"] = 1
        d1["state"] = "failed"
        li.append(d1)
        d2: dict[str, Any] = {}
        d2["count"] = 1
        d2["state"] = "ok"
        li.append(d2)
        assert ret == li

    def test_get_job_info(self, backend_repository: BackendRepository):
        ret = backend_repository.get_job_info(self.tower_job_id)
        assert ret is not None
        d: dict[str, Any] = {
            "awx_tags": ["tag1", "tag2"],
            "tower_job_template_id": 1001,
            "tower_schedule_name": "SchedName",
            "extra_vars": "{}",
            "artifacts": "{}",
            "tower_user_name": "towerUser",
            "tower_workflow_job_name": "WorkflowJobName",
            "ansible_limit": "ans Limit",
            "end_time": None,
            "playbook_path": None,
            "start_time": ret["start_time"],  # start time wird nicht betrachtet # pyright: ignore[reportTypedDictNotRequiredAccess]
        }
        assert ret == d

    def test_get_workflow_job_info(self, backend_repository: BackendRepository):
        ret = backend_repository.get_workflow_job_info(self.tower_workflow_job_id)
        d = ret[0]
        assert len(ret) == 2
        assert d["ansible_limit"] == "ans Limit"
        assert d["awx_organisation"] == "awxOrg1"
        assert d["awx_tags"] == ["tag1", "tag2"]
        assert check_delta(d["start_time"])
        assert d["end_time"] is None
        assert d["compliance_interval"] == 0
        assert d["state"] == "started"
        assert d["template_infos"] == self.template_infos
        assert d["tower_job_id"] == 2001
        assert d["tower_job_template_id"] == 1001
        assert d["tower_job_template_name"] == "template 1 Name"
        assert d["tower_schedule_id"] == 3001
        assert d["tower_schedule_name"] == "SchedName"
        assert d["tower_user_name"] == "towerUser"
        assert d["tower_workflow_job_id"] == 4001
        assert d["tower_workflow_job_name"] == "WorkflowJobName"

    def test_get_template(self, backend_repository: BackendRepository):
        ret = backend_repository.get_template(self.tower_job_template_id)
        # Prüfe ob template_infos in ret enthalten ist
        assert ret is not None
        assert ret["template_infos"] == TestBackendRepository.template_infos

    def test_get_callback_data(self, backend_repository: BackendRepository):
        ret = backend_repository.get_callback_data(self.task_callbacks_id)
        assert ret is not None
        assert ret["ansible_host"] == "AnsHost1"
        assert ret["result_dump"] == {}
        assert ret["state"] == "ok"
        assert ret["task_name"] == "TaskName"
        assert check_delta(ret["timestamp"])
        assert ret["tower_job_id"] == 2001
        assert ret["tower_job_template_id"] == 1001
        assert ret["tower_job_template_name"] == "template 1 Name"

    def test_get_all_host_compliance_state(self, backend_repository: BackendRepository):
        ret = backend_repository.get_all_host_compliance_state()
        line = ret[0]
        assert len(ret) == 1
        d: dict[str, Any] = {}
        d["ansible_host"] = "AnsHost1"
        d["compliant"] = 0
        assert line == d

    def test_get_host_templates(self, backend_repository: BackendRepository):
        actual = backend_repository.get_host_templates(self.ansible_host)
        d: list[dict[str, Any]] = [dict(line) for line in actual]
        for line in d:
            del line["start_time"]
        assert d == [
            {
                "tower_job_template_id": 1001,
                "tower_job_template_name": "template 1 Name",
                "awx_tags": ["tag1", "tag2"],
                "compliant": True,
                "successful": True,
                "compliance_interval": 0,
                "tower_job_id": 2001,
            },
            {
                "tower_job_template_id": 1002,
                "tower_job_template_name": "template 2 Name",
                "awx_tags": ["tag3", "tag4"],
                "compliant": False,
                "successful": False,
                "compliance_interval": 7,
                "tower_job_id": 2002,
            },
        ]

    def test_get_host_jobs(self, backend_repository: BackendRepository):
        ret = backend_repository.get_host_jobs(self.ansible_host)
        line = ret[0]
        assert len(ret) == 2
        assert line["ansible_host"] == "AnsHost1"
        assert line["awx_tags"] == ["tag1", "tag2"]
        assert line["changed"] == 202
        assert line["failed"] == 0
        assert line["ignored"] == 418
        assert line["ok"] == 200
        assert line["rescued"] == 201
        assert line["compliance_interval"] == 0
        assert line["skipped"] == 410
        assert check_delta(line["start_time"])
        assert line["tower_job_id"] == 2001
        assert line["tower_job_template_name"] == "template 1 Name"
        assert line["unreachable"] == 0

    def test_get_host_last_callback(self, task_api_repository: BackendRepository):
        ret = task_api_repository.get_host_last_callback(self.ansible_host)
        assert ret is not None
        line = ret[0]
        assert len(ret) == 1
        assert line["ansible_host"] == "AnsHost1"
        assert line["awx_tags"] == ["tag1", "tag2"]
        assert line["changed"] == 202
        assert line["failed"] == 0
        assert line["ignored"] == 418
        assert line["last_callback_id"] == 4
        assert line["ok"] == 200
        assert line["rescued"] == 201
        assert line["compliance_interval"] == 0
        assert line["skipped"] == 410
        assert check_delta(line["start_time"])
        assert line["tower_job_id"] == 2001
        assert line["tower_job_template_name"] == "template 1 Name"
        assert line["unreachable"] == 0

    def test_get_all_job_templates(self, backend_repository: BackendRepository):
        ret = backend_repository.get_all_job_templates()
        assert ret is not None
        line = ret[0]
        assert len(ret) == 2
        assert line["awx_organisation"] == "awxOrg1"
        assert check_delta(line["start_time"])
        assert line["tower_job_template_id"] == 1001
        assert line["tower_job_template_name"] == "template 1 Name"

    def test_get_template_job_data(self, backend_repository: BackendRepository):
        ret = backend_repository.get_template_job_data(self.tower_job_template_id)
        assert ret is not None
        line = ret[0]
        assert len(ret) == 1
        assert line["ansible_limit"] == "ans Limit"
        assert line["awx_tags"] == ["tag1", "tag2"]
        assert line["end_time"] is None
        assert check_delta(line["start_time"])
        assert line["state"] == "started"
        assert line["tower_job_id"] == 2001
        assert line["tower_job_template_id"] == 1001
        assert line["tower_schedule_id"] == 3001
        assert line["tower_schedule_name"] == "SchedName"
        assert line["tower_user_name"] == "towerUser"
        assert line["tower_workflow_job_id"] == 4001
        assert line["tower_workflow_job_name"] == "WorkflowJobName"

    def test_get_template_hosts_summary(self, backend_repository: BackendRepository):
        ret = backend_repository.get_template_hosts_summary(self.tower_job_template_id)
        assert ret is not None
        line = ret[0]
        assert len(ret) == 1
        assert line["ansible_host"] == "AnsHost1"
        assert line["awx_tags"] == ["tag1", "tag2"]
        assert line["compliant"]
        assert type(line["compliant"]) is bool
        assert type(ret[0]["start_time"]) is TimeStamp
        assert len(findall(r"^20\d\d-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d.\d\d\d$", ret[0]["start_time"])) == 1, ret[
            0
        ]["start_time"]
        assert line["compliance_interval"] == 0
        assert check_delta(line["start_time"])
        assert line["tower_job_id"] == 2001

    def test_get_last_host_callbacks_by_workflow_id(self, task_api_repository: BackendRepository):
        ret = task_api_repository.get_last_host_callbacks_by_workflow_id(self.tower_workflow_job_id)
        assert ret is not None
        assert len(ret) == 2
        line: HostCallback = ret[-1]
        assert line["task_name"] == "TaskName 3"
        assert line["ansible_host"] == "AnsHost2"
        assert line["id"] == 3
        assert line["state"] == "failed"
        assert check_delta(line["timestamp"])
        assert line["tower_job_id"] == 2001
        assert line["tower_job_template_name"] == "template 1 Name"
        line = ret[0]
        assert line["task_name"] == "TaskName 2"

    def test_get_last_host_callbacks_count_by_workflow_id(self, backend_repository: BackendRepository):
        ret = backend_repository.get_last_host_callbacks_count_by_workflow_id(self.tower_workflow_job_id)
        assert ret is not None
        line = ret[0]
        assert len(ret) == 1
        assert line == {"count": 1, "state": "ok"}

    def test_get_all_workflow_jobs(self, backend_repository: BackendRepository):
        actual = backend_repository.get_all_workflow_jobs()
        d = [dict(line) for line in actual]
        for line in d:
            del line["start_time"]
        assert d == [
            {
                "tower_workflow_job_id": 4001,
                "tower_workflow_job_name": "WorkflowJobName",
                "count_jobs": 2,
                "awx_tags": ["tag1", "tag2", "tag3", "tag4"],
                "ansible_limit": ["ans Limit"],
            }
        ]

    def test_get_workflow_job_stats(self, backend_repository: BackendRepository):
        ret = backend_repository.get_workflow_job_stats(self.tower_workflow_job_id)
        assert ret is not None
        line = ret[0]
        d: dict[str, Any] = {}
        d["ansible_host"] = "AnsHost1"
        d["changed"] = 202
        d["failed"] = 0
        d["ignored"] = 418
        d["ok"] = 200
        d["rescued"] = 201
        d["skipped"] = 410
        d["tower_job_id"] = 2001
        d["tower_job_template_name"] = "template 1 Name"
        d["unreachable"] = 0
        assert line == d

    def test_get_workflow_callbacks(self, backend_repository: BackendRepository):
        ret = backend_repository.get_workflow_callbacks(self.tower_workflow_job_id)
        assert ret is not None
        line = ret[0]
        assert line["ansible_host"] == "AnsHost1"
        assert line["id"] == 1
        assert line["state"] == "ok"
        assert line["task_name"] == "TaskName"
        assert check_delta(line["timestamp"])
        assert line["tower_job_id"] == 2001
        assert line["tower_job_template_name"] == "template 1 Name"

    def test_get_last_jobs_by_days(self, backend_repository: BackendRepository):
        ret = backend_repository.get_last_jobs_by_days(7)
        assert ret is not None
        line = ret[0]
        assert line["ansible_limit"] == "ans Limit"
        assert line["awx_organisation"] == "awxOrg1"  # pyright: ignore[reportTypedDictNotRequiredAccess]
        assert line["awx_tags"] == ["tag1", "tag2"]
        assert line["end_time"] is None  # pyright: ignore[reportTypedDictNotRequiredAccess]
        assert line["compliance_interval"] == 0
        assert check_delta(line["start_time"])  # pyright: ignore[reportTypedDictNotRequiredAccess]
        assert line["state"] == "started"
        assert line["template_infos"] == TestBackendRepository.template_infos  # pyright: ignore[reportTypedDictNotRequiredAccess]
        assert line["tower_job_id"] == 2001
        assert line["tower_job_template_id"] == 1001
        assert line["tower_job_template_name"] == "template 1 Name"
        assert line["tower_schedule_id"] == 3001  # pyright: ignore[reportTypedDictNotRequiredAccess]
        assert line["tower_schedule_name"] == "SchedName"  # pyright: ignore[reportTypedDictNotRequiredAccess]
        assert line["tower_user_name"] == "towerUser"
        assert line["tower_workflow_job_id"] == 4001  # pyright: ignore[reportTypedDictNotRequiredAccess]
        assert line["tower_workflow_job_name"] == "WorkflowJobName"  # pyright: ignore[reportTypedDictNotRequiredAccess]

    @pytest.fixture
    def with_end_time(self, api_repository: ApiRepository) -> ApiRepository:
        api_repository.update_job(self.tower_job_id, end_time=None, artifacts="{}")
        return api_repository

    def test_get_last_jobs_with_end_time(self, backend_repository: BackendRepository, with_end_time: ApiRepository):
        assert with_end_time is not None

        ret = backend_repository.get_last_jobs_by_days(7)

        ret.sort(key=lambda j: j["tower_job_id"])
        assert len(ret) == 2
        line = ret[0]
        assert line["tower_job_id"] == self.tower_job_id
        assert check_delta(line["end_time"])  # pyright: ignore[reportTypedDictNotRequiredAccess]

    def test_get_job_status_with_end_time(self, backend_repository: BackendRepository, with_end_time: ApiRepository):
        assert with_end_time is not None

        ret = backend_repository.get_job_status(self.tower_job_id)

        assert check_delta(ret["end_time"])  # pyright: ignore[reportTypedDictNotRequiredAccess]

    def test_get_compliant_non_compliant_stats(self, backend_repository: BackendRepository):
        ret = backend_repository.get_compliant_non_compliant_stats()
        assert ret is not None
        assert ret == {"compliant": 0, "not_compliant": 1}


def check_delta(dt: Optional[TimeStamp]) -> bool:
    assert dt is not None
    assert type(dt) is TimeStamp
    assert len(findall(r"^20\d\d-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d\d\d$", dt)) == 1, dt

    if dt is None:
        return False
    db_now = datetime.fromisoformat(dt) + timedelta(hours=1)
    delta = datetime.now() - db_now
    tdelta = timedelta(seconds=10)
    return delta <= tdelta
