from __future__ import annotations

import uuid

from lufa.repository.backend_repository import BackendRepository
from tests.integration.conftest import HostIntependantTowerJobStats, LufaFactory


def _all_zero_stats() -> HostIntependantTowerJobStats:
    return {
        "ok": 0,
        "failed": 0,
        "unreachable": 0,
        "changed": 0,
        "skipped": 0,
        "rescued": 0,
        "ignored": 0,
    }


class TestGetJobsCombined:
    def test_get_jobs_by_ids_returns_all_requested(
        self, backend_repository: BackendRepository, lufa_factory: LufaFactory
    ):
        job1 = lufa_factory.add_tower_template().add_job()
        job2 = lufa_factory.add_tower_template().add_job()
        job3 = lufa_factory.add_tower_template().add_job()

        ret = backend_repository.get_jobs_by_ids([job1.tower_job_id, job2.tower_job_id])

        assert {r["tower_job_id"] for r in ret} == {job1.tower_job_id, job2.tower_job_id}
        assert all("tower_job_template_name" in r for r in ret)
        assert job3.tower_job_id not in {r["tower_job_id"] for r in ret}

    def test_get_job_stats_by_ids(
        self,
        backend_repository: BackendRepository,
        lufa_factory: LufaFactory,
    ):
        job1 = lufa_factory.add_tower_template().add_job()
        job2 = lufa_factory.add_tower_template().add_job()
        ok_stats: HostIntependantTowerJobStats = {**_all_zero_stats(), "ok": 1}
        job1.with_stats("h1", ok_stats)
        job2.with_stats("h2", ok_stats)

        ret = backend_repository.get_job_stats_by_ids([job1.tower_job_id, job2.tower_job_id])

        assert {(r["tower_job_id"], r["ansible_host"]) for r in ret} == {
            (job1.tower_job_id, "h1"),
            (job2.tower_job_id, "h2"),
        }

    def test_get_last_host_callback_by_ids_unique_per_job_host(
        self, backend_repository: BackendRepository, lufa_factory: LufaFactory
    ):
        job = lufa_factory.add_tower_template().add_job()
        task = str(uuid.uuid4())
        lufa_factory.api_repository.add_task(task, job.tower_job_id, "task1")
        lufa_factory.api_repository.add_callback(task, "hostA", "ok", "{}", "setup")
        # a second callback on the same host+job should be superseded by the first (last per host)
        lufa_factory.api_repository.add_callback(task, "hostB", "failed", "{}", "setup")

        ret = backend_repository.get_last_host_callback_by_ids([job.tower_job_id])

        rows = [(r["tower_job_id"], r["ansible_host"]) for r in ret]
        assert len(rows) == len(set(rows))
        assert (job.tower_job_id, "hostA") in rows
        assert (job.tower_job_id, "hostB") in rows

    def test_get_job_task_callbacks_by_ids(self, backend_repository: BackendRepository, lufa_factory: LufaFactory):
        job1 = lufa_factory.add_tower_template().add_job()
        job2 = lufa_factory.add_tower_template().add_job()
        task1 = str(uuid.uuid4())
        task2 = str(uuid.uuid4())
        lufa_factory.api_repository.add_task(task1, job1.tower_job_id, "task1")
        lufa_factory.api_repository.add_task(task2, job2.tower_job_id, "task2")
        lufa_factory.api_repository.add_callback(task1, "hostA", "ok", "{}", "setup")
        lufa_factory.api_repository.add_callback(task2, "hostB", "failed", "{}", "setup")

        ret = backend_repository.get_job_task_callbacks_by_ids([job1.tower_job_id, job2.tower_job_id])

        assert {r["task_name"] for r in ret} == {"task1", "task2"}
        assert {r["tower_job_id"] for r in ret} == {job1.tower_job_id, job2.tower_job_id}

    def test_get_job_task_callback_count_by_ids_sums_over_jobs(
        self, backend_repository: BackendRepository, lufa_factory: LufaFactory
    ):
        job1 = lufa_factory.add_tower_template().add_job()
        job2 = lufa_factory.add_tower_template().add_job()
        task1 = str(uuid.uuid4())
        task2 = str(uuid.uuid4())
        lufa_factory.api_repository.add_task(task1, job1.tower_job_id, "task1")
        lufa_factory.api_repository.add_task(task2, job2.tower_job_id, "task2")
        lufa_factory.api_repository.add_callback(task1, "hostA", "ok", "{}", "setup")
        lufa_factory.api_repository.add_callback(task1, "hostB", "failed", "{}", "setup")
        lufa_factory.api_repository.add_callback(task2, "hostC", "ok", "{}", "setup")

        ret = backend_repository.get_job_task_callback_count_by_ids([job1.tower_job_id, job2.tower_job_id])

        counts = {r["state"]: r["count"] for r in ret}
        # two "ok" callbacks across both jobs plus one "failed"
        assert counts.get("ok") == 2
        assert counts.get("failed") == 1
