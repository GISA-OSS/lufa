import datetime
import json
import uuid

from lufa.repository.backend_repository import BackendRepository

endpoint_uri = "/api/v1"
API_KEY = "1234"
RO_API_KEY = "6789"
INVALID_API_KEY = "xxxx"
AUTH_USER = "admin"
AUTH_PASS = "pass"

malformed_auth = {"malformed": "data", "api_key": API_KEY}

AUTH_HEADERS = {"Authorization": f"token {API_KEY}"}
RO_AUTH_HEADERS = {"Authorization": f"token {RO_API_KEY}"}
INVALID_AUTH_HEADERS = {"Authorization": f"token {INVALID_API_KEY}"}

generic_job_tags = ["a", "b"]

generic_job_data = {
    "tower_job_id": 1,
    "tower_job_template_id": 1,
    "tower_job_template_name": "Job Template Name",
    "ansible_limit": "win999:win404",
    "tower_user_name": "example_user",
    "awx_tags": json.dumps(generic_job_tags),
    "extra_vars": json.dumps({"b": "a"}),
    "artifacts": json.dumps({"a": "b"}),
    "tower_schedule_id": None,
    "tower_schedule_name": None,
    "tower_workflow_job_id": None,
    "tower_workflow_job_name": None,
    "compliance_interval": 7,
    "api_key": API_KEY,
}

generic_task_data = {
    "ansible_uuid": str(uuid.uuid4()),
    "tower_job_id": generic_job_data["tower_job_id"],
    "task_name": "test task",
    "api_key": API_KEY,
}

generic_task_callback_data = {
    "task_ansible_uuid": generic_task_data["ansible_uuid"],
    "ansible_host": "win999.example.com",
    "state": "changed",
    "module": "test_module",
    "result_dump": json.dumps({"result": "dump"}),
    "api_key": API_KEY,
}

generic_stats_data = {
    "tower_job_id": generic_job_data["tower_job_id"],
    "stats": [
        {
            "ansible_host": "win999.example.com",
            "ok": 10,
            "failed": 0,
            "unreachable": 0,
            "changed": 5,
            "skipped": 1,
            "rescued": 1,
            "ignored": 0,
        },
        {
            "ansible_host": "win443.example.com",
            "ok": 8,
            "failed": 1,
            "unreachable": 0,
            "changed": 5,
            "skipped": 1,
            "rescued": 0,
            "ignored": 0,
        },
    ],
    "api_key": API_KEY,
}


class TestApi:
    def test_post_job(self, client):
        # unauthorized
        data = generic_job_data | {"api_key": "wrong key"}
        r = client.post(endpoint_uri + "/jobs", json=data)
        assert r.status_code == 401

        # malformed
        r = client.post(endpoint_uri + "/jobs", json=malformed_auth)
        assert r.status_code == 400

        # correct
        r = client.post(endpoint_uri + "/jobs", json=generic_job_data)
        assert r.status_code == 201, r.text

        # prüfe, ob job in /data/jobs enthalten ist
        job_res = client.get("/data/jobs")
        assert job_res.status_code != 403
        assert generic_job_data["tower_job_id"] in [item["tower_job_id"] for item in job_res.json["jobs_table"]["data"]]
        job = [j for j in job_res.json["jobs_table"]["data"] if j["tower_job_id"] == generic_job_data["tower_job_id"]][
            0
        ]
        assert job["awx_tags"] == generic_job_tags

        # insert same tower_job_id twice
        r = client.post(endpoint_uri + "/jobs", json=generic_job_data)
        assert r.status_code == 409

    def test_post_job_with_workflow_name(self, client):
        job_data = generic_job_data | {
            "tower_job_id": 2,
            "tower_workflow_job_id": 42,
            "tower_workflow_job_name": "Import Workflow",
        }

        r = client.post(endpoint_uri + "/jobs", json=job_data)
        assert r.status_code == 201, r.text

        job_res = client.get("/data/jobs")
        assert job_res.status_code != 403
        job = [j for j in job_res.json["jobs_table"]["data"] if j["tower_job_id"] == job_data["tower_job_id"]][0]

        assert job["tower_workflow_job_id"] == job_data["tower_workflow_job_id"]
        assert job["tower_workflow_job_name"] == job_data["tower_workflow_job_name"]

    def test_patch_job(self, client):
        """Create a Job and set end_time"""

        # creating job
        r = client.post(endpoint_uri + "/jobs", json=generic_job_data)
        assert r.status_code == 201, r.text

        url = "/jobs/" + str(generic_job_data["tower_job_id"])

        # sending unknown event
        data = {"event": "unknown_event", "api_key": API_KEY}

        r = client.patch(endpoint_uri + url, json=data)
        assert r.status_code == 409

        # sending finished event
        data = {"event": "finished", "api_key": API_KEY}

        r = client.patch(endpoint_uri + url, json=data)
        assert r.status_code == 201, r.text

        job_res = client.get("/data/jobs")
        assert job_res.status_code != 403
        job = job_res.json["jobs_table"]["data"][0]
        dt = datetime.datetime.now() - datetime.datetime.fromisoformat(job["end_time"])

        assert dt < datetime.timedelta(seconds=1)  # correct end_time
        # eigentlich < statt <= aber lieber <) als ein sleep einbauen
        assert datetime.datetime.fromisoformat(job["start_time"]) <= datetime.datetime.fromisoformat(job["end_time"])

    def test_compliance_autorisation(self, client):
        r = client.get(endpoint_uri + "/compliance/hosts", headers=AUTH_HEADERS)
        assert r.status_code == 200, r.text
        r = client.get(endpoint_uri + "/compliance/hosts", headers=RO_AUTH_HEADERS)
        assert r.status_code == 200, r.text
        r = client.get(endpoint_uri + "/compliance/hosts", headers=INVALID_AUTH_HEADERS)
        assert r.status_code == 401, r.text

    def test_compliance(self, client):
        # creating job
        r = client.get(endpoint_uri + "/compliance/hosts", headers=AUTH_HEADERS)
        assert r.status_code == 200, r.text
        assert len(r.json) == 0

        r = client.post(endpoint_uri + "/jobs", json=generic_job_data)
        assert r.status_code == 201, r.text

        r = client.post(endpoint_uri + "/stats", json=generic_stats_data)
        assert r.status_code == 201, r.text

        r = client.get(endpoint_uri + "/compliance/hosts", headers=AUTH_HEADERS)
        assert r.status_code == 200, r.text
        resp = r.json
        assert len(resp) == 1
        assert len(list(resp.values())[0]) == 1

    def test_post_jobs_multiple(self, client):
        # create x templates with y jobs each
        count_templates = 10
        count_jobs_each = 10

        job_id = 0
        for template in range(count_templates):
            for _ in range(count_jobs_each):
                special_data = {"tower_job_template_id": template, "tower_job_id": job_id}

                job_id += 1

                job_data = generic_job_data | special_data

                r = client.post(endpoint_uri + "/jobs", json=job_data)
                assert r.status_code == 201, r.text

        job_res = client.get("/data/jobs")
        assert job_res.status_code != 403
        assert len(job_res.json["jobs_table"]["data"]) == count_templates * count_jobs_each

        templates_res = client.get("/data/templates")
        assert len(templates_res.json["templates_table"]["data"]) == count_templates

    def test_post_task(self, client):
        # post job
        r = client.post(endpoint_uri + "/jobs", json=generic_job_data)
        assert r.status_code == 201, r.text

        # unauthorized
        data = generic_task_data | {"api_key": "wrong key"}
        r = client.post(endpoint_uri + "/tasks", json=data)
        assert r.status_code == 401

        # malformed
        r = client.post(endpoint_uri + "/tasks", json=malformed_auth)
        assert r.status_code == 400

        # correct
        r = client.post(endpoint_uri + "/tasks", json=generic_task_data)
        assert r.status_code == 201, r.text

        # insert same task twice
        r = client.post(endpoint_uri + "/tasks", json=generic_task_data)
        assert r.status_code == 409

    def test_post_task_callback(self, client):
        # post job
        r = client.post(endpoint_uri + "/jobs", json=generic_job_data)
        assert r.status_code == 201, r.text

        # post task
        r = client.post(endpoint_uri + "/tasks", json=generic_task_data)
        assert r.status_code == 201, r.text

        # malformed
        r = client.post(endpoint_uri + "/task_callbacks", json=malformed_auth)
        assert r.status_code == 400

        # unauthorized
        data = generic_task_callback_data | {"api_key": "wrong key"}
        r = client.post(endpoint_uri + "/task_callbacks", json=data)
        assert r.status_code == 401

        # correct
        r = client.post(endpoint_uri + "/task_callbacks", json=generic_task_callback_data)
        assert r.status_code == 201, r.text

        callbacks_res = client.get(f"/data/jobs/{generic_job_data['tower_job_id']}/callbacks")
        callback = callbacks_res.json["callbacks_table"]["data"][0]
        dt = datetime.datetime.now() - datetime.datetime.fromisoformat(callback["timestamp"])
        assert dt < datetime.timedelta(seconds=1)  # correct timestamp

    def test_post_stats(self, client):
        # post job
        r = client.post(endpoint_uri + "/jobs", json=generic_job_data)
        assert r.status_code == 201, r.text

        # malformed
        r = client.post(endpoint_uri + "/stats", json=malformed_auth)
        assert r.status_code == 400

        # unauthorized
        data = generic_stats_data | {"api_key": "wrong key"}
        r = client.post(endpoint_uri + "/stats", json=data)
        assert r.status_code == 401

        # insufficient permissions
        data = generic_stats_data | {"api_key": RO_API_KEY}
        r = client.post(endpoint_uri + "/stats", json=data)
        assert r.status_code == 401

        # correct
        r = client.post(endpoint_uri + "/stats", json=generic_stats_data)
        assert r.status_code == 201, r.text

    def test_playbook_run(self, client):
        with open("testdata/playbook_requests.json", "r") as testfile:
            json_req = json.load(testfile)
            for playbook_req in json_req["requests"]:
                path = playbook_req["path"]
                if playbook_req["method"] == "PATCH":
                    r = client.patch(path, json=playbook_req["body"], headers=AUTH_HEADERS)
                else:
                    r = client.post(path, json=playbook_req["body"], headers=AUTH_HEADERS)
                assert r.status_code == 201, r.text

            job_id = json_req["requests"][0]["body"]["tower_job_id"]
            last_callback = json_req["requests"][-3]["body"]

            job_overview_res = client.get(f"/data/jobs/{job_id}/overview")
            job_overview = job_overview_res.json

            # check last callback state and host
            assert len(job_overview["hosts_table"]["data"]) > 0
            assert job_overview["hosts_table"]["data"][0]["ansible_host"] == last_callback["ansible_host"]
            assert job_overview["hosts_table"]["data"][0]["state"] == last_callback["state"]

            # check "Status-Übersicht"
            assert job_overview["host_states"][0]["count"] == 1 and job_overview["host_states"][0]["state"] == "ok"

    def test_job_error_after_24_hours(self, client):
        job = dict(generic_job_data)
        start_time = datetime.datetime.now() - datetime.timedelta(hours=26)
        job["start_time"] = start_time.isoformat()
        r = client.post(endpoint_uri + "/jobs", json=job)
        assert r.status_code == 201, r.text

        jobs_res = client.get("/data/jobs")
        job = jobs_res.json["jobs_table"]["data"][0]
        assert job["state"] == BackendRepository.ERROR

        job_res = client.get(f"/data/jobs/{job['tower_job_id']}/state")
        job_info = job_res.json
        assert job_info["state"]["state"] == BackendRepository.ERROR
