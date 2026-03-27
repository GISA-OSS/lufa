import json
import traceback
from functools import wraps
from typing import Callable

from flask import Blueprint, current_app, jsonify, make_response

from lufa.auth import ro_token_required, sanitize, token_required, with_json_data
from lufa.decorators import debug_only
from lufa.provider import get_api_repository, get_awx_client, get_database_manager
from lufa.repository.api_repository import JobExport, LufaKeyError

MALFORMED_JSON = {"error": "Malformed json"}


bp = Blueprint("api v1", __name__, url_prefix="/api/v1")


def pass_safe_exceptions[**P, R](f: Callable[P, R]):
    @wraps(f)
    def decorator(*args: P.args, **kwargs: P.kwargs):
        try:
            return f(*args, **kwargs)
        except LufaKeyError as ex:
            current_app.logger.warn("LufaKeyError: %s", ex.msg)
            return make_response(jsonify({"error": ex.msg}), 409)
        except Exception as ex:
            current_app.logger.error("Unhandled Error: %s", ex)
            raise ex

    return decorator


@bp.route("/jobs", methods=["POST"])
@token_required
@pass_safe_exceptions
@with_json_data(
    {
        "tower_job_id": int,
        "tower_job_template_id": int,
        "tower_job_template_name": str,
        "awx_tags": str,
        "extra_vars": str,
        "artifacts": str,
    },
    {
        "ansible_limit": str,
        "tower_user_name": str,
        "tower_schedule_id": int,
        "tower_schedule_name": str,
        "tower_workflow_job_id": int,
        "tower_workflow_job_name": int,
        "compliance_interval": int,
        "template_infos": str,
        "playbook_path": str,
        "awx_organisation": str,
        "start_time": str,
    },
)
def jobs_post(data):
    """
    Inserts a new job into the database
    required data in body: [tower_job_id, tower_job_template_id, tower_job_template_name,
    ansible_limit, tower_user_name, awx_tags, tower_schedule_id, tower_schedule_name,
    tower_workflow_job_id, tower_workflow_job_name, security_relevant]
    """
    repository = get_api_repository()

    # Sorting Tags
    # this matters, because ["a", "b"] != ["b", "a"] in postgres
    # distinct_on(awx_tags) would not work correctly
    try:
        if data["awx_tags"] is not None:
            data["awx_tags"] = sorted(json.loads(data["awx_tags"]))
        for key in "artifacts", "extra_vars":
            json.loads(data[key])
    except json.decoder.JSONDecodeError:
        current_app.logger.error("malformed JSON %s", data)
        return make_response(jsonify(MALFORMED_JSON), 400)

    if "template_infos" not in data:
        data["template_infos"] = None

    # get AWX organisation
    data["awx_organisation"] = get_awx_client().get_template_organisation(data["tower_job_template_id"])

    if repository.job_exists(data["tower_job_id"]):
        current_app.logger.error("tried to insert job that already exists: %s", data["tower_job_id"])
        return jsonify({"error": "job already exists"}), 409

    repository.add_job(**data)

    current_app.logger.info("inserted job %s", data["tower_job_id"])

    return jsonify({"ok": "yes"}), 201


@bp.route("/jobs/<int:tower_job_id>", methods=["PATCH"])
@token_required
@pass_safe_exceptions
@with_json_data({"event": str}, {"end_time": str, "artifacts": str})
def jobs_patch(data: dict, tower_job_id: int):
    """
    Patch finished jobs.
    requires "event" in body
    """
    end_time = data.get("end_time", None)

    artifacts_json = data.get("artifacts", "{}")

    try:
        json.loads(artifacts_json)
    except json.decoder.JSONDecodeError:
        return make_response(jsonify(MALFORMED_JSON), 400)
    if data["event"] != "finished":
        return jsonify({"error": "unknown event"}), 409

    if not get_api_repository().job_exists(tower_job_id):
        current_app.logger.error("tried to insert job that not exists: %s", tower_job_id)
        return jsonify({"error": "job not exists"}), 409

    get_api_repository().update_job(tower_job_id, end_time, artifacts_json)
    current_app.logger.info("updated end_time of job %s", tower_job_id)

    return jsonify({"ok": "yes"}), 201


@bp.route("/compliance/hosts", methods=["GET"])
@ro_token_required
@pass_safe_exceptions
def compliance():
    """
    Returns a dictionary with non-compliant hosts.
    """
    repository = get_api_repository()
    resp = repository.get_all_noncompliant_hosts()

    return jsonify(resp)


@bp.route("/tasks", methods=["POST"])
@token_required
@pass_safe_exceptions
@with_json_data(
    {
        "ansible_uuid": str,
        "tower_job_id": int,
        "task_name": str,
    }
)
def tasks_post(data):
    """Insert a new task into the database"""
    repository = get_api_repository()

    if repository.tasks_exists(data["ansible_uuid"]):
        current_app.logger.error("tried to insert task that already exists: %s", data["ansible_uuid"])
        return jsonify({"error": "task already exists"}), 409

    repository.add_task(data["ansible_uuid"], data["tower_job_id"], data["task_name"])
    current_app.logger.info("inserted task: %s", data["ansible_uuid"])

    return jsonify({"ok": "yes"}), 201


@bp.route("/task_callbacks", methods=["POST"])
@token_required
@pass_safe_exceptions
@with_json_data(
    {"task_ansible_uuid": str, "ansible_host": str, "state": str, "module": str, "result_dump": str},
    {"timestamp": str},
)
def task_callbacks_post(data):
    """Insert a new task callback into the database"""
    try:
        json.loads(data["result_dump"])
    except json.decoder.JSONDecodeError:
        return make_response(jsonify(MALFORMED_JSON), 400)
    get_api_repository().add_callback(**data)

    return jsonify({"ok": "yes"}), 201


@bp.route("/stats", methods=["POST", "PUT"])
@token_required
@pass_safe_exceptions
@with_json_data(
    {
        "tower_job_id": int,
        "stats": list,  # contains a list of stats for each host
    }
)
def stats_post(data):
    """
    Insert stats into the database.
    requirements in body: [ansible_host, ok, failed, unreachable, changed, skipped, rescued, ignored]
    """
    requirements_stats = {
        "ansible_host": str,
        "ok": int,
        "failed": int,
        "unreachable": int,
        "changed": int,
        "skipped": int,
        "rescued": int,
        "ignored": int,
    }

    for stat in data["stats"]:
        if sanitize(stat, requirements_stats, {}) is None:
            resp = {"requirements stats": list(requirements_stats)}
            return jsonify(resp), 400

    get_api_repository().add_stats(data["tower_job_id"], data["stats"])
    current_app.logger.info("inserted stats for job: %s", data["tower_job_id"])

    return jsonify({"ok": "yes"}), 201


@bp.route("/jobs/<int:tower_job_id>/export", methods=["GET"])
@ro_token_required
@pass_safe_exceptions
def jobs_export(tower_job_id: int):
    """Export a complete job with all related data as JSON"""

    if not get_api_repository().job_exists(tower_job_id):
        return jsonify({"error": f"Job with id {tower_job_id} does not exist"}), 404

    job_data = get_api_repository().export_job(tower_job_id)
    return jsonify(job_data), 200


@bp.route("/jobs/import", methods=["POST"])
@token_required
@pass_safe_exceptions
@with_json_data(
    {
        "job": dict,
        "job_template": dict,
    },
    {
        "exported_at": str,
        "stats": list,
        "tasks": list,
    },
)
def import_job(data: JobExport):
    """
    Imports a job from export data.

    Returns:
    - 201: Job imported successfully with tower_job_id
    - 400: Invalid export data format
    - 409: Job already exists
    - 500: Unexpected import failed
    """
    repository = get_api_repository()

    try:
        # validate required job fields
        job_data = data.get("job", {})
        required_job_fields = ["tower_job_id", "tower_job_template_id", "tower_job_template_name"]
        missing_fields = [field for field in required_job_fields if field not in job_data]
        if missing_fields:
            return make_response(jsonify({"error": f"Missing required job fields: {', '.join(missing_fields)}"}), 400)

        # validate required template fields
        template_data = data.get("job_template", {})
        required_template_fields = ["tower_job_template_id", "tower_job_template_name"]
        missing_template_fields = [field for field in required_template_fields if field not in template_data]
        if missing_template_fields:
            return make_response(
                jsonify({"error": f"Missing required template fields: {', '.join(missing_template_fields)}"}), 400
            )

        # check if job already exists
        tower_job_id = job_data["tower_job_id"]
        if repository.job_exists(tower_job_id):
            current_app.logger.warning("Attempted to import job that already exists: %s", tower_job_id)
            return make_response(jsonify({"error": f"Job with id {tower_job_id} already exists"}), 409)

        # import the job
        imported_job_id = repository.import_job(data)

        current_app.logger.info("Successfully imported job %s", imported_job_id)

        return jsonify({"ok": "yes", "tower_job_id": imported_job_id, "message": "Job imported successfully"}), 201

    except ValueError as e:
        current_app.logger.error("Invalid import data: %s", str(traceback.format_exc()))
        return make_response(jsonify({"error": f"Invalid import data: {str(e)}"}), 400)
    except Exception as e:
        current_app.logger.error("Failed to import job: %s", str(traceback.format_exc()))
        return make_response(jsonify({"error": f"Import failed: {str(e)}"}), 500)


@bp.route("/echo", methods=["POST"])
@debug_only
@with_json_data({})
def echo(data):
    """Debugging endpoint to test json"""
    return jsonify(data)


@bp.route("/db_now", methods=["GET"])
@debug_only
def db_now():
    """Debugging endpoint to get database date and time"""
    now = get_database_manager().get_db_now()

    resp = {"db_now": now}
    return jsonify(resp)
