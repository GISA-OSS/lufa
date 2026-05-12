import json
import os
from io import BytesIO
from typing import Tuple

from flask import Blueprint, Response, current_app, jsonify, request, send_file
from flask_login import login_required

from lufa import render_template
from lufa.decorators import debug_only
from lufa.provider import get_backend_repository
from lufa.repository.backend_repository import ResourceNotFoundError

bp = Blueprint("frontend", __name__, url_prefix="")


@bp.errorhandler(ResourceNotFoundError)
def handle_resource_not_found(e: ResourceNotFoundError) -> Tuple[str, int]:
    """Handles ResourceNotFoundError by rendering an error page with appropriate title and message."""

    return render_template("error.html", error_title="Resource not found", error_message=e.msg), 404


@bp.route("/", methods=["GET"])
@login_required
def welcome():
    """Renders the welcome page with compliance statistics."""
    compliance = get_backend_repository().get_compliant_non_compliant_stats()

    return render_template("welcome.html", compliance=compliance)


@bp.route("/jobs", methods=["GET"])
@login_required
def jobs():
    """Renders the jobs page."""
    return render_template("jobs.html")


@bp.route("/data/jobs", methods=["GET"])
@login_required
def jobs_data():
    """Returns the data for the jobs table in json format."""
    days = request.args.get("days", default=3, type=int)

    data = {"jobs_table": {"data": get_backend_repository().get_last_jobs_by_days(days)}}

    return jsonify(data)


@bp.route("/jobs/<int:tower_job_id>/recap", methods=["GET"])
@login_required
def job_recap(tower_job_id):
    """Renders the job recap page for the given job ID."""
    initial_job_state = get_backend_repository().get_job_status(tower_job_id)["state"]

    return render_template(
        "job_recap.html",
        tower_job_id=tower_job_id,
        initial_job_state=initial_job_state,
        tower_job_template_name=get_backend_repository().get_job_template_name_by_job_id(tower_job_id),
    )


@bp.route("/data/jobs/<int:tower_job_id>/recap", methods=["GET"])
@login_required
def job_recap_data(tower_job_id):
    """Returns the data for the job recap page in json format."""
    data = {"stats_table": {"data": {}}}

    data["stats_table"]["data"] = get_backend_repository().get_job_stats(tower_job_id)

    return jsonify(data)


@bp.route("/jobs/<int:tower_job_id>/callbacks", methods=["GET"])
@login_required
def job_callbacks(tower_job_id: int):
    """Renders the job callbacks page for the given job ID."""
    initial_job_state = get_backend_repository().get_job_status(tower_job_id)["state"]

    return render_template(
        "job_callbacks.html",
        tower_job_id=tower_job_id,
        initial_job_state=initial_job_state,
        tower_job_template_name=get_backend_repository().get_job_template_name_by_job_id(tower_job_id),
    )


@bp.route("/data/jobs/<int:tower_job_id>/callbacks", methods=["GET"])
@login_required
def job_callbacks_data(tower_job_id: int):
    """Returns the data for the job callbacks page in json format."""
    data = {
        "callbacks_table": {"data": get_backend_repository().get_job_task_callbacks(tower_job_id)},
        "task_states": get_backend_repository().get_last_host_callback_task_count(tower_job_id),
    }

    return jsonify(data)


@bp.route("/jobs/<int:tower_job_id>", methods=["GET"])
@bp.route("/jobs/<int:tower_job_id>/overview", methods=["GET"])
@login_required
def job_hosts_overview(tower_job_id: int):
    """Renders the job overview page for the given job ID."""
    initial_job_state = get_backend_repository().get_job_status(tower_job_id)["state"]

    return render_template(
        "job_overview.html",
        tower_job_id=tower_job_id,
        initial_job_state=initial_job_state,
        tower_job_template_name=get_backend_repository().get_job_template_name_by_job_id(tower_job_id),
    )


@bp.route("/data/jobs/<int:tower_job_id>", methods=["GET"])
@bp.route("/data/jobs/<int:tower_job_id>/overview", methods=["GET"])
@login_required
def job_overview_data(tower_job_id: int):
    """Returns the data for the job overview page in json format."""
    data = {
        "hosts_table": {"data": get_backend_repository().get_last_host_callback(tower_job_id)},
        "host_states": get_backend_repository().get_last_host_callback_count(tower_job_id),
    }

    return jsonify(data)


@bp.route("/data/jobs/<int:tower_job_id>/state", methods=["GET"])
@login_required
def job_status_data(tower_job_id: int):
    """Returns the data for the job status page in json format."""
    data = {"state": get_backend_repository().get_job_status(tower_job_id)}

    return jsonify(data)


@bp.route("/jobs/<int:tower_job_id>/infos", methods=["GET"])
@login_required
def job_infos(tower_job_id: int):
    """Renders the job infos page for the given job ID."""
    job_info = get_backend_repository().get_job_info(tower_job_id)
    if job_info is None:
        raise ResourceNotFoundError(f"Job not found: {tower_job_id}")

    tower_job_template_id = job_info["tower_job_template_id"]
    tower_user_name = job_info["tower_user_name"]
    tower_schedule_name = job_info["tower_schedule_name"]
    tower_workflow_job_name = job_info["tower_workflow_job_name"]
    awx_tags = job_info["awx_tags"]
    ansible_limit = job_info["ansible_limit"]
    start_time = job_info["start_time"]
    end_time = job_info["end_time"]
    playbook_path = job_info["playbook_path"]
    extra_vars = json.loads(job_info["extra_vars"])
    artifacts = json.loads(job_info["artifacts"])

    template = get_backend_repository().get_template(tower_job_template_id)

    awx_template_link = current_app.config["AWX_BASE_URL"] + "/#/templates/job_template/" + str(tower_job_template_id)
    awx_job_link = current_app.config["AWX_BASE_URL"] + "/#/jobs/playbook/" + str(tower_job_id)

    initial_job_state = get_backend_repository().get_job_status(tower_job_id)["state"]

    return render_template(
        "job_infos.html",
        tower_job_id=tower_job_id,
        initial_job_state=initial_job_state,
        tower_job_template_name=get_backend_repository().get_job_template_name_by_job_id(tower_job_id),
        tower_job_template_id=tower_job_template_id,
        tower_user_name=tower_user_name,
        tower_schedule_name=tower_schedule_name,
        tower_workflow_job_name=tower_workflow_job_name,
        awx_template_link=awx_template_link,
        awx_job_link=awx_job_link,
        awx_tags=awx_tags,
        ansible_limit=ansible_limit,
        start_time=start_time,
        end_time=end_time,
        playbook_path=playbook_path,
        template=template,
        extra_vars_pretty=json.dumps(extra_vars, indent=2),
        artifacts_pretty=json.dumps(artifacts, indent=2),
    )


@bp.route("/task_callbacks/<int:task_callback_id>", methods=["GET"])
@login_required
def task_callback(task_callback_id: int):
    """Renders the task callback page for the given task callback ID."""
    task_callback_data = get_backend_repository().get_callback_data(task_callback_id)

    return render_template(
        "task_callback.html",
        task_callback_id=task_callback_id,
        task_callback_data=json.dumps(task_callback_data, indent=2),
    )


@bp.route("/hosts", methods=["GET"])
@login_required
def hosts():
    """Renders the hosts page."""
    return render_template("hosts.html")


@bp.route("/data/hosts", methods=["GET"])
@login_required
def hosts_data():
    """Returns the data for the hosts table in json format."""
    data = {"hosts_table": {"data": {}}}

    data["hosts_table"]["data"] = get_backend_repository().get_all_host_compliance_state()

    return jsonify(data)


@bp.route("/hosts/<string:ansible_host>", methods=["GET"])
@bp.route("/hosts/<string:ansible_host>/templates", methods=["GET"])
@login_required
def host_templates(ansible_host: str):
    """Renders the host templates page for the given host."""
    return render_template("host_templates.html", ansible_host=ansible_host)


@bp.route("/data/hosts/<string:ansible_host>", methods=["GET"])
@bp.route("/data/hosts/<string:ansible_host>/templates", methods=["GET"])
@login_required
def host_templates_data(ansible_host: str):
    """Returns the data for the host templates page in json format."""
    data = {"templates_table": {"data": get_backend_repository().get_host_templates(ansible_host)}}

    return jsonify(data)


@bp.route("/hosts/<string:ansible_host>/jobs", methods=["GET"])
@login_required
def host_jobs(ansible_host: str):
    """Renders the host jobs page for the given host."""
    return render_template("host_jobs.html", ansible_host=ansible_host)


@bp.route("/data/hosts/<string:ansible_host>/jobs", methods=["GET"])
@login_required
def host_jobs_data(ansible_host: str):
    """Returns the data for the host jobs page in json format."""
    data = {"jobs_table": {"data": get_backend_repository().get_host_jobs(ansible_host)}}

    return jsonify(data)


@bp.route("/data/hosts/<string:ansible_host>/jobs_extended", methods=["GET"])
@login_required
def host_jobs_data_extended(ansible_host: str):
    """Returns the data for the host jobs page in json format.

    This endpoint extends /data/hosts/<string:ansible_host>/jobs so that
    the last callback of the currently selected host for a job is additionally shown.
    The SQL is adjusted accordingly.
    """
    data = {"jobs_table": {"data": get_backend_repository().get_host_last_callback(ansible_host)}}

    return jsonify(data)


@bp.route("/templates", methods=["GET"])
@login_required
def templates():
    """Renders the templates page."""
    return render_template("templates.html")


@bp.route("/data/templates", methods=["GET"])
@login_required
def templates_data():
    """Returns the data for the templates table in json format."""
    data = {"templates_table": {"data": {}}}

    data["templates_table"]["data"] = get_backend_repository().get_all_job_templates()

    return jsonify(data)


@bp.route("/templates/<int:tower_job_template_id>", methods=["GET"])
@bp.route("/templates/<int:tower_job_template_id>/jobs", methods=["GET"])
@login_required
def template_jobs(tower_job_template_id: int):
    """Renders the template jobs page for the given template ID."""
    return render_template(
        "template_jobs.html",
        tower_job_template_id=tower_job_template_id,
        tower_job_template_name=get_backend_repository().get_job_template_name_by_template_id(tower_job_template_id),
    )


@bp.route("/data/templates/<int:tower_job_template_id>", methods=["GET"])
@bp.route("/data/templates/<int:tower_job_template_id>/jobs", methods=["GET"])
@login_required
def template_jobs_data(tower_job_template_id: int):
    """Returns the data for the template jobs page in json format."""
    data = {"jobs_table": {"data": get_backend_repository().get_template_job_data(tower_job_template_id)}}

    return jsonify(data)


@bp.route("/templates/<int:tower_job_template_id>/hosts", methods=["GET"])
@login_required
def template_hosts(tower_job_template_id: int):
    """Renders the template hosts page for the given template ID."""
    return render_template(
        "template_hosts.html",
        tower_job_template_id=tower_job_template_id,
        tower_job_template_name=get_backend_repository().get_job_template_name_by_template_id(tower_job_template_id),
    )


@bp.route("/data/templates/<int:tower_job_template_id>/hosts", methods=["GET"])
@login_required
def template_hosts_data(tower_job_template_id: int):
    """Returns the data for the template hosts page in json format."""
    data = {"hosts_table": {"data": get_backend_repository().get_template_hosts_summary(tower_job_template_id)}}

    return jsonify(data)


@bp.route("/templates/<int:tower_job_template_id>/infos", methods=["GET"])
@login_required
def template_infos(tower_job_template_id: int):
    """Renders the template infos page for the given template ID."""
    template = get_backend_repository().get_template(tower_job_template_id)
    if template is None:
        raise ResourceNotFoundError(f"Template not found: {tower_job_template_id}")

    if template["template_infos"]:
        template["template_infos"] = json.loads(template["template_infos"])

    try:
        awx_template_link = current_app.config["AWX_BASE_URL"] + f"/#/templates/job_template/{tower_job_template_id}"
    except KeyError:
        awx_template_link = "https://awx_base_url_not_configured.example.com/"

    return render_template(
        "template_infos.html",
        tower_job_template_id=tower_job_template_id,
        tower_job_template_name=get_backend_repository().get_job_template_name_by_template_id(tower_job_template_id),
        awx_template_link=awx_template_link,
        template=template,
    )


@bp.route("/workflows", methods=["GET"])
@login_required
def workflows():
    """Renders the workflows page."""
    return render_template("workflows.html")


@bp.route("/data/workflows", methods=["GET"])
@login_required
def workflows_data():
    """Returns the data for the workflows table in json format."""
    data = {"workflow_jobs_table": {"data": {}}}
    data["workflow_jobs_table"]["data"] = get_backend_repository().get_all_workflow_jobs()

    return jsonify(data)


@bp.route("/data/jobs/<int:tower_workflow_job_id>/workflowstates", methods=["GET"])
@login_required
def workflow_status_data(tower_workflow_job_id: int):
    """Returns the data for the workflow status page in json format."""
    data = {"states": get_backend_repository().get_workflow_job_info(tower_workflow_job_id)}

    return jsonify(data)


@bp.route("/workflows/<int:tower_workflow_job_id>", methods=["GET"])
@bp.route("/workflows/<int:tower_workflow_job_id>/overview", methods=["GET"])
@login_required
def workflow_overview(tower_workflow_job_id: int):
    """Renders the workflow overview page for the given workflow ID."""
    return render_template(
        "workflow_overview.html",
        tower_workflow_job_id=tower_workflow_job_id,
        tower_workflow_job_name=get_backend_repository().get_workflow_job_name(tower_workflow_job_id),
    )


@bp.route("/data/workflows/<int:tower_workflow_job_id>", methods=["GET"])
@bp.route("/data/workflows/<int:tower_workflow_job_id>/overview", methods=["GET"])
@login_required
def workflow_overview_data(tower_workflow_job_id: int):
    """Returns the data for the workflow overview page in json format."""
    data = {
        "hosts_table": {"data": get_backend_repository().get_last_host_callbacks_by_workflow_id(tower_workflow_job_id)},
        "host_states": get_backend_repository().get_last_host_callbacks_count_by_workflow_id(tower_workflow_job_id),
    }

    return jsonify(data)


@bp.route("/workflows/<int:tower_workflow_job_id>/recaps", methods=["GET"])
@login_required
def workflow_recaps(tower_workflow_job_id: int):
    """Renders the workflow recaps page for the given workflow ID."""
    return render_template(
        "workflow_recaps.html",
        tower_workflow_job_id=tower_workflow_job_id,
        tower_workflow_job_name=get_backend_repository().get_workflow_job_name(tower_workflow_job_id),
    )


@bp.route("/data/workflows/<int:tower_workflow_job_id>/recaps", methods=["GET"])
@login_required
def workflow_recaps_data(tower_workflow_job_id: int):
    """Returns the data for the workflow recaps page in json format."""
    data = {
        "workflow_job_info": get_backend_repository().get_workflow_job_info(tower_workflow_job_id),
        "stats_table": {"data": get_backend_repository().get_workflow_job_stats(tower_workflow_job_id)},
    }

    return jsonify(data)


@bp.route("/workflows/<int:tower_workflow_job_id>/callbacks", methods=["GET"])
@login_required
def workflow_callbacks(tower_workflow_job_id: int):
    """Renders the workflow callbacks page for the given workflow ID."""
    return render_template(
        "workflow_callbacks.html",
        tower_workflow_job_id=tower_workflow_job_id,
        tower_workflow_job_name=get_backend_repository().get_workflow_job_name(tower_workflow_job_id),
    )


@bp.route("/data/workflows/<int:tower_workflow_job_id>/callbacks", methods=["GET"])
@login_required
def workflow_callbacks_data(tower_workflow_job_id: int):
    """Returns the data for the workflow callbacks page in json format."""
    data = {
        "workflow_job_info": get_backend_repository().get_workflow_job_info(tower_workflow_job_id),
        "workflow_states": get_backend_repository().get_workflow_callbacks_count(tower_workflow_job_id),
        "callbacks_table": {"data": get_backend_repository().get_workflow_callbacks(tower_workflow_job_id)},
    }

    return jsonify(data)


@bp.route("/sqlite", methods=["GET"])
@debug_only
@login_required
def download_sqlite():
    """Makes the sql file available for download."""
    if current_app.config.get("DB_TYPE").upper() == "SQLITE":
        db_path = current_app.config["DB_DATABASE"]
        if "." not in db_path:
            db_path = db_path + ".db"
        if os.path.isfile(db_path):
            with open(db_path, "rb") as file:
                bytes_io = BytesIO(file.read())
                return send_file(bytes_io, mimetype="application/octet-stream", download_name="sqlite.db")
        else:
            return Response(f"file not found: {db_path}", status=404)
    else:
        return Response("sqlite not in use", status=404)
