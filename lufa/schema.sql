DROP VIEW IF EXISTS v_template_hosts_summary;
DROP VIEW IF EXISTS v_host_noncompliance;
DROP VIEW IF EXISTS v_host_templates_ordered;
DROP VIEW IF EXISTS v_template_compliance;
DROP VIEW IF EXISTS v_host_compliance;
DROP VIEW IF EXISTS v_host_template_compliance;
DROP INDEX IF EXISTS v_host_templates_index;
DROP MATERIALIZED VIEW IF EXISTS v_host_templates;
DROP VIEW IF EXISTS v_job_status;

DROP INDEX IF EXISTS jobs_tower_job_template_id_index;
DROP INDEX IF EXISTS jobs_start_time_index;
DROP INDEX IF EXISTS tasks_tower_job_id_index;
DROP INDEX IF EXISTS task_callbacks_task_uuid;
DROP INDEX IF EXISTS task_callbacks_ansible_host_index;
DROP INDEX IF EXISTS task_callbacks_timestamp_index;
DROP INDEX IF EXISTS stats_ansible_host_index;

DROP TRIGGER IF EXISTS delete_unreferenced_job_templates ON jobs;
DROP FUNCTION IF EXISTS delete_unreferenced_job_templates();

DROP TABLE IF EXISTS task_callbacks;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS lufa_job_vars; -- deprecated table
DROP TABLE IF EXISTS stats;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS job_templates;
DROP TABLE IF EXISTS lufa_users;

CREATE TABLE lufa_users (
	distinguished_name varchar PRIMARY KEY,
	username varchar,
	data jsonb
);

CREATE TABLE job_templates (
	tower_job_template_id integer PRIMARY KEY,
	tower_job_template_name varchar,
	playbook_path varchar,
	compliance_interval integer DEFAULT 0,
	awx_organisation varchar,
	template_infos jsonb
);

CREATE TABLE jobs (
	tower_job_id integer PRIMARY KEY,
	tower_job_template_id integer REFERENCES job_templates ON DELETE RESTRICT,
	ansible_limit varchar,
	tower_user_name varchar,
	awx_tags jsonb,
	extra_vars jsonb default '{}',
	artifacts jsonb default '{}',
	tower_schedule_id integer,
	tower_schedule_name varchar,
	tower_workflow_job_id integer,
	tower_workflow_job_name varchar,
	start_time timestamp DEFAULT now(),
	end_time timestamp
);

CREATE TABLE stats (
	tower_job_id integer REFERENCES jobs ON DELETE CASCADE,
	ansible_host varchar,
	ok integer,
	failed integer,
	unreachable integer,
	changed integer,
	skipped integer,
	rescued integer,
	ignored integer,
	PRIMARY KEY (tower_job_id, ansible_host)
);

CREATE TABLE tasks (
	ansible_uuid uuid PRIMARY KEY,
	tower_job_id integer REFERENCES jobs ON DELETE CASCADE,
	task_name varchar
);

CREATE TABLE task_callbacks (
	id bigserial PRIMARY KEY,
	task_ansible_uuid uuid REFERENCES tasks ON DELETE CASCADE,
	ansible_host varchar,
	state varchar,
	module varchar,
	timestamp timestamp DEFAULT now(),
	result_dump jsonb
);

CREATE FUNCTION delete_unreferenced_job_templates()
RETURNS trigger
AS $delete_unreferenced_job_templates$
	BEGIN
		-- try to delete job_template of deleted job (OLD)
		DELETE FROM job_templates
		WHERE tower_job_template_id = OLD.tower_job_template_id;
		RETURN NULL; -- required, but return value is ignored
	EXCEPTION
		WHEN foreign_key_violation then
			-- job_template still referenced by other jobs
			-- therefore no action is required here
		RETURN NULL; -- required, but return value is ignored
	END;
$delete_unreferenced_job_templates$ LANGUAGE plpgsql;

CREATE TRIGGER delete_unreferenced_job_templates
AFTER DELETE ON jobs
FOR EACH ROW
EXECUTE FUNCTION delete_unreferenced_job_templates();

CREATE INDEX stats_ansible_host_index ON stats (ansible_host);
CREATE INDEX jobs_tower_job_template_id_index ON jobs (tower_job_template_id);
CREATE INDEX jobs_start_time_index ON jobs (start_time);
CREATE INDEX tasks_tower_job_id_index ON tasks (tower_job_id);
CREATE INDEX task_callbacks_task_uuid_index ON task_callbacks (task_ansible_uuid);
CREATE INDEX task_callbacks_ansible_host_index ON task_callbacks (ansible_host);
CREATE INDEX task_callbacks_timestamp_index ON task_callbacks (timestamp);
CREATE INDEX task_callbacks_state_index ON task_callbacks (state);

CREATE VIEW v_job_status AS
	SELECT
        jobs.*,
		CASE
		    WHEN end_time IS NULL
		        AND start_time < NOW() - INTERVAL '24 HOURS'
		        THEN 'error'
		    WHEN end_time IS NULL THEN 'started'
		    WHEN EXISTS (SELECT 1 FROM stats
							WHERE jobs.tower_job_id = stats.tower_job_id
							AND (failed > 0 OR unreachable > 0))
			THEN 'failed'
		    ELSE 'success'
		END as state
	FROM jobs;

CREATE MATERIALIZED VIEW v_host_templates AS
    SELECT DISTINCT
		ansible_host,
        job_templates.tower_job_template_id,
        job_templates.tower_job_template_name,
        awx_tags,
        (start_time >= NOW() - INTERVAL '1 DAY' * compliance_interval
             AND failed=0 AND unreachable = 0
             OR compliance_interval = 0) AS compliant,
        (failed=0 AND unreachable=0) AS successful,
        start_time,
        compliance_interval,
        jobs.tower_job_id,
        awx_organisation,
        playbook_path
    FROM stats
    JOIN jobs
    ON stats.tower_job_id = jobs.tower_job_id
    JOIN job_templates
    ON jobs.tower_job_template_id = job_templates.tower_job_template_id;

CREATE INDEX v_host_templates_index
  ON v_host_templates (tower_job_template_id, ansible_host);

CREATE VIEW v_host_template_compliance AS
	SELECT
		ansible_host,
		tower_job_template_id,
		tower_job_template_name,
		compliance_interval,
		awx_organisation,
		playbook_path,
		BOOL_OR(compliant) AS template_compliant
	FROM v_host_templates
	GROUP BY ansible_host, tower_job_template_id, tower_job_template_name, compliance_interval, awx_organisation, playbook_path;

CREATE VIEW v_host_compliance AS
	SELECT
		ansible_host,
		BOOL_AND(template_compliant) AS compliant
	FROM v_host_template_compliance
	GROUP BY ansible_host;

CREATE VIEW v_template_compliance AS
	SELECT
		tower_job_template_id,
		ARRAY_AGG(ansible_host) AS noncompliant
	FROM v_host_template_compliance
	WHERE NOT template_compliant
	GROUP BY tower_job_template_id;
	
CREATE VIEW v_host_templates_ordered AS
	SELECT
	    ansible_host,
	    awx_tags,
	    compliance_interval,
	    compliant,
	    start_time,
	    tower_job_id,
		tower_job_template_id,
		successful
	FROM v_host_templates
	ORDER BY ansible_host, tower_job_template_id, successful DESC, start_time DESC;

CREATE VIEW v_host_noncompliance AS
	SELECT
		ansible_host,
		ARRAY_AGG(json_build_object(
			'last_successful', (SELECT COALESCE(extract(epoch from MAX(start_time)), 0)
								 FROM v_host_templates_ordered AS ordered
								 WHERE compliance.tower_job_template_id = ordered.tower_job_template_id
									AND compliance.ansible_host = ordered.ansible_host
									AND ordered.successful),
			'last_job_run', (SELECT MAX(tower_job_id)
							 FROM v_host_templates_ordered AS ordered
							 WHERE compliance.tower_job_template_id = ordered.tower_job_template_id
								AND compliance.ansible_host = ordered.ansible_host),
			'compliance_interval_in_days', compliance_interval,
			'playbook', playbook_path,
			'template_name', tower_job_template_name,
			'tower_job_template_id', tower_job_template_id,
			'organisation', awx_organisation)
		) AS noncompliant
	FROM v_host_template_compliance AS compliance
	WHERE NOT template_compliant
	GROUP BY ansible_host;

CREATE VIEW v_template_hosts_summary AS
	SELECT DISTINCT ON (ansible_host, tower_job_template_id)
	    ansible_host,
	    awx_tags,
	    compliance_interval,
	    compliant,
	    start_time,
	    tower_job_id,
		tower_job_template_id
	FROM v_host_templates_ordered
