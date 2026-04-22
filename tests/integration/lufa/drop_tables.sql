DROP INDEX IF EXISTS jobs_tower_job_template_id_index CASCADE;
DROP INDEX IF EXISTS jobs_start_time_index CASCADE;
DROP INDEX IF EXISTS tasks_tower_job_id_index CASCADE;
DROP INDEX IF EXISTS task_callbacks_task_uuid CASCADE;
DROP INDEX IF EXISTS task_callbacks_ansible_host_index CASCADE;
DROP INDEX IF EXISTS task_callbacks_timestamp_index CASCADE;
DROP INDEX IF EXISTS stats_ansible_host_index CASCADE;
DROP INDEX IF EXISTS v_host_templates_index;

DROP TRIGGER IF EXISTS delete_unreferenced_job_templates ON jobs;
DROP FUNCTION IF EXISTS delete_unreferenced_job_templates();

DROP TABLE IF EXISTS task_callbacks CASCADE;
DROP TABLE IF EXISTS tasks CASCADE;
DROP TABLE IF EXISTS lufa_job_vars CASCADE; -- deprecated table
DROP TABLE IF EXISTS stats CASCADE;
DROP TABLE IF EXISTS jobs CASCADE;
DROP TABLE IF EXISTS job_templates CASCADE;
DROP TABLE IF EXISTS lufa_users CASCADE;