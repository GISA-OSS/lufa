# Compliance Tracking

The compliance-related feature allows you to mark specific jobs and job templates in AWX for compliance tracking and 
audit purposes. This is particularly useful for organizations that need to track critical configurations and 
security-related automation tasks.

## How it works

**Variable Configuration**: Jobs are marked with compliance-related by setting the `lufa_compliance_interval` variable 
to a positive integer in the job template, workflow template, or schedule. If this variable is not set or is set to `0`
the job is considered non-compliance-related.

Example configuration in AWX:
```yaml
extra_vars:
  lufa_compliance_interval: 7
```

**Callback Plugin Integration**: The [callback plugin](https://github.com/GISA-OSS/lufa-callback) automatically detects 
the presence of this variable when a job starts and transmits this information to the dashboard.

**Database Storage**: The dashboard stores this information in a `compliance_interval` column (int, default: 0)
in the job_templates table.

**Frontend Filtering**: The dashboard frontend provides filtering capabilities,
allowing auditors and administrators to quickly identify and review all compliance-related jobs and templates.

## Use Cases

**Auditors**: Track whether all compliance-related jobs have been successfully executed on designated hosts within the given interval in days.

**System Administrators**: Monitor when compliance-related jobs were last successfully executed on a host and identify
any action items.

**Automation Administrators**: Mark jobs and templates appropriately so they are visible in the dashboard for audit and
compliance evaluations.

## Compliance Logic

- A host is considered **compliant** when all compliance-related jobs have been successfully executed within the interval in days.
- After a successful re-run of a previously failed compliance-related job, the host is automatically marked as
compliant again.
- This flexible marking system can be applied to any job or template as needed.

## Benefits

- **Audit Compliance**: Effortlessly recognize and monitor automation tasks that are critical for compliance
- **Compliance Reporting**: Generate reports specifically for compliance-related jobs
- **Risk Management**: Quickly assess the impact and status of compliance-related automation
- **Operational Oversight**: Provide clear visibility into critical infrastructure changes

---

[← Back to README](../Readme.md)