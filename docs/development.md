# Development & Testing

## Setup
- Use virtualenv and pin dependencies.
- Install development dependencies:
```bash
pip install -r requirements-dev.txt
```

## Linting and Formatting (ruff)
This project uses Ruff for both linting (ruff check) and code formatting (ruff format).
- Lint the code:
```bash
# Show lint issues
ruff check

# Automatically fix what can be fixed
ruff check --fix
```

- Format the code:
```bash
# Format all files in place
ruff format

# Check formatting without changing files
ruff format --check

```

## Testing
The project uses three types of tests, executed with pytest:
- Unit tests (fast, isolated)
- Integration tests (service-level, database-backed)
- End-to-end (E2E) tests (full flow, closest to real usage)

Integration and E2E test suites can run against both PostgreSQL and SQLite.
By default, an empty Postgres database is required. 
The data is provided as environment variables (POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD).
Database selection can be controlled via pytest `postgres` and `sqlite3` markers.

- Run unit tests:
```bash
cd tests/unit; PYTHONPATH=../..: pytest -v
```

- Run integration tests:
```bash
cd tests/integration; PYTHONPATH=../..: pytest -v
```
or to run only against the sqlite backend:
```bash
cd tests/integration; PYTHONPATH=../..: pytest -m sqlite3 -v
```

- Run E2E tests:
```bash
cd tests/e2e; PYTHONPATH=../..: pytest -v
```

---

[← Back to README](../Readme.md)