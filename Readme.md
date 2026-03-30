<div align="center">
  <img src="lufa/static/assets/logo/lufa_logo.svg" alt="LUFA Logo" width="300">
</div>

# LUFA - Logging UI for automations

A lightweight dashboard for visualizing and analyzing for Ansible / AWX data. 
It provides quick insights into jobs, templates, and workflows with helpful filters, summaries, and links back to AWX.

## Features
- Overview and detail views for AWX jobs, templates, and workflows
- Powerful filtering and search for efficient analysis
- Compliance-related job marking for compliance tracking and audits
- Optional "Live" mode to periodically refresh data
- Simple configuration for local or production environments
- Runs as a Flask/WSGI app or in Docker

## Requirements
- Python 3.12 (at least 3.12)
- Node.js and npm (for frontend dependency management)
- virtualenv (recommended)
- Optional: Docker (for containerized deployment)
- An SQLite or PostgreSQL database
- An AWX instance or compatible data source
- [Lufa callback plugin](https://github.com/GISA-OSS/lufa-callback)

## Quick Start

### Docker Compose (Recommended)
Docker Compose provides the easiest way to get LUFA up and running quickly.

**Note:** Replace all `<secret>` placeholders with your own credentials before starting.

1) Create a `docker-compose.yml` file:

    **SQLite (Simple Setup)**
    ```yaml
    services:
      lufa:
        image: ghcr.io/gisa-oss/lufa:nightly
        restart: always
        environment: 
          LUFA_SECRET_KEY: "<secret>"
          LUFA_API_KEYS: "['<secret>']"
          LUFA_AWX_BASE_URL: https://awx.example.com
          LUFA_AWX_API_TOKEN: "<secret>"
          LUFA_DB_TYPE: SQLITE
          LUFA_DB_DATABASE: "/home/lufa/lufa.db"
          LUFA_LOG_FILE_PATH: /home/lufa/lufa.log
          LUFA_LOG_LEVEL: INFO
          LUFA_AUTH: LOCAL
          LUFA_AUTH_USER: admin
          LUFA_AUTH_PASSWORD: "<secret>"
        volumes:
          - "./lufa.log:/home/lufa/lufa.log:rw"
          - "./lufa.db:/home/lufa/lufa.db:rw"
        ports:
          - "8080:8080"
    ```
    or

    **PostgreSQL (Production Setup)**
    ```yaml
    services:
      postgres:
        image: postgres:16
        restart: always
        environment:
          POSTGRES_DB: lufa
          POSTGRES_USER: lufa
          POSTGRES_PASSWORD: "<secret>"
          TZ: "<timezone>"
        volumes:
          - postgres-data:/var/lib/postgresql/data
      lufa:
        image: ghcr.io/gisa-oss/lufa:nightly
        restart: always
        environment:
          LUFA_API_KEYS: "['<secret>']"
          LUFA_AUTH_PASSWORD: "<secret>"
          LUFA_SECRET_KEY: "<secret>"
          LUFA_DB_PASSWORD: "<secret>"
          LUFA_AWX_BASE_URL: https://awx.example.com
          LUFA_AWX_API_TOKEN: "<secret>"
          LUFA_DB_TYPE: POSTGRES
          LUFA_DB_HOST: "postgres"
          LUFA_DB_DATABASE: "lufa"
          LUFA_DB_USER: "lufa"
          LUFA_LOG_FILE_PATH: /home/lufa/lufa.log
          LUFA_LOG_LEVEL: INFO
          LUFA_AUTH: LOCAL
          LUFA_AUTH_USER: admin
          TZ: "<timezone>"
        volumes:
          - "./lufa.log:/home/lufa/lufa.log:rw"
        ports:
          - "8080:8080"
        depends_on:
          - postgres
    volumes:
      postgres-data:
    ```

2) Start docker compose:
   ```bash
   docker-compose up -d
   ```

The application will be available at [http://localhost:8080](http://localhost:8080).

> [!IMPORTANT]  
> For `LUFA_AWX_API_TOKEN`, use a token from an audit user with read-only permissions.
> LUFA only requires read access to AWX/Ansible Automation Platform.

### Virtual environment
1) Clone the repository:
```bash
git clone <REPO-URL>
cd lufa
```

2) Create and activate a virtual environment:
```bash
python -m venv .venv
. .venv/bin/activate
```

3) Install dependencies:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

4) Install frontend dependencies:
```bash
npm install
bash scripts/copy-assets.sh
```

5) Configure the app:
- Create a configuration file config.py based on the provided example:
```bash
cp config.py.example config.py
```
- Create an instance directory for secrets:
```bash
mkdir -p instance
```

- Store sensitive values (e.g., SECRET_KEY, API keys, DB passwords) in instance/secrets.py.

6) Run the app (development):
```bash
flask --app wsgi.py run
```
By default, the app will be available at http://127.0.0.1:5000.

### Docker
Build and run the container:

1) Build the image:
```bash
docker build -t lufa:latest .
```
or to use a custom base image:
```bash
docker build --build-arg BASE_IMAGE=<my_registry>/<my_org>/<my_image>:<my_tag> -t lufa:latest .
```

2) Run the container (example):
```bash
docker run --rm -p 8080:8080 \
  -e FLASK_ENV=production \
  -v "$(pwd)/instance:/app/instance" \
  lufa:latest
```

Notes:
- Mount instance/ to provide secrets/config inside the container or use environment variables.
- All configuration variables can be specified as environment variables if `LUFA_` is prefixed.
- For production, orchestrate with systemd, Docker Compose, or Kubernetes and set a non-root user where possible.

## Configuration
The application can be configured via configuration files (config.py and instance/secrets.py)
or environment variables (LUFA_ prefix). Typical settings include:
- Database (type, host, port, name, user, password)
- Logging (log level, file path)
- Authentication (local login or directory service)
- API access keys and external endpoints (e.g., AWX base URL)
- UI customization (texts, labels)
- Compliance-related job tracking (via `lufa_compliance_interval` variable in AWX templates)

All configuration parameters are described in config.py.example.

Recommendations:
- Keep secrets only in instance/ or environment variables.
- Use separate configurations per environment (dev/test/prod).
- Never commit secrets to the repository.

## Further Documentation

| Topic                 | Link                                           |
|-----------------------|------------------------------------------------|
| Compliance Tracking   | [docs/compliance.md](docs/compliance.md)       |
| Development & Testing | [docs/development.md](docs/development.md)     |
| Frontend Dependencies | [docs/frontend-deps.md](docs/frontend-deps.md) |


## Disclaimer
This project provides a custom web frontend to visualize outputs from Ansible / AWX and is not affiliated with, endorsed by, or supported by Red Hat, Inc.. The names are used strictly for descriptive purposes of compatibility.
- The AWX Project is a trademark of Red Hat, Inc., used with permission in accordance with the [AWX Trademark Guidelines](https://github.com/ansible/awx-logos/blob/master/TRADEMARKS.md). 
- Ansible is a registered trademark of Red Hat, Inc.   

---
Thank you for using and contributing to LUFA! If you have questions or need help, please open an issue or start a discussion.