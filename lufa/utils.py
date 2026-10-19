import logging
import os
import subprocess
import tomllib
from pathlib import Path

from flask import current_app


def get_project_version() -> str:
    project_version = "unknown"
    try:
        # get version from pyproject.toml
        pyproject_toml_file_path = os.path.join(Path(current_app.root_path).parent, "pyproject.toml")
        with open(pyproject_toml_file_path, "rb") as pyproject_toml_file:
            data = tomllib.load(pyproject_toml_file)
            project_version = data["project"]["version"]

        # build version via git
        if project_version == "unknown-version":
            git_version = (
                subprocess.check_output(
                    [
                        "git",
                        "describe",
                        "--tags",
                    ]
                )
                .strip()
                .decode()
                .split("-")[0]
            )
            git_branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).strip().decode()
            git_commit = subprocess.check_output(["git", "rev-parse", "--short=8", "HEAD"]).strip().decode()

            project_version = f"{git_version}-git.{git_branch}.{git_commit}"
    except Exception as e:
        logging.error(e)

    return project_version
