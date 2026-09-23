import logging
import os
from typing import Protocol, cast
from uuid import uuid4

import flask
from flask import Flask, current_app, g, redirect, request, send_from_directory, url_for
from flask_ldap3_login import LDAP3LoginManager
from flask_ldap3_login.forms import LDAPLoginForm
from flask_login import LoginManager, login_required, login_user, logout_user
from flask_wtf.csrf import CSRFProtect

from lufa.auth import TestLogin, User, UserManager
from lufa.decorators import debug_only
from lufa.utils import get_project_version


class LufaRequestData(Protocol):
    crp_nonce: str


def render_template(jinja_template: str, **kwargs) -> str:
    r = cast(LufaRequestData, g)
    return flask.render_template(jinja_template, crp_nonce=r.crp_nonce, **kwargs)


def drop_unsafe_redirects(path: str | None) -> str | None:
    return path if path is None or path.startswith("/") else None


def _load_app_config(app: Flask, test_config=None):
    if test_config is not None:
        app.config.from_mapping(test_config)
        return

    if check_env_vars_prefix("LUFA"):
        app.config.from_prefixed_env(prefix="LUFA")
        return

    app.config.from_object("config")
    app.config.from_pyfile("secrets.py")


def _setup_logging(app: Flask):
    log_level = app.config.get("LOG_LEVEL", "INFO")
    app.logger.setLevel(log_level)
    log_file_path = app.config.get("LOG_FILE_PATH")
    if isinstance(log_file_path, str) and log_file_path:
        log_handler = logging.FileHandler(log_file_path)
        log_file_formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
        log_handler.setFormatter(log_file_formatter)
        app.logger.addHandler(log_handler)


def create_app(test_config=None):
    """Create and configure the app"""
    app = Flask(__name__, instance_relative_config=True)

    # Enable CSRFProtect
    app.config.setdefault("WTF_CSRF_ENABLED", True)
    if not app.config.get("TESTING", False) and not app.config.get("WTF_CSRF_ENABLED", True):
        raise RuntimeError("CSRFProtect must be enabled in production")

    csrf = CSRFProtect()
    csrf.init_app(app)

    _load_app_config(app, test_config)
    _setup_logging(app)

    @app.before_request
    def log_request_info():
        """Log request info"""
        app.logger.debug(
            "Path: %s | Method: %s",
            request.path,
            request.method,
        )

    @app.before_request
    def create_crp_nonce():
        # https://tedboy.github.io/flask/interface_api.app_globals.html?highlight=global
        # Flask "ensures (g) is only valid for the active request and that will return different values for each request."
        g.crp_nonce = uuid4()

    @app.after_request
    def set_security_headers(response):
        """Set HTTP security headers"""
        try:
            nonce = g.crp_nonce
        except AttributeError:
            nonce = uuid4()
            app.logger.warning("No g.crp_nonce")

        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            f"script-src 'self' 'nonce-{nonce}'; "
            f"style-src 'self' 'nonce-{nonce}'; "
            "img-src 'self' data:; "
            "font-src 'self'; "
            "frame-ancestors 'none'"
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response

    from . import provider

    provider.init_app(app)

    from . import api_v1, frontend

    app.register_blueprint(api_v1.bp)
    csrf.exempt(api_v1.bp)
    app.register_blueprint(frontend.bp)

    # Prometheus metrics
    if app.config.get("ENABLE_METRICS_ENDPOINT", False):
        from prometheus_flask_exporter import PrometheusMetrics

        PrometheusMetrics(app, prefix="lufa")
        app.logger.info("Prometheus metrics enabled at /metrics")

    login_manager = LoginManager(app)
    if str(app.config.get("AUTH")).upper().startswith("LOCAL"):
        user_manager = UserManager(app)
    else:
        user_manager = LDAP3LoginManager(app)

    @app.context_processor
    def inject_constants():
        """Inject constants"""
        return {"RELOAD_INTERVAL": app.config.get("RELOAD_INTERVAL", 5000)}

    @login_manager.user_loader
    def load_user(
        id,
    ):
        """Loading user by ID"""
        user = provider.get_user_repository().get_user(id)

        # user not found
        if not user:
            return None

        dn = user["distinguished_name"]
        username = user["username"]
        data = user["data"]

        return User(dn, username, data)

    @user_manager.save_user
    def save_user(dn, username, data, memberships):
        """Saving user info"""

        provider.get_user_repository().save_user(username, dn, data)

        return User(dn, username, data)

    @login_manager.unauthorized_handler
    def unauthorized_callback():
        """Redirect unauthenticated users to the login page."""
        return redirect(url_for("login", next=request.path), code=302)

    @app.route("/login", methods=["GET", "POST"])
    def login():
        """Login user either with test credentials or via LDAP"""
        if str(current_app.config.get("AUTH")).upper().startswith("LOCAL"):
            form = TestLogin()
        else:
            form = LDAPLoginForm()

        if form.validate_on_submit():
            # successful login
            login_user(form.user)
            return redirect(drop_unsafe_redirects(request.args.get("next")) or url_for("frontend.welcome"))

        login_ui = {
            "title": current_app.config.get("LOGIN_TITLE", "Sign in"),
            "intro": current_app.config.get("LOGIN_INTRO", None),
            "username_label": current_app.config.get("LOGIN_USERNAME_LABEL", "Username"),
            "password_label": current_app.config.get("LOGIN_PASSWORD_LABEL", "Password"),
            "submit_label": current_app.config.get("LOGIN_SUBMIT_LABEL", "Sign in"),
        }

        return render_template("login.html", form=form, login_ui=login_ui)

    @app.route("/logout")
    @login_required
    def logout():
        """Logout"""
        logout_user()
        return redirect("/")

    @app.route("/about")
    def version():
        """Show about page"""
        project_version = get_project_version()
        return render_template("about.html", project_version=project_version)

    @app.route("/schema")
    @debug_only
    def schema():
        """Return postgres schema"""
        return send_from_directory(".", "schema.sql", as_attachment=True)

    return app


def check_env_vars_prefix(prefix: str = "LUFA"):
    """Checks if the prefix is in one environment variable

    @param prefix: prefix to find in env
    @return: true if any environment variable contains the prefix
    """
    prefix = f"{prefix}_"

    for env_var in os.environ.keys():
        if prefix in env_var:
            return True

    return False
