import json
from functools import partial, wraps
from typing import Callable, Concatenate

import wtforms  # type:ignore
from flask import current_app, jsonify, make_response, request
from flask_login import UserMixin  # type:ignore
from flask_wtf import FlaskForm  # type:ignore
from wtforms import validators


class TestLogin(FlaskForm):
    username = wtforms.StringField("Username", validators=[validators.DataRequired()])
    password = wtforms.PasswordField("Password", validators=[validators.DataRequired()])
    submit = wtforms.SubmitField("Submit")
    remember_me = wtforms.BooleanField("Remember Me", default=True)

    def validate(self, *args, **kwargs):
        username = self.username.data
        password = self.password.data

        auth_type = "local"
        if (
            auth_type == "local"
            and username == str(current_app.config.get("AUTH_USER"))
            and password == current_app.config.get("AUTH_PASSWORD")
        ):
            self.user = current_app.local_user_manager._save_user(f"{username}_dn", username, "", None)
            return True

        self.user = None

        self.username.errors = ["Invalid Username/Password."]
        self.password.errors = ["Invalid Username/Password."]
        return False


class User(UserMixin):
    def __init__(self, dn, username, data):
        self.dn = dn
        self.username = username
        self.data = data

    def __repr__(self):
        return self.dn

    def get_id(self):
        return self.dn


class UserManager:
    def __init__(self, app=None):
        self._save_user = None

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.local_user_manager = self

    def save_user(self, callback):
        self._save_user = callback
        return callback


def api_key_valid(api_key: str | None, require_write_permission: bool):
    if api_key is None:
        return False
    if api_key in current_app.config["API_KEYS"]:
        current_app.logger.debug("valid api_key from %s", request.remote_addr)
        return True
    try:
        if api_key in current_app.config["API_KEYS_RO"]:
            if not require_write_permission:
                return True
            current_app.logger.warning("api_key is insufficient from %s", request.remote_addr)
            return False
    except KeyError:
        pass

    current_app.logger.warning("invalid api_key from %s", request.remote_addr)
    return False


# Authentication decorator
def token_required[**P, R](f: Callable[P, R]):
    return decorate_token_required(f, True)


def ro_token_required[**P, R](f: Callable[P, R]):
    return decorate_token_required(f, False)


def with_json_data(requirements: dict[str, type], optional: dict[str, type] = {}):
    return partial(decorate_with_json_data, requirements, optional)


def sanitize(data: dict, requirements: dict[str, type], optional: dict[str, type]) -> dict | None:
    if len(requirements) + len(optional) > 0 and type(data) is not dict:
        current_app.logger.warning("data requirements is not dict")
        return None
    ret = {}
    try:
        for k, t in requirements.items():
            if type(data[k]) is not t:
                current_app.logger.warning(f"required data {k} has wrong type: {type(data[k])} instead of {t}")
                return None
            ret[k] = data[k]
    except KeyError:
        current_app.logger.warning(f"data requirements has missing key {k}")
        return None
    for k, t in optional.items():
        try:
            if data[k] is not None and type(data[k]) is not t:
                current_app.logger.warning(f"optional data {k} has wrong type: {type(data[k])} instead of {t}")
                continue
            ret[k] = data[k]
        except KeyError:
            continue
    current_app.logger.debug("data requirements met")
    return ret


def decorate_with_json_data[**P, R](
    requirements: dict[str, type], optional: dict[str, type], f: Callable[Concatenate[dict, P], R]
):
    @wraps(f)
    def decorator(*args: P.args, **kwargs: P.kwargs):
        try:
            data = json.loads(request.data)
        except json.decoder.JSONDecodeError:
            return make_response(jsonify({"error": "Malformed or missing json body"}), 400)
        sanitized = sanitize(data, requirements, optional)
        if sanitized is None:
            resp = {"requirements": list(requirements)}
            return jsonify(resp), 400

        return f(sanitized, *args, **kwargs)

    return decorator


def decorate_token_required[**P, R](f: Callable[P, R], write_permissions: bool):
    @wraps(f)
    def decorator(*args: P.args, **kwargs: P.kwargs):
        try:
            data = json.loads(request.data)
            # Old way
            if "api_key" in data:
                if not api_key_valid(data["api_key"], write_permissions):
                    return make_response(jsonify({"error": "invalid api_key"}), 401)
                return f(*args, **kwargs)
        except json.decoder.JSONDecodeError:
            pass
        # Authorization Header
        if request.authorization is None:
            return make_response(jsonify({"error": "No Authorization"}), 401)
        elif not api_key_valid(request.authorization.token, write_permissions):
            return make_response(jsonify({"error": "invalid api_key"}), 401)
        return f(*args, **kwargs)

    return decorator
