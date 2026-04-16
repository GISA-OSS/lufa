import json
from abc import ABC, abstractmethod
from typing import Collection, Optional, TypedDict

from lufa.database import DatabaseManager


class User(TypedDict):
    distinguished_name: str
    username: str
    data: dict[str, str]


class UserRepository(ABC):
    @abstractmethod
    def save_user(self, username: str, distinguished_name: str, data: dict[str, str]) -> None:
        pass

    @abstractmethod
    def get_user(self, user_dn: Collection[str]) -> Optional[User]:
        pass


class SqliteUserRepository(UserRepository):
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def save_user(self, username: str, distinguished_name: str, data: dict[str, str]) -> None:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        data_json = json.dumps(dict(data), default=str)

        cursor.execute(
            """
                        INSERT INTO lufa_users (
                            distinguished_name,
                            username,
                            data
                        )
                        VALUES (?, ?, ?)
                        ON CONFLICT (distinguished_name) DO UPDATE
                            SET (username, data) = (?, ?);
                        """,
            (distinguished_name, username, data_json, username, data_json),
        )

        conn.commit()

    def get_user(self, user_dn: Collection[str]) -> Optional[User]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
                        SELECT
                            distinguished_name,
                            username,
                            data
                        FROM lufa_users
                        WHERE distinguished_name = ?;
                        """,
            (user_dn,),
        )
        # cursor.rowcount ist immer -1..
        ret = cursor.fetchone()
        if ret is not None:
            ret["data"] = json.loads(ret["data"])
        return ret


class PostgresUserRepository(UserRepository):
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def save_user(self, username: str, distinguished_name: str, data: dict[str, str]) -> None:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()
        data_json = json.dumps(dict(data), default=str)

        cursor.execute(
            """
                        INSERT INTO lufa_users (
                            distinguished_name,
                            username,
                            data
                        )
                        VALUES (%s, %s, %s)
                        ON CONFLICT (distinguished_name) DO UPDATE
                            SET (username, data) = (%s, %s);
                        """,
            (distinguished_name, username, data_json, username, data_json),
        )

        conn.commit()

    def get_user(self, user_dn: Collection[str]) -> Optional[User]:
        conn = self.db_manager.get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
                        SELECT
                            distinguished_name,
                            username,
                            data
                        FROM lufa_users
                        WHERE distinguished_name = %s;
                        """,
            (user_dn,),
        )
        # cursor.rowcount ist immer -1..
        return cursor.fetchone()
