import os

import snowflake.sqlalchemy
import sqlalchemy
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)


def read_private_key(path, passphrase):
    with open(path, "rb") as infile:
        key = load_pem_private_key(
            infile.read(), password=passphrase.encode(), backend=default_backend(),
        )
    return key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


def get_engine():
    if "SNOWFLAKE_PRIVATE_KEY_PATH" in os.environ:
        private_key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
        private_key_passphrase = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"]
        connect_args = {
            "private_key": read_private_key(
                private_key_path, private_key_passphrase
            )
        }
    else:
        connect_args = {}
    parameters = {
        "user": os.environ["SNOWFLAKE_USER"],
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
    }
    for param in (
        "role",
        "password",
        "warehouse",
        "cache_column_metadata",
        "client_session_keep_alive",
    ):
        val = os.environ.get(f"SNOWFLAKE_{param.upper()}")
        if val:
            parameters[param] = val
    if "SNOWFLAKE_LOGIN" in os.environ:
        parameters["user"] = os.environ["SNOWFLAKE_LOGIN"]
    url = snowflake.sqlalchemy.URL(**parameters)
    return sqlalchemy.create_engine(url, connect_args=connect_args)
