"""
db connector is used to connect to the database for CRUD operations
used by the respective definitions.
"""
import os
import json
import pymysql
from secret_manager import get_secret

RDS_SECRET_NAME = os.environ.get('RDS_SECRET_NAME')
secret = json.loads(get_secret(RDS_SECRET_NAME))


def db_connector():
    """
    db_connector is used to connect to the database for CRUD operations
    used by the respective definitions.
    """
    my_sql_con = pymysql.connect(
        host=secret.get('url'),
        user=secret.get('username'),
        password=secret.get('password'),
        db=secret.get('schema_name'),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        port=int(secret.get('Port_db')),
    )
    return my_sql_con
