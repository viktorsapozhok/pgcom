from functools import wraps

from psycopg2 import sql

from pgcom import Commuter


class ConnParams:
    def __init__(self):
        self.host = 'postgresql'
        self.port = '5432'
        self.user = 'postgres'
        self.password = 'postgres'
        self.dbname = 'test'

    def get(self):
        return self.__dict__


conn_params = ConnParams().get()
commuter = Commuter(**conn_params)


def delete_table(table_name, schema='public'):
    if commuter.is_table_exist(table_name, schema=schema):
        commuter.execute(
            sql.SQL("DROP TABLE {} CASCADE").format(
                sql.Identifier(schema, table_name)))


def with_table(table_name, create_callback, *create_args, schema='public'):
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            delete_table(table_name, schema=schema)
            try:
                cmd = create_callback(table_name, *create_args, schema=schema)
                commuter.execute(cmd)
                func(*args, **kwargs)
            except Exception:
                assert False
            finally:
                delete_table(table_name, schema=schema)
        return wrapped
    return decorator
