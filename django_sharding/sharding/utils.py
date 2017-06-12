from functools import wraps

from django.db import connection


class use_shard:
    def __init__(self, name):
        self.shard_name = name

    def __enter__(self):
        self.old_schema_name = connection.get_schema()
        connection.set_schema(self.shard_name)
        return self.shard_name

    def __exit__(self, exc_type, exc_value, exc_traceback):
        connection.set_schema(self.old_schema_name)

    def __call__(self, querying_func):
        @wraps(querying_func)
        def inner(*args, **kwargs):
            # Call the function in our context manager
            with self:
                return querying_func(*args, **kwargs)

        return inner


def create_schema(schema_name):
    cursor = connection.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS {};".format(schema_name))
