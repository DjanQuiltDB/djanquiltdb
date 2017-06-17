import threading

from django.core.management import call_command
from functools import wraps

from django.conf import settings
from django.db import connections, connection
from django.utils.module_loading import import_string

THREAD_LOCAL = threading.local()


class DynamicDbRouter(object):
    """A router that decides what db to read from based on a variable
    local to the current thread.

    source: https://github.com/ambitioninc/django-dynamic-db-router
    credits: ambitioninc

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
    """

    def db_for_read(self, model, **hints):
        override_list = getattr(THREAD_LOCAL, 'DB_OVERRIDE', None)
        if override_list is None:
            return None  # fallback to the default router
        return override_list[-1]

    def db_for_write(self, model, **hints):
        override_list = getattr(THREAD_LOCAL, 'DB_OVERRIDE', None)
        if override_list is None:
            return None
        return override_list[-1]

    def allow_relation(self, *args, **kwargs):
        return True

    def allow_syncdb(self, *args, **kwargs):
        return None

    def allow_migrate(self, *args, **kwargs):
        return None


def _use_connection(node):
    if not hasattr(THREAD_LOCAL, 'DB_OVERRIDE') or not THREAD_LOCAL.DB_OVERRIDE:
        THREAD_LOCAL.DB_OVERRIDE = [node]
    else:
        THREAD_LOCAL.DB_OVERRIDE.append(node)
    return connections[node]


def _set_schema(schema_name, _connection=None):
    if not _connection:
        _connection = connection
    _connection.set_schema(schema_name)


class use_shard:
    def __init__(self, shard=None, node_name=None, schema_name=None):
        shard_class_name = settings.SHARDING['SHARD_CLASS']

        if shard:
            if not isinstance(shard, import_string(shard_class_name)):
                raise ValueError("Shard value {} ({}) must of type {}".format(shard,
                                                                              type(shard).__name__,
                                                                              shard_class_name))

            self.shard = shard
            self.schema_name = shard.schema_name
            self.node = shard.node_name
        else:
            self.schema_name = schema_name
            self.node = node_name


        if self.node not in connections:
            raise ValueError("Connection '{}' does not exist. Is it listed in settings.DATABASES?".format(self.node))

    def __enter__(self):
        # first: set the connection
        self.old_connection_name = connection.settings_dict['NAME']
        self.connection = _use_connection(self.node)

        # second: set the correct search_path for the requested schema
        self.old_schema_name = self.connection.get_schema()

        _set_schema(self.schema_name, self.connection)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # reset both the connection and the schema back to the old situation
        _set_schema(self.old_schema_name, self.connection)
        if not THREAD_LOCAL.DB_OVERRIDE or THREAD_LOCAL.DB_OVERRIDE == [self.node]:
            THREAD_LOCAL.DB_OVERRIDE = None
        else:
            THREAD_LOCAL.DB_OVERRIDE.pop()  # remove last entree, which is self.node

    def __call__(self, querying_func):
        @wraps(querying_func)
        def inner(*args, **kwargs):
            # Call the function in our context manager
            with self:
                return querying_func(*args, **kwargs)

        return inner


def create_schema_on_node(schema_name, node_name, migrate=True):
    """
    Create a schema of a given node. If no node is given, it will take the current one used.
    By default it will also call a migration to the newly made schema.
    """
    if node_name not in connections:
        raise ValueError("Connection '{}' does not exist. Is it listed in settings.DATABASES?".format(node_name))
    cursor = connections[node_name].cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS {};".format(schema_name))

    if migrate:
        with use_shard(node_name=node_name, schema_name=schema_name):
            # The following will create table headers for all models, not just the sharded ones!
            call_command('migrate', database=node_name)  # ensure we migrate using our connection
