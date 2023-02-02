"""
Taken, changed and adopted from:
    https://github.com/bernardopires/django-tenant-schemas/blob/master/tenant_schemas/postgresql_backend/base.py
Credits goes to bernardopires

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""
import logging
import re

from django.conf import settings
from django.db.backends.base.base import NO_DB_ALIAS
from django.db.backends.postgresql_psycopg2.base import DatabaseWrapper as BaseDatabaseWrapper
from django.db.utils import DatabaseError, IntegrityError
from django.utils.module_loading import import_string
from psycopg2 import InternalError, sql

from sharding.postgresql_backend.introspection import DatabaseSchemaIntrospection
from sharding.postgresql_backend.utils import CursorDebugWrapper, CursorWrapper

logger = logging.getLogger(__name__)

# Clone function is from the PostgreSQL wiki by Emanuel '3manuek'.
# Adjusted to set the value of the created sequences to the same value as those we clone.
clone_function = """
CREATE OR REPLACE FUNCTION clone_schema(source_schema TEXT, dest_schema TEXT) RETURNS VOID AS
$BODY$
DECLARE
  default_ TEXT;
  column_ TEXT;
  name_ TEXT;
  child_column_ TEXT;
  dest_table TEXT;
  dest_table_path TEXT;
  parent_table_ TEXT;
  parent_schema_ TEXT;
  parent_column_ TEXT;

BEGIN
  /* Create all sequences that exist on the source schema on the target schema.  */
  FOR dest_table IN
    SELECT sequence_name::text FROM information_schema.SEQUENCES WHERE sequence_schema = source_schema
  LOOP
    EXECUTE 'CREATE SEQUENCE ' || dest_schema || '.' || dest_table;
    EXECUTE format('SELECT setval(%L, (SELECT last_value FROM %I.%I), (SELECT is_called FROM %I.%I))',
      dest_schema || '.' || dest_table, source_schema, dest_table, source_schema, dest_table);
  END LOOP;

  FOR dest_table IN
    SELECT TABLE_NAME::text FROM information_schema.TABLES WHERE table_schema = source_schema
  LOOP
    dest_table_path := dest_schema || '.' || dest_table;
    /* Create all table on the target schema. */
    EXECUTE 'CREATE TABLE ' || dest_table_path || ' (LIKE ' || source_schema || '.' || dest_table || ' INCLUDING ALL)';
    EXECUTE 'INSERT INTO ' || dest_table_path || '(SELECT * FROM ' || source_schema || '.' || dest_table || ')';

    /* For all tables, link the fields default value to their respective sequences made earlier. */
    FOR column_, default_ IN
      SELECT column_name::TEXT, regexp_replace(column_default::TEXT, source_schema, dest_schema)
        FROM information_schema.COLUMNS WHERE table_schema = dest_schema AND TABLE_NAME = dest_table
        AND column_default LIKE 'nextval(%' || source_schema || '%)'
    LOOP
      EXECUTE 'ALTER TABLE ' || dest_table_path || ' ALTER COLUMN ' || column_ || ' SET DEFAULT ' || default_;
    END LOOP;
  END LOOP;

  /* For all tables, create their foreign key constraints.
     Do not use the information_schema for this. The views there are very slow. We use pg_catalog directly.
     This endeavor has two sources:
     hielkehoeve on feb 2014 - for the constraint cloning loop: https://gist.github.com/hielkehoeve/8818562 .
     Cervo on may 2015 - for the pg_catalog query: https://stackoverflow.com/a/30178351 .
     */
  FOR dest_table IN
    SELECT TABLE_NAME::text FROM information_schema.TABLES WHERE table_schema = source_schema
  LOOP
    dest_table_path := dest_schema || '.' || dest_table;
    FOR name_, child_column_, parent_schema_, parent_table_, parent_column_ IN
      SELECT
        con.constraint_name AS "name_",
        pg_attribute2.attname AS "child_column_",
        /* Replace the source schema with destination schema. Keep others schema's (like 'public') in tact. */
        REPLACE (pg_namespace_outer.nspname, source_schema, dest_schema) AS "parent_schema_",
        pg_class.relname AS "parent_table_",
        pg_attribute1.attname AS "parent_column_"
      FROM
        ( SELECT
            unnest(pg_constraint.conkey) as "parent",
            unnest(pg_constraint.confkey) as "child",
            pg_constraint.conname as constraint_name,
            pg_constraint.confrelid,
            pg_constraint.conrelid
          FROM pg_catalog.pg_class AS pg_class
            JOIN pg_catalog.pg_namespace AS pg_namespace_inner ON pg_namespace_inner.oid = pg_class.relnamespace
            JOIN pg_catalog.pg_constraint AS pg_constraint ON pg_constraint.conrelid = pg_class.oid
          WHERE  pg_constraint.contype = 'f'  /* foreign key */
            AND pg_namespace_inner.nspname = source_schema
            AND pg_class.relname = dest_table  /* child_table */
            AND pg_class.relkind = 'r' /* ordinary table, */
        ) AS con
        JOIN pg_catalog.pg_attribute AS pg_attribute1 ON pg_attribute1.attrelid = con.confrelid
                                                      AND pg_attribute1.attnum = con.child
        JOIN pg_catalog.pg_class AS pg_class ON pg_class.oid = con.confrelid
        JOIN pg_catalog.pg_attribute AS pg_attribute2 ON pg_attribute2.attrelid = con.conrelid
                                                      AND pg_attribute2.attnum = con.parent
        JOIN pg_catalog.pg_namespace AS pg_namespace_outer ON pg_namespace_outer.oid = pg_class.relnamespace
    LOOP
      EXECUTE 'ALTER TABLE ' || dest_table_path || ' ADD CONSTRAINT ' || name_ || '
        FOREIGN KEY (' || child_column_ || ')
        REFERENCES ' || parent_schema_ || '.' || parent_table_ || ' (' || parent_column_ || ')
        DEFERRABLE INITIALLY DEFERRED';
    END LOOP;
  END LOOP;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;
"""

PUBLIC_SCHEMA_NAME = 'public'


def get_validated_schema_name(schema_name, is_template=False):
    from sharding.utils import get_template_name  # Prevent cyclic imports

    if not isinstance(schema_name, str):
        raise ValueError("Schema name '{}' needs to be a string".format(schema_name))

    if not re.match(r'^[A-Za-z][0-9A-Za-z_]*$', schema_name):
        raise ValueError("Schema name '{}' contains illegal characters and/or does not start with a letter"
                         .format(schema_name))

    if not is_template and schema_name == get_template_name():
        raise ValueError("Schema name '{}' cannot be the same as the template name '{}' ".format(schema_name,
                                                                                                 get_template_name()))
    if schema_name in [PUBLIC_SCHEMA_NAME, 'information_schema', 'default']:
        raise ValueError("Schema name '{}' is not allowed ".format(schema_name))

    if schema_name.startswith('pg_'):
        raise ValueError("Schema name '{}' is not allowed to mimic PostgreSQL native schema names "
                         "(starting with 'pg_')".format(schema_name))

    return schema_name


def get_database_creation_class():
    return import_string(
        settings.SHARDING.get('DATABASE_CREATION_CLASS', 'sharding.postgresql_backend.creation.DatabaseCreation')
    )


class DatabaseWrapper(BaseDatabaseWrapper):
    """
    Adds the capability to manipulate the search_path using set_schema and set_schema_to_public
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Replace the default introspection with a patched version of
        # the DatabaseIntrospection that only returns the table list
        # for the currently selected schema.
        self.introspection = DatabaseSchemaIntrospection(self)

        # Give the library user the possibility to overwrite the database creation class. This allows them to adjust the
        # creation of the test database, to make a default shard for example.
        self.creation = get_database_creation_class()(self)

        self.current_search_paths = [PUBLIC_SCHEMA_NAME]

        self.schema_name = PUBLIC_SCHEMA_NAME
        self.include_public_schema = True

        # Django < 2.0 require this attribute set, in our case it should be just a noop.
        if hasattr(self, '_start_transaction_under_autocommit'):
            self._start_transaction_under_autocommit = (lambda x: None)

    def __str__(self):
        return self.alias

    def close(self):
        self.current_search_paths = [PUBLIC_SCHEMA_NAME]
        super().close()

    def rollback(self):
        super().rollback()
        self.current_search_paths = [PUBLIC_SCHEMA_NAME]

    def get_schema(self):
        return self.schema_name

    def is_public_schema(self):
        return self.schema_name == PUBLIC_SCHEMA_NAME

    def get_ps_schema(self, schema_name, _cursor=None):
        cursor = _cursor or self.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);',
                       [schema_name])
        if cursor.fetchall()[0][0]:
            return schema_name

    def get_all_pg_schemas(self, _cursor=None):
        return self.introspection.get_schema_names(_cursor or self.cursor())

    def get_all_table_headers(self, schema_name=None, _cursor=None):
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_type='BASE TABLE';",
            [schema]
        )
        return [x[0] for x in cursor.fetchall()]  # We get a list of single tuples

    def get_all_table_sequences(self, schema_name=None, _cursor=None):
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        cursor.execute(
            "SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema=%s;",
            [schema]
        )
        return [x[0] for x in cursor.fetchall()]  # We get a list of single tuples

    def truncate_all_tables(self, schema_name=None, _cursor=None):
        cursor = _cursor or self.cursor()
        table_headers = self.get_all_table_headers(schema_name, cursor)
        cursor.execute('TRUNCATE ONLY {} CASCADE;'.format(', '.join('"{}"'.format(header) for header in table_headers)))

    def flush_schema(self, schema_name=None, _cursor=None):
        """
        Drops all tables on the given schema
        """
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        for table in self.get_all_table_headers(schema_name=schema):
            cursor.execute('DROP TABLE "{}" CASCADE'.format(table))
            cursor.execute('DROP SEQUENCE IF EXISTS "{}_id_seq" CASCADE'.format(table))

    def get_schema_for_model(self, model, _cursor=None):
        """
        Returns the schema the given model lives on.
        """
        cursor = _cursor or self.cursor()
        cursor.execute('SELECT table_schema FROM information_schema.tables WHERE table_name=%s;',
                       [model._meta.db_table])
        return cursor.fetchall()

    def get_schema_for_sequence(self, sequence_name, _cursor=None):
        cursor = _cursor or self.cursor()
        cursor.execute('SELECT sequence_schema FROM information_schema.sequences WHERE sequence_name=%s;',
                       [sequence_name])
        return cursor.fetchall()

    def create_schema(self, schema_name, is_template=False):
        schema_name = get_validated_schema_name(schema_name, is_template=is_template)
        cursor = self.cursor()
        cursor.execute(
            'CREATE SCHEMA IF NOT EXISTS "{}";'.format(schema_name))  # Params cannot be used for schema names

    def delete_schema(self, schema_name, is_template=False):
        schema_name = get_validated_schema_name(schema_name, is_template=is_template)
        cursor = self.cursor()
        cursor.execute('DROP SCHEMA "{}" CASCADE;'.format(schema_name))  # Params cannot be used for schema names

    def clone_schema(self, from_schema, to_schema):
        cursor = self.cursor()
        if not self.get_ps_schema(from_schema, cursor):
            raise ValueError("Schema '{}' does not exist on node '{}'.".format(from_schema, self))
        if not self.get_ps_schema(to_schema, cursor):
            raise ValueError("Schema '{}' does not exist on node '{}'.".format(from_schema, self))

        self.set_clone_function()

        cursor.execute("SELECT clone_schema(%s, %s);", [from_schema, to_schema])

    def set_clone_function(self, _cursor=None):
        cursor = _cursor or self.cursor()
        cursor.execute(clone_function)

    def reset_sequence(self, model_list, _cursor=None):
        from django.db import models

        cursor = _cursor or self.cursor()
        statements = []
        qn = self.ops.quote_name
        for model in model_list:
            # Use `coalesce` to set the sequence for each model to the max pk value if there are records,
            # or 1 if there are none. Set the `is_called` property (the third argument to `setval`) to true
            # if there are records (as the max pk value is already in use), otherwise set it to false.

            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    statements.append(
                        "SELECT setval('{s}', coalesce(max({f}), 1), max({f}) IS NOT null) FROM {qnm}"  # nosec
                        .format(
                            s='{}_{}_seq'.format(model._meta.db_table, f.column),
                            f=qn(f.column),
                            qnm=qn(model._meta.db_table)
                        )
                    )
                    break  # Only one AutoField is allowed per model, so don't bother continuing.
            for f in model._meta.many_to_many:
                # Django < 2.0
                remote_field = 'rel' if hasattr(f, 'rel') else 'remote_field'
                if not getattr(f, remote_field).through:
                    statements.append(
                        "SELECT setval('{s}', coalesce(max({f}), 1), max({f}) IS NOT null) FROM {qnm}"  # nosec
                        .format(
                            s='{}_{}_seq'.format(f.m2m_db_table(), 'id'),
                            f=qn('id'),
                            qnm=qn(f.m2m_db_table())
                        )
                    )
        cursor.execute(';\n'.join(statements))

    def make_debug_cursor(self, cursor, skip_lock=False):
        """
        Creates a cursor that logs all queries in self.queries_log, and that can set advisory locks as well.
        """
        return CursorDebugWrapper(cursor, self, lock=getattr(self, 'lock_on_execute', False) and not skip_lock)

    def make_cursor(self, cursor, skip_lock=False):
        """
        Creates a cursor without debug logging, and that can set advisory locks as well.
        """
        return CursorWrapper(cursor, self, lock=getattr(self, 'lock_on_execute', False) and not skip_lock)

    def acquire_advisory_lock(self, key, shared=True, _cursor=None):
        """
        Set a shared or exclusive advisory lock on a given key.
        """
        cursor = _cursor or self.cursor()
        cursor.acquire_advisory_lock(key, shared=shared)

    def release_advisory_lock(self, key, shared=True, _cursor=None):
        """
        Release a shared or exclusive advisory lock on a given key.
        """
        cursor = _cursor or self.cursor()
        cursor.release_advisory_lock(key, shared=shared)

    def cursor(self):
        """
        Note that this is backported from Django 1.11 to have the same behaviour between Django 1.8 to Django 1.11.
        """
        return self._cursor()

    def _cursor(self, name=None):
        """Database cursor to write whatever we want.

        Typically used for migrations, this function will check
        to see if SCHEMA_NAME is set or not. If it is, then it
        will create it if it doesn't yet exist. Finally, it will
        point to that schema.

        Check for a connection and see if it is usable. If not: close the connection and the Super() class will
        automatically reconnect in its _cursor() function.
        """
        if self.connection is not None and self.errors_occurred:
            if not self.is_usable():
                logger.warning('Database connection is unusable. Reconnecting and continuing.')
                self.close()

        cursor = self._get_cursor(name=name)

        if self.include_public_schema and self.schema_name != PUBLIC_SCHEMA_NAME:
            search_paths = [self.schema_name, PUBLIC_SCHEMA_NAME]
        else:
            search_paths = [self.schema_name]

        # No need to set search paths for operations without a database,
        # or when there are no changes to the selected schemas.
        if self.alias == NO_DB_ALIAS or self.current_search_paths == search_paths:
            return cursor

        with self._get_cursor(name=name, skip_lock=True) as cursor_for_get_ps_schema:
            if self.schema_name != PUBLIC_SCHEMA_NAME \
                    and not self.get_ps_schema(self.schema_name, cursor_for_get_ps_schema):
                raise IntegrityError("Schema '{}' does not exist.".format(self.schema_name))

        with self._get_cursor(name=name, skip_lock=True) as cursor_for_search_path:
            # In the event that an error already happened in this transaction and we are going
            # to rollback we should just ignore database error when setting the search_path
            # if the next instruction is not a rollback it will just fail also, so
            # we do not have to worry that it's not the good one
            try:
                sql_ = 'SET search_path = {}'.format(','.join(sql.Identifier(x).string for x in search_paths))
                cursor_for_search_path.execute(sql_)
                logger.debug(sql_)
            except (DatabaseError, InternalError):
                logger.warning('Something went wrong with setting the search path.', exc_info=True)
            else:
                self.current_search_paths = search_paths

        return cursor

    def _get_cursor(self, name=None, skip_lock=False):
        """
        Copied from Django 1.11's _cursor() method with the addition of `skip_lock`. Note that this is different from
        the _cursor() method from previous versions. For this library to work easily with multiple Django versions, we
        backported this from Django 1.11.
        """
        self.ensure_connection()
        with self.wrap_database_errors:
            cursor = self.create_cursor(name)
            return self._prepare_cursor(cursor, skip_lock=skip_lock)

    def _prepare_cursor(self, cursor, skip_lock=False):
        """
        Validate the connection is usable and perform database cursor wrapping. Copied from Django 1.11, but with an
        addition of `skip_lock`, which will return a cursor that doesn't do locking if `skip_lock` is True.
        """
        self.validate_thread_sharing()
        if self.queries_logged:
            wrapped_cursor = self.make_debug_cursor(cursor, skip_lock=skip_lock)
        else:
            wrapped_cursor = self.make_cursor(cursor, skip_lock=skip_lock)
        return wrapped_cursor


class ShardDatabaseWrapper(DatabaseWrapper):
    """
    Wrapper around DatabaseWrapper that handles shard routing. This class shares the connection of the database wrapper
    it wraps (from now one called the main connection). This ensures that we are not making a new connection to the
    database each time we switch to a certain schema name. In order to do this, we route all calls to the properties
    defined in _PROXY_FIELDS to the main connection instance (which are the same fields as in the constructor of
    DjangoBaseDatabaseWrapper, excluding `alias`, but including `current_search_paths`).

    Note that this class should not be used for connections to the public schema. You can use the main connection for
    that one.
    """
    _PROXY_FIELDS = ('connection', 'settings_dict', 'queries_log', 'force_debug_cursor', 'autocommit',
                     'in_atomic_block', 'savepoint_state', 'savepoint_ids', 'commit_on_exit', 'needs_rollback',
                     'close_at', 'closed_in_transaction', 'errors_occurred', '_thread_ident', 'current_search_paths',
                     'run_on_commit', 'run_commit_hooks_on_set_autocommit_on')

    def __init__(self, main_connection, options):
        self._main_connection = main_connection
        self.shard_options = options

        # We proxy the fields specified in _PROXY_FIELDS to the main connection. Because we call the super().__init__
        # here, that means that we would reset the values of the fields we proxy. We don't want that here, so we keep
        # track whether we are in the initialization state and only set the proxy values outside of the __init__ method.
        self._initialized = False
        super().__init__(
            settings_dict=main_connection.settings_dict,
            alias=main_connection.alias,
        )
        self._initialized = True

        if self.shard_options.schema_name == PUBLIC_SCHEMA_NAME:
            raise ValueError('Connection to the public schema should be handled by the default DatabaseWrapper.')

        self.schema_name = options.schema_name
        self.include_public_schema = options.kwargs.get('include_public', True)

        # Determine whether we need to set an advisory lock or not. If use_shard on the options is True, this means that
        # we activated this connection in a context manager, meaning that we already activated the lock and we don't
        # have to do that in the cursor's execute method.
        self.lock_on_execute = bool(options.lock and not options.use_shard and options.lock_keys)

        # Django < 2.2 #30171
        if not hasattr(self._main_connection, '_thread_sharing_lock'):
            self._PROXY_FIELDS = self._PROXY_FIELDS + ('allow_thread_sharing',)

        # Django > 2.0
        if hasattr(self._main_connection, 'execute_wrappers'):
            self._PROXY_FIELDS = self._PROXY_FIELDS + ('execute_wrappers',)

    @property
    def alias(self):
        return '{}|{}'.format(self._main_connection.alias, self.schema_name)

    @alias.setter
    def alias(self, value):
        if value != self._main_connection.alias:
            raise ValueError('The alias is managed by the main connection and cannot be changed.')

    def __getattribute__(self, item):
        if item in ShardDatabaseWrapper._PROXY_FIELDS:
            return getattr(self._main_connection, item)
        return super().__getattribute__(item)

    def __setattr__(self, key, value):
        # We don’t want to reset the attributes on the main connection when initializing this class instance, hence we
        # check on the value of self._initialized here.
        if key in ShardDatabaseWrapper._PROXY_FIELDS and self._initialized:
            return setattr(self._main_connection, key, value)
        return super().__setattr__(key, value)

    def acquire_locks(self, shared=True):
        for key in self.shard_options.lock_keys:
            self.acquire_advisory_lock(key, shared=shared)

    def release_locks(self, shared=True):
        for key in self.shard_options.lock_keys:
            self.release_advisory_lock(key, shared=shared)
