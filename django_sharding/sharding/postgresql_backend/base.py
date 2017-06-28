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

from django.db.backends.postgresql_psycopg2.base import DatabaseWrapper as BaseDatabaseWrapper
from django.db.utils import DatabaseError, IntegrityError
from psycopg2 import InternalError


clone_function = """
CREATE OR REPLACE FUNCTION clone_schema(source_schema text, dest_schema text) RETURNS void AS
$BODY$
DECLARE 
  objeto text;
  buffer text;
BEGIN
    FOR objeto IN
        SELECT TABLE_NAME::text FROM information_schema.TABLES WHERE table_schema = source_schema
    LOOP        
        buffer := dest_schema || '.' || objeto;
        EXECUTE 'CREATE TABLE ' || buffer || ' (LIKE ' || source_schema || '.' || objeto || ' INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS)';
        EXECUTE 'INSERT INTO ' || buffer || '(SELECT * FROM ' || source_schema || '.' || objeto || ')';
    END LOOP;
 
END;
$BODY$
LANGUAGE plpgsql VOLATILE;
"""


class DatabaseWrapper(BaseDatabaseWrapper):
    """
    Adds the capability to manipulate the search_path using set_schema and set_schema_to_public
    """

    include_public_schema = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.clone_function_set = False
        self.schema_name = None
        self.search_path_set = False
        self.set_schema_to_public()

    def close(self):
        self.search_path_set = False
        super().close()

    def rollback(self):
        super().rollback()
        # Django's rollback clears the search path so we have to set it again the next time.
        self.search_path_set = False

    def set_schema(self, schema_name):
        """
        Main API method to current database schema,
        but it does not actually modify the db connection.
        """
        self.schema_name = schema_name
        self.search_path_set = False

    def set_schema_to_public(self):
        """
        Instructs to stay in the common 'public' schema.
        """
        self.schema_name = None
        self.search_path_set = False

    def get_schema(self):
        return self.schema_name

    def get_ps_schema(self, shard_name, _cursor=None):
        cursor = _cursor or self.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);',
                       [shard_name])
        if cursor.fetchall()[0][0]:
            return shard_name

    def get_all_pg_schemas(self, _cursor=None):
        cursor = _cursor or self.cursor()
        cursor.execute('SELECT schema_name FROM information_schema.schemata;')
        return cursor.fetchall()

    def create_schema(self, schema_name):
        cursor = self.cursor()
        cursor.execute(
            'CREATE SCHEMA IF NOT EXISTS "{}";'.format(schema_name))  # params cannot be used for schema names

    def clone_schema(self, from_schema, to_schema):
        cursor = self.cursor()
        if not self.get_ps_schema(from_schema, cursor):
            raise ValueError("Schema '{}' does not exist on node '{}'.".format(from_schema, self))
        if not self.get_ps_schema(to_schema, cursor):
            raise ValueError("Schema '{}' does not exist on node '{}'.".format(from_schema, self))

        self.set_clone_function()

        cursor.execute("SELECT clone_schema(%s, %s);", [from_schema, to_schema])

    def set_clone_function(self, _cursor=None):
        # cursor = self.cursor()
        cursor = _cursor or self.cursor()

        if not self.clone_function_set:
            cursor.execute(clone_function)
            self.clone_function_set = True
        else:
            cursor.execute("SELECT pg_get_functiondef('clone_schema(text, text)'::regprocedure);")

    def _cursor(self, name=None):
        """Database cursor to write whatever we want.

        Typically used for migrations, this function will check
        to see if SCHEMA_NAME is set or not. If it is, then it
        will create it if it doesn't yet exist. Finally, it will
        point to that schema.
        """
        if name:
            # Only supported and required by Django 1.11 (server-side cursor)
            cursor = super(DatabaseWrapper, self)._cursor(name=name)
        else:
            cursor = super(DatabaseWrapper, self)._cursor()

        if self.search_path_set:
            return cursor

        if self.schema_name:
            # confirm that the schema exists.
            if not self.get_ps_schema(self.schema_name, cursor):
                raise IntegrityError("Schema '{}' does not exist.".format(self.schema_name))

        # Since all schemas contain all tables for now, we disable the public schema to test content separation.
        # search_paths = [self.schema_name, 'public'] if self.schema_name else ['public']
        search_paths = [self.schema_name] if self.schema_name else ['public']

        if name:
            # Named cursor can only be used once
            cursor_for_search_path = self.connection.cursor()
        else:
            # Reuse
            cursor_for_search_path = cursor

        # In the event that an error already happened in this transaction and we are going
        # to rollback we should just ignore database error when setting the search_path
        # if the next instruction is not a rollback it will just fail also, so
        # we do not have to worry that it's not the good one
        try:
            cursor_for_search_path.execute('SET search_path = %s', [','.join(search_paths)])
        except (DatabaseError, InternalError):
            self.search_path_set = False
        else:
            self.search_path_set = True

        if name:
            cursor_for_search_path.close()

        return cursor

    def _start_transaction_under_autocommit(self):
        pass
