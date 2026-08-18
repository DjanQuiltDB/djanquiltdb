"""
Executing migrations for the operations DjanQuiltDB hints.

The router tests cover ``allow_migrate`` one call at a time. What is covered here is the step after that: a migration
carrying a ``sharding_mode`` hint, run through ``migrate`` the way a project runs it, ends up in the schemas
the hint asked for and nowhere else.
"""

from django.core.management import call_command
from django.test import override_settings

from djanquiltdb.db import connection
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import State, create_template_schema
from djanquiltdb_tests import ShardingTestCase
from example.models import Shard

SHARD_SCHEMA = 'test_sina'
TEMPLATE_SCHEMA = 'template'

HINTED_MODULES = {'migration_tests': 'migration_tests.test_migrations_hinted'}


class MigrationPlacementTestCase(ShardingTestCase):
    """
    A template and a shard to migrate into, and per-schema assertions that do not lean on the search path.

    A shard reaches the public schema through its search path, so anything listing what is visible from a shard also
    lists what is only in public. Counting the catalogue by schema is the only way to tell where an object really is.
    """

    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        create_template_schema(migrate=False)
        create_template_schema('other', migrate=False)

        self.sina = Shard.objects.create(
            alias='sina', schema_name=SHARD_SCHEMA, node_name='default', state=State.ACTIVE
        )

    def relation_exists(self, db_name, schema_name):
        """
        Whether a table or a view of that name lives in that schema. Both are relations, so one query covers them.
        """
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM pg_catalog.pg_class cls
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE cls.relname = %s AND nsp.nspname = %s
            """,
            [db_name, schema_name],
        )
        return cursor.fetchone()[0] == 1

    def assertPublic(self, exists, db_name):
        """
        In the public schema and in neither the shard nor the template it is cloned from.
        """
        self.assertTrue(exists(db_name, PUBLIC_SCHEMA_NAME), db_name)
        self.assertFalse(exists(db_name, SHARD_SCHEMA), db_name)
        self.assertFalse(exists(db_name, TEMPLATE_SCHEMA), db_name)

    def assertSharded(self, exists, db_name):
        """
        In the shard and in the template, so that a shard created later inherits it, and never in public.
        """
        self.assertFalse(exists(db_name, PUBLIC_SCHEMA_NAME), db_name)
        self.assertTrue(exists(db_name, SHARD_SCHEMA), db_name)
        self.assertTrue(exists(db_name, TEMPLATE_SCHEMA), db_name)


class HintedOperationTestCase(MigrationPlacementTestCase):
    """
    The operations that carry a sharding mode of their own, rather than inheriting one from a model.
    """

    @override_settings(MIGRATION_MODULES=HINTED_MODULES)
    def test_a_hinted_run_sql_lands_where_its_hint_says(self):
        """
        Case: Migrate a RunSQL hinted PUBLIC and one hinted SHARDED, each creating a table.
        Expected: Each table only in the schemas its hint allows. Until now only the unhinted case was covered, which
                  asserts the refusal rather than the placement.
        """
        call_command('migrate', 'migration_tests', verbosity=0)

        self.assertPublic(self.relation_exists, 'hinted_public')
        self.assertSharded(self.relation_exists, 'hinted_sharded')

    @override_settings(MIGRATION_MODULES=HINTED_MODULES)
    def test_a_hinted_run_python_lands_where_its_hint_says(self):
        """
        Case: Migrate a RunPython hinted SHARDED, creating a table through the schema editor it is handed.
        Expected: The table in the shard and the template only. The hint reaches the router from RunPython exactly as
                  it does from RunSQL, and the operation runs once per schema it is allowed in.
        """
        call_command('migrate', 'migration_tests', verbosity=0)

        self.assertSharded(self.relation_exists, 'hinted_python')
