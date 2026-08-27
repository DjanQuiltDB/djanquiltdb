from django.core.management import call_command
from django.test import override_settings
from djanquiltdb.db import connection
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.testing import ShardingTestCase
from djanquiltdb.utils import State, create_template_schema

from example.models import Shard

SHARD_SCHEMA = 'test_sina'
TEMPLATE_SCHEMA = 'template'

DECLARED_MODULES = {'declared': 'declared.test_migrations_declared_objects'}
PRE_PLUGIN_MODULES = {'declared': 'declared.test_migrations_pre_plugin'}


class DeclaredObjectTestCase(ShardingTestCase):
    available_apps = ['declared', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        create_template_schema(migrate=False)
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

    def function_exists(self, db_name, schema_name):
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM pg_catalog.pg_proc proc
            JOIN pg_catalog.pg_namespace nsp ON proc.pronamespace = nsp.oid
            WHERE proc.proname = %s AND nsp.nspname = %s
            """,
            [db_name, schema_name],
        )
        return cursor.fetchone()[0] == 1

    def assertOnlyPublic(self, exists, db_name):
        self.assertTrue(exists(db_name, PUBLIC_SCHEMA_NAME), db_name)
        self.assertFalse(exists(db_name, SHARD_SCHEMA), db_name)
        self.assertFalse(exists(db_name, TEMPLATE_SCHEMA), db_name)

    def assertOnlySharded(self, exists, db_name):
        self.assertFalse(exists(db_name, PUBLIC_SCHEMA_NAME), db_name)
        self.assertTrue(exists(db_name, SHARD_SCHEMA), db_name)
        self.assertTrue(exists(db_name, TEMPLATE_SCHEMA), db_name)

    @override_settings(MIGRATION_MODULES=DECLARED_MODULES)
    def test_a_declared_function_lands_where_its_annotation_says(self):
        """
        Case: Migrate an AddFunction hinted PUBLIC and one hinted SHARDED.
        Expected: Each function only in the schemas its annotation allows.
        """
        call_command('migrate', 'declared', verbosity=0)

        self.assertOnlyPublic(self.function_exists, 'declared_public')
        self.assertOnlySharded(self.function_exists, 'declared_sharded')

    @override_settings(MIGRATION_MODULES=PRE_PLUGIN_MODULES)
    def test_a_migration_written_before_the_plugin_falls_back_to_the_default_placement(self):
        """
        Case: Migrate an AddFunction recorded without hints (grandfathered in pre-installing this library).
        Expected: It applies, and the function lands on the public schema only.
        """
        call_command('migrate', 'declared', verbosity=0)

        self.assertOnlyPublic(self.function_exists, 'declared_hintless')

    @override_settings(MIGRATION_MODULES=DECLARED_MODULES)
    def test_a_declared_view_lands_where_its_annotation_says(self):
        """
        Case: Migrate an AddView hinted PUBLIC and one hinted SHARDED.
        Expected: Each view only in the schemas its annotation allows.
        """
        call_command('migrate', 'declared', verbosity=0)

        self.assertOnlyPublic(self.relation_exists, 'declared_public_view')
        self.assertOnlySharded(self.relation_exists, 'declared_sharded_view')
