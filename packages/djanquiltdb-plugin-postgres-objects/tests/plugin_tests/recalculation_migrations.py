"""
The recalculation of a stored generated column driven by the real ``migrate`` command.

Everywhere else in this suite a recalculation is applied by hand, once per schema, inside an explicit ``use_shard``.
That shows what the operation does, but not how often DjanQuiltDB runs it: the per-shard iteration comes from the
test's own loop. Here nothing loops. The command walks the shards itself, so a regression that rewrote the column once
globally instead of once per shard shows up as a shard left holding stale values.

The ``declared`` app carries no models, so the sharded model these migrations create lives in migration state only and
is routed by ``OVERRIDE_SHARDING_MODE`` rather than by a ``@sharded_model`` decorator. Both reach the same branch of
the router; that a decorator resolves to the same mode is covered by RecalculationPlacementTestCase.
"""

from io import StringIO

from django.conf import settings
from django.core.management import call_command
from django.test import override_settings
from djanquiltdb import ShardingMode
from djanquiltdb.db import connection
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.testing import ShardingTestCase
from djanquiltdb.utils import State, create_template_schema, get_template_name, use_shard

from example.models import Shard

SHARD_SCHEMAS = ('test_sina', 'test_kavi')

TABLE_NAME = 'declared_crumb'
FIELD_NAME = 'name_uppercased'

RECALCULATION_MODULES = {'declared': 'declared.test_migrations_recalculation'}

#: The app the fixture migrations below live in holds no models, so the router cannot read a sharding mode off one.
#: The settings override is the documented way to give an app a placement without a decorated model.
SHARDED_DECLARED = {
    **settings.QUILT_DB,
    'OVERRIDE_SHARDING_MODE': {
        **settings.QUILT_DB.get('OVERRIDE_SHARDING_MODE', {}),
        ('declared',): ShardingMode.SHARDED,
    },
}


@override_settings(MIGRATION_MODULES=RECALCULATION_MODULES, QUILT_DB=SHARDED_DECLARED)
class RecalculationMigrationTestCase(ShardingTestCase):
    available_apps = ['declared', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        create_template_schema(migrate=False)
        for alias, schema_name in zip(('sina', 'kavi'), SHARD_SCHEMAS):
            Shard.objects.create(alias=alias, schema_name=schema_name, node_name='default', state=State.ACTIVE)

    def migrate(self, target=None):
        """
        Migrate the fixture app, and fail on a schema that did not make it.

        The command keeps going when one schema errors, so that the other shards still get their migration, and reports
        what went wrong on stderr. Without reading that back, a shard left behind would show up here only as a puzzling
        stale value.
        """
        stderr = StringIO()
        call_command('migrate', 'declared', *filter(None, [target]), verbosity=0, stderr=stderr)

        self.assertEqual(stderr.getvalue(), '')

    def insert(self, schema_name, name):
        with use_shard(node_name='default', schema_name=schema_name) as env:
            env.connection.cursor().execute('INSERT INTO {} (name) VALUES (%s)'.format(TABLE_NAME), [name])

    def stored_values(self, schema_name):
        with use_shard(node_name='default', schema_name=schema_name) as env:
            cursor = env.connection.cursor()
            cursor.execute('SELECT {} FROM {} ORDER BY id'.format(FIELD_NAME, TABLE_NAME))
            return [value for (value,) in cursor.fetchall()]

    def table_exists(self, schema_name):
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM pg_catalog.pg_class cls
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE cls.relname = %s AND nsp.nspname = %s
            """,
            [TABLE_NAME, schema_name],
        )
        return cursor.fetchone()[0] == 1

    def test_migrate_rewrites_the_column_on_every_shard(self):
        """
        Case: Migrate a body change to a PUBLIC function together with the recalculation of a sharded model's stored
              generated column computed with it, across two shards holding a row each.
        Expected: Both shards hold what the new body computes. The command reaches each shard on its own, so neither
                  is left behind by a rewrite that ran somewhere else.
        """
        self.migrate('0001')

        for schema_name in SHARD_SCHEMAS:
            self.insert(schema_name, 'cake')
            self.assertEqual(self.stored_values(schema_name), ['CAKE'])

        self.migrate()

        for schema_name in SHARD_SCHEMAS:
            self.assertEqual(self.stored_values(schema_name), ['CAKE!'])

    def test_migrate_leaves_the_column_out_of_the_public_schema(self):
        """
        Case: Migrate the same pair, then ask which schemas ended up with the table.
        Expected: The template and both shards have it and the public schema does not, so the rewrite above had no
                  public copy to run against either.
        """
        self.migrate()

        self.assertTrue(self.table_exists(get_template_name()))
        for schema_name in SHARD_SCHEMAS:
            self.assertTrue(self.table_exists(schema_name))

        self.assertFalse(self.table_exists(PUBLIC_SCHEMA_NAME))
