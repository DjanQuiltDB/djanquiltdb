from django.db import ProgrammingError
from django.db.migrations.loader import MigrationLoader
from django.test import SimpleTestCase
from djanquiltdb import ShardingMode
from djanquiltdb.router import DynamicDbRouter
from djanquiltdb.testing import pending_migration_changes
from djanquiltdb.utils import get_template_name
from postgres_denormalize.operations import BackfillDenormalized

# The apps whose migrations this project maintains. django-postgres-denormalize and pgtrigger are dependencies, so their
# migrations are not maintained here.
OWNED_APPS = frozenset({'djanquiltdb', 'example', 'antipattern'})


class BackfillRoutingTestCase(SimpleTestCase):
    def setUp(self):
        migration = MigrationLoader(None, ignore_no_migrations=True).disk_migrations['example', '0002_backfill_tasks']
        [self.operation] = migration.operations
        self.router = DynamicDbRouter()

    def test_the_backfill_runs_in_the_template_and_in_every_shard(self):
        """
        Case: DjanQuiltDB's router is asked whether the backfill runs in the template, in a shard on the primary node
              and in a shard on the other node, with the hints that the operation gives.
        Expected: Yes for each, because the operation gives the model name, from which the router reads the sharding
                  mode of Task.
        """
        for alias in ('default|{}'.format(get_template_name()), 'default|some_shard', 'other|some_shard'):
            with self.subTest(alias=alias):
                self.assertTrue(self.router.allow_migrate(alias, 'example', **self.operation.hints))

    def test_the_backfill_does_not_run_in_the_public_schema(self):
        """
        Case: DjanQuiltDB's router is asked whether the backfill runs in the public schema of the primary node.
        Expected: No, because Task is a sharded model and has no table there.
        """
        self.assertFalse(self.router.allow_migrate('default', 'example', **self.operation.hints))


class BackfillOfAModelNamedWithItsAppLabelTestCase(SimpleTestCase):
    def setUp(self):
        self.router = DynamicDbRouter()

    def test_without_hints_the_router_cannot_place_the_backfill(self):
        """
        Case: DjanQuiltDB's router is asked whether BackfillDenormalized('example.task') runs in the template, with the
              hints that the operation gives.
        Expected: ProgrammingError, because the operation gives no model name, so the router cannot read a sharding
                  mode.
        """
        operation = BackfillDenormalized('example.task')

        with self.assertRaisesMessage(ProgrammingError, 'Cannot determine sharding mode'):
            self.router.allow_migrate('default|{}'.format(get_template_name()), 'example', **operation.hints)

    def test_a_sharding_mode_hint_places_the_backfill_in_the_template_and_in_every_shard(self):
        """
        Case: The router is asked the same for BackfillDenormalized('example.task', hints={'sharding_mode':
              ShardingMode.SHARDED}), for the template, for a shard on each node and for the public schema.
        Expected: Yes for the template and the shards, and no for the public schema.
        """
        operation = BackfillDenormalized('example.task', hints={'sharding_mode': ShardingMode.SHARDED})

        for alias in ('default|{}'.format(get_template_name()), 'default|some_shard', 'other|some_shard'):
            with self.subTest(alias=alias):
                self.assertTrue(self.router.allow_migrate(alias, 'example', **operation.hints))

        self.assertFalse(self.router.allow_migrate('default', 'example', **operation.hints))


class NoPendingModelMigrationsTestCase(SimpleTestCase):
    def test_every_model_change_has_a_migration(self):
        """
        Case: The autodetector compares the models of this project, and the triggers of their denormalized fields, with
              its migrations.
        Expected: Nothing is pending, so a model or trigger change without a matching migration cannot go unnoticed.
        """
        self.assertEqual(pending_migration_changes(OWNED_APPS), {})
