from collections import OrderedDict
from threading import Thread, Event
from unittest import mock

from django.apps import apps
from django.db import connections, ProgrammingError, models, DEFAULT_DB_ALIAS
from django.test import override_settings

from example.models import Organization, Shard
from sharding import State
from sharding.decorators import sharded_model, mirrored_model, public_model
from sharding.router import DynamicDbRouter, set_active_connection, get_active_connection, _active_connection
from sharding.tests import ShardingTestCase, ShardingTransactionTestCase
from sharding.utils import create_schema_on_node, create_template_schema, migrate_schema, \
    ShardingMode


@sharded_model()
class DummyShardedModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'sharding'
        abstract = True


@mirrored_model()
class DummyMirroredModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'sharding'
        abstract = True


class DummyPublicModelManager(models.Manager):
    def get_by_natural_key(self, *args, **kwargs):
        return self.get(*args, **kwargs)


@public_model()
class DummyPublicModel(models.Model):
    test_model = True

    objects = DummyPublicModelManager()

    class Meta:
        app_label = 'sharding'
        abstract = True
        unique_together = [[]]

    def natural_key(self):
        return ()


class DummyNonShardedModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'sharding'
        abstract = True


class ActiveConnectionTestCase(ShardingTransactionTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard1 = Shard.objects.create(state=State.ACTIVE, alias='test1', schema_name='test1', node_name='default')
        self.shard2 = Shard.objects.create(state=State.ACTIVE, alias='test2', schema_name='test2', node_name='default')

        self.active_connections = {}

    def test_get_active_connection_none_set(self):
        """
        Case: Have no active connection set
        Expected: get_active_connection() returns the DEFAULT_DB_ALIAS
        """
        delattr(_active_connection, 'connection')
        self.assertEqual(get_active_connection(), DEFAULT_DB_ALIAS)

    def test_get_active_connection(self):
        """
        Case: Get the result of get_active_connection()
        Expected: Returns the value of _active_connection.connection
        """
        sentinel = object()

        _active_connection.connection = sentinel
        self.assertIs(get_active_connection(), sentinel)

    def test_set_active_connection(self):
        """
        Case: Set the active connection
        Expected: _active_connection.connection is equal to what we set
        """
        sentinel = object()

        set_active_connection(sentinel)
        self.assertIs(get_active_connection(), sentinel)

    def test_multiple_threads(self):
        """
        Case: When having multiple threads, go to a different shard with a context manager in both the threads and
              save the current active connection at that time
        Expected: The active connection should be the one set by that current thread, and should not be shared among
                  threads
        """
        def run(shard, sync_event, wait_for):
            with shard.use() as env:
                sync_event.set()  # Notify that this thread is in a shard context now

                # Do not wait longer than 5 seconds for the other thread to be in the shard context.
                if not wait_for.wait(5):
                    return

                # Save the active connection at this moment for this thread, and save also it's expected value
                self.active_connections[shard] = (get_active_connection(), env.options)

        sync_event1 = Event()
        sync_event2 = Event()
        thread1 = Thread(target=run, kwargs={'shard': self.shard1, 'sync_event': sync_event1, 'wait_for': sync_event2})
        thread2 = Thread(target=run, kwargs={'shard': self.shard2, 'sync_event': sync_event2, 'wait_for': sync_event1})

        thread1.start()
        thread2.start()

        # Wait until the threads are finished
        thread1.join()
        thread2.join()

        self.assertEqual(len(self.active_connections), 2, 'Threads did not start properly')

        # Now assert that the active connection in the thread is the one we expect
        for shard, (active_connection, shard_options) in self.active_connections.items():
            self.assertEqual(
                active_connection,
                shard_options,
                'Active connection for {} is incorrect. Is the active connection thread safe?'.format(shard)
            )


class DynamicDbRouterTestCase(ShardingTestCase):

    def clean_models(self):
        apps.app_configs['sharding'].models = {}

    def setUp(self):
        super().setUp()

        self.addCleanup(self.clean_models)

        # Trick to register abstract models. We want them to be abstract or they will show up in other tests as well.
        apps.app_configs['sharding'].models = OrderedDict([('dummyshardedmodel', DummyShardedModel),
                                                           ('dummymirroredmodel', DummyMirroredModel),
                                                           ('dummypublicmodel', DummyPublicModel),
                                                           ('dummynonshardedmodel', DummyNonShardedModel)])

        self.router = DynamicDbRouter()

    def test_db_for_read_while_not_set(self):
        """
        Case: Call db_for_read with the active_connection being None
        Expected: None returned
        """
        set_active_connection(None)
        self.assertIsNone(self.router.db_for_read(model=mock.MagicMock()))

    def test_db_for_read_while_set(self):
        """
        Case: Call db_for_read with the active_connection being test_node
        Expected: Name of the correct node returned
        """
        set_active_connection('test_node')
        self.assertEqual(self.router.db_for_read(model=mock.MagicMock()), 'test_node')

    def test_db_for_write_while_not_set(self):
        """
        Case: Call db_for_write with the active_connection being None
        Expected: None returned
        """
        set_active_connection(None)
        self.assertIsNone(self.router.db_for_write(model=mock.MagicMock()))

    def test_db_for_write_while_set(self):
        """
        Case: Call db_for_write with the active_connection being test_node
        Expected: Name of the correct node returned
        """
        set_active_connection('test_node')
        self.assertEqual(self.router.db_for_write(model=mock.MagicMock()), 'test_node')

    def test_allow_relation(self):
        """
        Case: Call allow_relation for two models with various modes
        Expected: Correct return value for the given mode combinations
        """
        combinations = [
            ('PUBLIC -> PUBLIC', DummyPublicModel, DummyPublicModel, True),
            ('PUBLIC -> MIRRORED', DummyPublicModel, DummyMirroredModel, True),
            ('PUBLIC -> SHARDED', DummyPublicModel, DummyShardedModel, False),
            ('PUBLIC -> None', DummyPublicModel, DummyNonShardedModel, True),
            ('MIRRORED -> PUBLIC', DummyMirroredModel, DummyPublicModel, True),
            ('MIRRORED -> MIRRORED', DummyMirroredModel, DummyMirroredModel, True),
            ('MIRRORED -> SHARDED', DummyMirroredModel, DummyShardedModel, False),
            ('MIRRORED -> None', DummyMirroredModel, DummyNonShardedModel, True),
            ('SHARDED -> PUBLIC', DummyShardedModel, DummyPublicModel, True),
            ('SHARDED -> MIRRORED', DummyShardedModel, DummyMirroredModel, True),
            ('SHARDED -> SHARDED', DummyShardedModel, DummyShardedModel, True),
            ('SHARDED -> None', DummyShardedModel, DummyNonShardedModel, False),
            ('None -> PUBLIC', DummyNonShardedModel, DummyPublicModel, True),
            ('None -> MIRRORED', DummyNonShardedModel, DummyMirroredModel, True),
            ('None -> SHARDED', DummyNonShardedModel, DummyShardedModel, False),
            ('None -> None', DummyNonShardedModel, DummyNonShardedModel, None),
        ]
        for name, model_from, model_to, result in combinations:
            with self.subTest(name):
                self.assertEqual(self.router.allow_relation(model_to(), model_from()), result)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                 'OVERRIDE_SHARDING_MODE': {
                                     ('sharding', 'dummynonshardedmodel'): ShardingMode.SHARDED,
                                 }})
    def test_allow_relation_between_sharded_models_settings_override_model(self):
        """
        Case: Call allow_relation with two sharded models, the latter is set through the configuration.
        Expected: True, we don't check if they are on the same shard yet.
        """
        self.assertTrue(self.router.allow_relation(DummyShardedModel(), DummyNonShardedModel()))

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                 'OVERRIDE_SHARDING_MODE': {
                                     ('sharding',): ShardingMode.SHARDED,
                                 }})
    def test_allow_relation_between_sharded_models_settings_override_app(self):
        """
        Case: Call allow_relation with two sharded models, the latter is set through the configuration.
        Expected: True, we don't check if they are on the same shard yet.
        """
        self.assertTrue(self.router.allow_relation(Organization(), DummyNonShardedModel()))

    def test_allow_syncdb(self):
        """
        Case: Call allow_syncdb on a normal model
        Expected: None
        """
        self.assertIsNone(self.router.allow_syncdb())

    def test_allow_syncdb_on_test_model(self):
        """
        Case: Call allow_syncdb on a test model
        Expected: None
        """
        self.assertFalse(self.router.allow_syncdb(model=DummyShardedModel))

    def test_allow_migrate(self):
        """
        Case: Migrate a combination of unsharded, mirrored and
              sharded models: namely example.models
        Expected: unsharded to go to default-public.
                  mirrored to go to default-public and other-public
                  sharded to go to default-template, other-template,
                  default-schema1 and other-schema2
        """
        create_template_schema('default')  # Also calls for a migration

        # Mirrored, mapping and django default tables. Note that this also included tables from apps outside those
        # selected for the testcase. Since the initial migration happens before that, and includes all models knows to
        # django.
        default_public_tables = ['django_migrations', 'django_content_type', 'auth_group', 'auth_permission',
                                 'auth_group_permissions', 'example_shard', 'django_session', 'example_type',
                                 'example_supertype', 'example_caketype', 'example_organizationshards',
                                 'example_mirroreduser', 'example_defaultuser', 'migration_tests_supermirroredmodel',
                                 'migration_tests_mirroredmodel']
        # The tables present on all non-default public schema's are all the mirrored tables.
        other_public_tables = ['django_migrations',  'example_shard', 'example_type', 'example_supertype',
                               'example_caketype', 'django_content_type', 'auth_group', 'auth_permission',
                               'auth_group_permissions', 'example_mirroreduser', 'migration_tests_supermirroredmodel',
                               'migration_tests_mirroredmodel']
        # The tables present on the template schema's are all the sharded tables.
        template_tables = ['django_migrations', 'example_organization', 'example_suborganization', 'example_user',
                           'example_statement', 'example_cake', 'example_user_cake', 'example_statement_type']

        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='public'),
                              default_public_tables)
        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='template'), template_tables)
        self.assertCountEqual(connections['other'].get_all_table_headers(schema_name='public'), other_public_tables)

        create_template_schema('other')
        self.assertCountEqual(connections['other'].get_all_table_headers(schema_name='template'), template_tables)

        create_schema_on_node('schema1', node_name='default', migrate=False)
        # Schema is created empty (cause we say: 'migrate=False')
        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='schema1'), [])

        # Obviously, after migration shard schema's have the same tables as the template.
        migrate_schema('default', 'schema1')
        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='schema1'), template_tables)

        create_schema_on_node('schema2', node_name='other', migrate=True)
        self.assertCountEqual(connections['other'].get_all_table_headers(schema_name='schema2'), template_tables)

    def test_not_allow_migrate_sharded_on_public(self):
        """
        Case: Check if sharded model is allowed to migrate on default connection public schema.
        Expected: The sharded model should not be allowed to migrate to the public schema.
        """
        self.assertFalse(self.router.allow_migrate('default', 'example', 'organization'))
        self.assertFalse(self.router.allow_migrate('other', 'example', 'organization'))

    @mock.patch('sharding.router.logger.warning')
    def test_allow_migrate_on_nonexisting_model(self, mock_logger_warning):
        """
        Case: Call allow_migrate for a model that (no longer) exists.
        Expected: Warning to be logged.
        """
        self.router.allow_migrate('default', 'example', 'outer_space')
        self.assertEqual(mock_logger_warning.call_count, 1)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                 'MAPPING_MODEL': 'example.models.OrganizationShards',
                                 'NEW_SHARD_NODE': 'other',
                                 'OVERRIDE_SHARDING_MODE': {
                                     ('example', 'organization'): ShardingMode.MIRRORED,
                                 }})
    def test_allow_migrate_sharded_settings_override_to_mirrored(self):
        """
        Case: Check if previously sharded model is allowed to migrate onto public schema if it is set
              to ShardingMode.MIRRORED through the configuration.
        Expected: The router should allow the overridden model to be migrated onto the public schema.
        """
        self.assertTrue(self.router.allow_migrate('default', 'example', 'organization'))
        self.assertTrue(self.router.allow_migrate('other', 'example', 'organization'))

    def test_allow_migrate_on_none(self):
        """
        Case: Call allow_migrate without a model_name
        Expected: Programming error to be raised
        """
        with self.assertRaises(ProgrammingError):
            self.router.allow_migrate('default', 'example', model_name=None)

    def test_allow_migrate_with_hints(self):
        """
        Case: run_python in migration with hints given
        Expected: Router to route correctly
        """
        self.assertTrue(self.router.allow_migrate('default', 'example', model_name=None,
                                                  sharding_mode=ShardingMode.MIRRORED))
        self.assertFalse(self.router.allow_migrate('default', 'example', model_name=None,
                                                   sharding_mode=ShardingMode.SHARDED))
        self.assertTrue(self.router.allow_migrate('default', 'example', model_name='organization',
                                                  sharding_mode=ShardingMode.MIRRORED))
