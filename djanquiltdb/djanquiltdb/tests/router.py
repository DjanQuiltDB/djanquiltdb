from collections import OrderedDict
from threading import Event, Thread
from unittest import mock

from django.apps import apps
from django.db import DEFAULT_DB_ALIAS, ProgrammingError, connections, models
from django.test import override_settings
from example.models import (
    CakeType,
    DefaultUser,
    MirroredUser,
    Organization,
    OrganizationShards,
    Shard,
    Statement,
    SuperType,
    Type,
    Unrelated,
)

from djanquiltdb import State
from djanquiltdb.decorators import mirrored_model, override_sharding_setting, public_model, sharded_model
from djanquiltdb.options import ShardOptions
from djanquiltdb.router import DynamicDbRouter, _active_connection, get_active_connection, set_active_connection
from djanquiltdb.tests import ShardingTestCase, ShardingTransactionTestCase
from djanquiltdb.utils import ShardingMode, create_schema_on_node, create_template_schema, migrate_schema, use_shard


@sharded_model()
class DummyShardedModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'djanquiltdb'
        abstract = True


@mirrored_model()
class DummyMirroredModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'djanquiltdb'
        abstract = True


class DummyPublicModelManager(models.Manager):
    def get_by_natural_key(self, *args, **kwargs):
        return self.get(*args, **kwargs)


@public_model()
class DummyPublicModel(models.Model):
    test_model = True

    objects = DummyPublicModelManager()

    class Meta:
        app_label = 'djanquiltdb'
        abstract = True
        unique_together = [[]]

    def natural_key(self):
        return ()


class DummyNonShardedModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'djanquiltdb'
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
                'Active connection for {} is incorrect. Is the active connection thread safe?'.format(shard),
            )


class DynamicDbRouterTestCase(ShardingTestCase):
    def clean_models(self):
        apps.app_configs['djanquiltdb'].models = {}

    def setUp(self):
        super().setUp()

        self.addCleanup(self.clean_models)

        # Trick to register abstract models. We want them to be abstract or they will show up in other tests as well.
        apps.app_configs['djanquiltdb'].models = OrderedDict(
            [
                ('dummyshardedmodel', DummyShardedModel),
                ('dummymirroredmodel', DummyMirroredModel),
                ('dummypublicmodel', DummyPublicModel),
                ('dummynonshardedmodel', DummyNonShardedModel),
            ]
        )

        self.router = DynamicDbRouter()

    def test_db_for_read_while_not_set(self):
        """
        Case: Call db_for_read with the active_connection being None
        Expected: None returned
        """
        set_active_connection(None)
        self.assertIsNone(self.router.db_for_read(model=DummyNonShardedModel))

    def test_db_for_read_while_set(self):
        """
        Case: Call db_for_read with the active_connection being test_node
        Expected: Name of the correct node returned
        """
        set_active_connection('test_node')
        self.assertEqual(self.router.db_for_read(model=DummyNonShardedModel), 'test_node')

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    @mock.patch('djanquiltdb.router.get_model_definition')
    def test_db_for_read_for_routing_to_primary_db(self, mock_get_model_definition):
        """
        Case: Call db_for_read while the given model has routing_to_primary_db of True and False
        Expected: Directly routed to the primary db when set to True, normal flow when set to False.
        """
        with self.subTest('route_to_primary_db=True'):

            class DummyModelClass:
                route_to_primary_db = True

            mock_get_model_definition.return_value = DummyModelClass

            self.assertEqual(self.router.db_for_read(model=OrganizationShards), 'other')

        with self.subTest('route_to_primary_db=False'):

            class DummyModelClass:
                route_to_primary_db = False

            mock_get_model_definition.return_value = DummyModelClass

            self.assertEqual(self.router.db_for_read(model=OrganizationShards), 'default')

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    @mock.patch('djanquiltdb.router.get_model_sharding_mode')
    @mock.patch('djanquiltdb.router.DynamicDbRouter.db_for_read', return_value='read_answer')
    def test_db_for_write(self, mock_db_for_read, mock_get_model_sharding_mode):
        """
        Case: Call db_for_write
        Expected: Given model's sharding mode checked, db_for_read called when it's not mirrored
        """
        with self.subTest('Mirrored model'):
            mock_get_model_sharding_mode.return_value = ShardingMode.MIRRORED

            self.assertEqual(self.router.db_for_write(model='some_model', hints='shard_options'), 'other')
            mock_get_model_sharding_mode.assert_called_once_with('some_model')
            self.assertEqual(mock_db_for_read.call_count, 1)

            mock_get_model_sharding_mode.reset_mock()
            mock_db_for_read.reset_mock()

        with self.subTest('Non mirrored model'):
            mock_get_model_sharding_mode.return_value = ShardingMode.SHARDED

            self.assertEqual(self.router.db_for_write(model='some_model'), 'read_answer')
            mock_get_model_sharding_mode.assert_called_once_with('some_model')
            mock_db_for_read.assert_called_once_with('some_model')

            mock_get_model_sharding_mode.reset_mock()
            mock_db_for_read.reset_mock()

    @override_sharding_setting('PRIMARY_DB_ALIAS', 'default')
    @mock.patch('djanquiltdb.router.DynamicDbRouter.db_for_read')
    def test_db_for_write_mirrored_model(self, mock_db_for_read):
        """
        Case: Call db_for_write for a mirrored model.
        Expected: db_for_read called and returned if its result is a ShardOptions for the primary connection.
                  Otherwise return the primary connection name.
        """
        with self.subTest('With primary node as ShardingObject in context'):
            mock_db_for_read.reset_mock()

            with use_shard(node_name='default', schema_name='public') as env:
                mock_db_for_read.return_value = env.options
                self.assertEqual(self.router.db_for_write(model=DummyMirroredModel), env.options)

            self.assertEqual(mock_db_for_read.call_count, 2)

        with self.subTest('With primary node as a string in context'):
            mock_db_for_read.reset_mock()

            with use_shard(node_name='default', schema_name='public'):
                mock_db_for_read.return_value = 'default'
                self.assertEqual(self.router.db_for_write(model=DummyMirroredModel), 'default')

            mock_db_for_read.assert_called_once_with(DummyMirroredModel)

        with self.subTest('With non-primary node as ShardingObject in context'):
            mock_db_for_read.reset_mock()

            with use_shard(node_name='other', schema_name='public') as env:
                mock_db_for_read.return_value = env.options
                self.assertEqual(self.router.db_for_write(model=DummyMirroredModel), 'default')

            mock_db_for_read.assert_called_once_with(DummyMirroredModel)

        with self.subTest('With non-primary node as string in context'):
            mock_db_for_read.reset_mock()

            with use_shard(node_name='other', schema_name='public'):
                mock_db_for_read.return_value = 'other'
                self.assertEqual(self.router.db_for_write(model=DummyMirroredModel), 'default')

            mock_db_for_read.assert_called_once_with(DummyMirroredModel)

    def test_db_for_write_while_not_set(self):
        """
        Case: Call db_for_write with the active_connection being None.
        Expected: None returned.
        """
        set_active_connection(None)
        self.assertIsNone(self.router.db_for_write(model=DummyNonShardedModel))

    def test_db_for_write_while_set(self):
        """
        Case: Call db_for_write with the active_connection being test_node
        Expected: Name of the correct node returned
        """
        set_active_connection('test_node')
        self.assertEqual(self.router.db_for_write(model=DummyNonShardedModel), 'test_node')

    def assert_save_table(self, mock_save_table, model, node_name, schema_name, options=True):
        mock_save_table.assert_called_once()
        call_args = mock_save_table.call_args[0]
        self.assertFalse(call_args[0])  # raw SQL
        self.assertEqual(call_args[1], model)  # Model class
        self.assertFalse(call_args[2])  # force_inert
        self.assertFalse(call_args[3])  # force_update
        if options:
            self.assertIsInstance(call_args[4], ShardOptions)  # using
            self.assertEqual(call_args[4].node_name, node_name)
            self.assertEqual(call_args[4].schema_name, schema_name)
        else:
            self.assertEqual(call_args[4], node_name)
        self.assertFalse(call_args[5])  # update_fields

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    @mock.patch('django.db.models.base.Model._save_table')
    def test_db_for_write_system_with_sharding_context(self, mock_save_table):
        """
        Case: Call a write for various types of models from inside a use_shard context.
        Expected: All writes to happen on the connection of the context, except mirrored models: those are routed to
                  the primary connection, and route_to_primary_db mapping tables are written to the primary db when
                  set to True.
        Note: System test
        """
        create_template_schema('default')
        self.test_shard = Shard.objects.create(
            alias='a_shard', node_name='default', schema_name='test_other_schema', state=State.ACTIVE
        )
        with use_shard(self.test_shard):
            with self.subTest('Non-sharded model'):
                mock_save_table.reset_mock()
                obj = DefaultUser()
                obj.save()
                self.assert_save_table(mock_save_table, DefaultUser, 'default', 'test_other_schema')

            with self.subTest('Sharded model'):
                mock_save_table.reset_mock()
                with mock.patch('djanquiltdb.tests.router.Organization._save_table') as mockery:
                    obj = Organization(name='example')
                    obj.save()
                    self.assert_save_table(mockery, Organization, 'default', 'test_other_schema')

            with self.subTest('Public model'):
                mock_save_table.reset_mock()
                obj = CakeType()
                obj.save()
                self.assert_save_table(mock_save_table, CakeType, 'default', 'test_other_schema')

            with self.subTest('Mirrored model'):
                mock_save_table.reset_mock()
                obj = MirroredUser()
                obj.save()
                self.assert_save_table(mock_save_table, MirroredUser, 'other', None, options=False)

            with self.subTest('route_to_primary_db=True'):
                mock_save_table.reset_mock()
                obj = OrganizationShards()
                obj.save()
                self.assert_save_table(mock_save_table, OrganizationShards, 'other', None, options=False)

            with self.subTest('route_to_primary_db=False'):
                mock_save_table.reset_mock()
                OrganizationShards.route_to_primary_db = False

                obj = OrganizationShards()
                obj.save()
                self.assert_save_table(mock_save_table, OrganizationShards, 'default', 'test_other_schema')

                # cleanup
                OrganizationShards.route_to_primary_db = True

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    @mock.patch('django.db.models.base.Model._save_table')
    def test_db_for_write_system_without_sharding_context(self, mock_save_table):
        """
        Case: Call a write for various types of models, outside of a use_shard context.
        Expected: All writes to happen on the default connection -which is what is normally active in
                  test cases,- except mirrored models: those are routed to the primary connection.
                  Oh and route_to_primary_db mapping tables are written to the primary db when set to True :).
        Note: System test.
        """
        with self.subTest('Non-sharded model'):
            mock_save_table.reset_mock()
            obj = DefaultUser()
            obj.save()
            self.assert_save_table(mock_save_table, DefaultUser, 'default', None, options=False)

        with self.subTest('Sharded model'):
            mock_save_table.reset_mock()
            with mock.patch('djanquiltdb.tests.router.Organization._save_table') as mockery:
                obj = Organization(name='example')
                obj.save()
                self.assert_save_table(mockery, Organization, 'default', None, options=False)
            # Note: This will lead to a DB error if not mocked, since the table would not exist on the public schema

        with self.subTest('Public model'):
            mock_save_table.reset_mock()
            obj = CakeType()
            obj.save()
            self.assert_save_table(mock_save_table, CakeType, 'default', None, options=False)

        with self.subTest('Mirrored model'):
            mock_save_table.reset_mock()
            obj = MirroredUser()
            obj.save()
            self.assert_save_table(mock_save_table, MirroredUser, 'other', None, options=False)

        with self.subTest('route_to_primary_db=True'):
            mock_save_table.reset_mock()
            obj = OrganizationShards()
            obj.save()
            self.assert_save_table(mock_save_table, OrganizationShards, 'other', None, options=False)

        with self.subTest('route_to_primary_db=False'):
            mock_save_table.reset_mock()
            OrganizationShards.route_to_primary_db = False

            obj = OrganizationShards()
            obj.save()
            self.assert_save_table(mock_save_table, OrganizationShards, 'default', None, options=False)

            # cleanup
            OrganizationShards.route_to_primary_db = True

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    def test_db_for_write_end_to_end(self):
        """
        Case: Call a write for various types of models.
        Expected: All writes to happen on the connection of the context, except mirrored models: those are routed to
                  the primary connection.
        Note: System test.
        """
        create_template_schema('default')
        self.test_shard = Shard.objects.create(
            alias='a_shard', node_name='default', schema_name='test_other_schema', state=State.ACTIVE
        )

        with self.subTest('Non-sharded model'):
            # In the example app, the Unrelated model is not denoted with a sharding mode.
            # Not specifically routed. So requires to be called from the correct context
            with use_shard(node_name='default', schema_name='public'):
                Unrelated.objects.create(name='disinterested party')

            with use_shard(node_name='default', schema_name='public'):
                self.assertTrue(Unrelated.objects.filter(name='disinterested party').exists())
            # Testing this fails on node 'other' will leave the connection in an erroneous state,
            # and let all subsequent tests fail.

        with self.subTest('Sharded model'):
            # Only on the shard, the table does exist on public schemas.
            with use_shard(self.test_shard):
                Organization.objects.create(name='test_cake')

            with use_shard(node_name='default', schema_name='test_other_schema'):
                self.assertTrue(Organization.objects.filter(name='test_cake').exists())

        with self.subTest('Public model'):
            with use_shard(self.test_shard):
                SuperType.objects.create(name='test_cake')

            with use_shard(node_name='default', schema_name='public'):
                self.assertTrue(SuperType.objects.filter(name='test_cake').exists())

            with use_shard(node_name='other', schema_name='public'):
                self.assertFalse(SuperType.objects.filter(name='test_cake').exists())

        with self.subTest('Mirrored model'):
            with use_shard(self.test_shard):
                Type.objects.create(name='test_cake')

            with use_shard(node_name='default', schema_name='public'):
                self.assertFalse(Type.objects.filter(name='test_cake').exists())

            with use_shard(node_name='other', schema_name='public'):
                self.assertTrue(Type.objects.filter(name='test_cake').exists())

        with self.subTest('route_to_primary_db=True'):
            # the 'other' database does not even have OrganizationShard as a table.
            with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'default'}):
                with use_shard(self.test_shard):
                    organization = Organization.objects.create(name='stay')

                with use_shard(node_name='other', schema_name='public'):
                    OrganizationShards.objects.create(shard=self.test_shard, organization_id=organization.id)

                with use_shard(node_name='default', schema_name='public'):
                    self.assertTrue(OrganizationShards.objects.filter(organization_id=organization.id).exists())

                # cleanup
                OrganizationShards.objects.all().delete()

        with self.subTest('route_to_primary_db=False'):
            # the 'other' database does not even have OrganizationShard as a table.
            with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'default'}):
                with use_shard(self.test_shard):
                    organization = Organization.objects.create(name='stay')

                with use_shard(node_name='other', schema_name='public'):
                    OrganizationShards.route_to_primary_db = False
                    with self.assertRaisesMessage(
                        ProgrammingError, 'relation "example_organizationshards" does not exist'
                    ):
                        OrganizationShards.objects.create(shard=self.test_shard, organization_id=organization.id)

                with use_shard(node_name='default', schema_name='public'):
                    self.assertFalse(OrganizationShards.objects.filter(organization_id=organization.id).exists())

                # cleanup
                OrganizationShards.route_to_primary_db = True

    def test_allow_relation(self):
        """
        Case: Call allow_relation for two models with various modes.
        Expected: Correct return value for the given mode combinations.
        """
        combinations = [
            ('PUBLIC -> PUBLIC', CakeType, CakeType, True),
            ('PUBLIC -> MIRRORED', CakeType, MirroredUser, True),
            ('PUBLIC -> SHARDED', CakeType, Statement, False),
            ('PUBLIC -> None', CakeType, DefaultUser, True),
            ('MIRRORED -> PUBLIC', MirroredUser, CakeType, True),
            ('MIRRORED -> MIRRORED', MirroredUser, MirroredUser, True),
            ('MIRRORED -> SHARDED', MirroredUser, Statement, False),
            ('MIRRORED -> None', MirroredUser, DefaultUser, True),
            ('SHARDED -> PUBLIC', Statement, CakeType, True),
            ('SHARDED -> MIRRORED', Statement, MirroredUser, True),
            ('SHARDED -> SHARDED', Statement, Statement, True),
            ('SHARDED -> None', Statement, DefaultUser, False),
            ('None -> PUBLIC', DefaultUser, CakeType, True),
            ('None -> MIRRORED', DefaultUser, MirroredUser, True),
            ('None -> SHARDED', DefaultUser, Statement, False),
            ('None -> None', DefaultUser, DefaultUser, None),
        ]
        for name, model_from, model_to, result in combinations:
            with self.subTest(name):
                self.assertEqual(self.router.allow_relation(model_to(), model_from()), result)

    @override_settings(
        SHARDING={
            'SHARD_CLASS': 'example.models.Shard',
            'OVERRIDE_SHARDING_MODE': {
                ('djanquiltdb', 'simplemodel'): ShardingMode.SHARDED,
            },
        }
    )
    def test_allow_relation_between_sharded_models_settings_override_model(self):
        """
        Case: Call allow_relation with two sharded models, the latter is set through the configuration.
        Expected: True, we don't check if they are on the same shard yet.
        """

        class SimpleModel(models.Model):
            class Meta:
                app_label = 'djanquiltdb'

        self.assertTrue(self.router.allow_relation(Organization(), SimpleModel()))

    @override_settings(
        SHARDING={
            'SHARD_CLASS': 'example.models.Shard',
            'OVERRIDE_SHARDING_MODE': {
                ('djanquiltdb',): ShardingMode.SHARDED,
            },
        }
    )
    def test_allow_relation_between_sharded_models_settings_override_app(self):
        """
        Case: Call allow_relation with two sharded models, the latter is set through the configuration.
        Expected: True, we don't check if they are on the same shard yet.
        """

        class ShardedByDefaultModel(models.Model):
            class Meta:
                app_label = 'djanquiltdb'

        self.assertTrue(self.router.allow_relation(Organization(), ShardedByDefaultModel()))

    def test_allow_syncdb(self):
        """
        Case: Call allow_syncdb on a normal model.
        Expected: None.
        """
        self.assertIsNone(self.router.allow_syncdb())

    def test_allow_syncdb_on_test_model(self):
        """
        Case: Call allow_syncdb on a test model.
        Expected: None.
        """
        self.assertFalse(self.router.allow_syncdb(model=DummyShardedModel))

    def test_allow_migrate(self):
        """
        Case: Migrate a combination of unsharded, mirrored and
              sharded models: namely example.models.
        Expected: unsharded to go to default-public.
                  mirrored to go to default-public and other-public
                  sharded to go to default-template, other-template,
                  default-schema1 and other-schema2.
        """
        create_template_schema('default')  # Also calls for a migration

        # Mirrored, mapping and django default tables. Note that this also included tables from apps outside those
        # selected for the testcase. Since the initial migration happens before that, and includes all models knows to
        # django.
        default_public_tables = [
            'django_migrations',
            'django_content_type',
            'auth_group',
            'auth_permission',
            'auth_group_permissions',
            'example_shard',
            'django_session',
            'example_type',
            'example_supertype',
            'example_caketype',
            'example_coatingtype',
            'example_coatingtype_super_type',
            'example_organizationshards',
            'example_mirroreduser',
            'example_mirroreduser_type',
            'example_defaultuser',
            'migration_tests_supermirroredmodel',
            'migration_tests_mirroredmodel',
            'example_unrelated',
        ]
        # The tables present on all non-default public schema's are all the mirrored tables.
        other_public_tables = [
            'django_migrations',
            'example_shard',
            'example_type',
            'example_supertype',
            'example_coatingtype',
            'example_coatingtype_super_type',
            'example_caketype',
            'django_content_type',
            'auth_group',
            'auth_permission',
            'auth_group_permissions',
            'example_mirroreduser',
            'example_mirroreduser_type',
            'migration_tests_supermirroredmodel',
            'migration_tests_mirroredmodel',
        ]
        # The tables present on the template schema's are all the sharded tables.
        template_tables = [
            'django_migrations',
            'example_organization',
            'example_suborganization',
            'example_user',
            'example_statement',
            'example_cake',
            'example_user_cake',
            'example_statement_type',
        ]

        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='public'), default_public_tables)
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

    @mock.patch('djanquiltdb.router.logger.warning')
    def test_allow_migrate_on_nonexisting_model(self, mock_logger_warning):
        """
        Case: Call allow_migrate for a model that (no longer) exists.
        Expected: Warning to be logged.
        """
        self.router.allow_migrate('default', 'example', 'outer_space')
        self.assertEqual(mock_logger_warning.call_count, 1)

    @override_settings(
        SHARDING={
            'SHARD_CLASS': 'example.models.Shard',
            'MAPPING_MODEL': 'example.models.OrganizationShards',
            'NEW_SHARD_NODE': 'other',
            'OVERRIDE_SHARDING_MODE': {
                ('example', 'organization'): ShardingMode.MIRRORED,
            },
        }
    )
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
        Case: Call allow_migrate without a model_name.
        Expected: Programming error to be raised.
        """
        with self.assertRaises(ProgrammingError):
            self.router.allow_migrate('default', 'example', model_name=None)

    def test_allow_migrate_with_hints(self):
        """
        Case: run_python in migration with hints given
        Expected: Router to route correctly
        """
        self.assertTrue(
            self.router.allow_migrate('default', 'example', model_name=None, sharding_mode=ShardingMode.MIRRORED)
        )
        self.assertFalse(
            self.router.allow_migrate('default', 'example', model_name=None, sharding_mode=ShardingMode.SHARDED)
        )
        self.assertTrue(
            self.router.allow_migrate(
                'default', 'example', model_name='organization', sharding_mode=ShardingMode.MIRRORED
            )
        )


class DynamicDbRouterSystemTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema('default')
        self.instance = Type.objects.create(name='Wrecker')

    def test_del_mirrored_model_with_relation(self):
        """
        Case: Delete a mirrored model that has a relation.
        Expected: Model gets correctly deleted.
        """
        with use_shard(node_name='default', schema_name='template'):
            Type.objects.filter(id=self.instance.id).delete()
            self.assertFalse(Type.objects.exists())
