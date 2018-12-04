from unittest import mock

from django.db import connections, ProgrammingError, models
from django.test import override_settings

from example.models import Organization
from sharding.decorators import sharded_model, mirrored_model
from sharding.router import DynamicDbRouter
from sharding.tests import ShardingTestCase
from sharding.utils import create_schema_on_node, create_template_schema, migrate_schema, \
    ShardingMode


@sharded_model()
class DummyShardedModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'sharding'
        managed = False


@mirrored_model()
class DummyMirroredModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'sharding'
        managed = False


class DummyNonShardedModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'sharding'
        managed = False


class DynamicDbRouterTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        self.router = DynamicDbRouter()

    def test_db_for_read_while_not_set(self):
        """
        Case: Call db_for_read with the active_connection being None
        Expected: None returned
        """
        DynamicDbRouter.active_connection = None
        self.assertIsNone(self.router.db_for_read(model=mock.MagicMock()))

    def test_db_for_read_while_set(self):
        """
        Case: Call db_for_read with the active_connection being test_node
        Expected: Name of the correct node returned
        """
        DynamicDbRouter.active_connection = 'test_node'
        self.assertEqual(self.router.db_for_read(model=mock.MagicMock()), 'test_node')

    def test_db_for_write_while_not_set(self):
        """
        Case: Call db_for_write with the active_connection being None
        Expected: None returned
        """
        DynamicDbRouter.active_connection = None
        self.assertIsNone(self.router.db_for_write(model=mock.MagicMock()))

    def test_db_for_write_while_set(self):
        """
        Case: Call db_for_write with the active_connection being test_node
        Expected: Name of the correct node returned
        """
        DynamicDbRouter.active_connection = 'test_node'
        self.assertEqual(self.router.db_for_write(model=mock.MagicMock()), 'test_node')

    def test_allow_relation_between_non_sharded_models(self):
        """
        Case: Call allow_relation with two models that are not sharded
        Expected: None, the router does not care about non-sharded models.
        """
        self.assertIsNone(self.router.allow_relation(DummyNonShardedModel(), DummyNonShardedModel()))

    def test_allow_relation_between_sharded_and_non_sharded_models(self):
        """
        Case: Call allow_relation with a sharded and non-sharded model.
        Expected: False, such relationship is not allowed
        """
        self.assertFalse(self.router.allow_relation(DummyShardedModel(), DummyNonShardedModel()))

    def test_allow_relation_between_sharded_and_mirrored_models(self):
        """
        Case: Call allow_relation with a sharded and mirrored model.
        Expected: True, mirrored exists in the public schema.
        """
        self.assertTrue(self.router.allow_relation(DummyShardedModel(), DummyMirroredModel()))

    def test_allow_relation_between_sharded_models(self):
        """
        Case: Call allow_relation with two sharded models
        Expected: True, we don't check if they are on the same shard yet.
        """
        self.assertTrue(self.router.allow_relation(DummyShardedModel(), DummyShardedModel()))

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                 'OVERRIDE_SHARDING_MODE': {
                                     ('sharding', 'dummynonshardedmodel'): ShardingMode.MIRRORED,
                                 }})
    def test_allow_relation_between_sharded_models_settings_override_model(self):
        """
        Case: Call allow_relation with two sharded models, the latter is set through the configuration.
        Expected: True, we don't check if they are on the same shard yet.
        """
        self.assertTrue(self.router.allow_relation(DummyShardedModel(), DummyNonShardedModel()))

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                 'OVERRIDE_SHARDING_MODE': {
                                     ('sharding',): ShardingMode.MIRRORED,
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

        # Mirrored, mapping and django default tables
        default_public_tables = ['django_migrations', 'django_content_type', 'auth_group', 'auth_permission',
                                 'auth_group_permissions', 'example_shard', 'django_session', 'example_type',
                                 'example_supertype', 'example_organizationshards', 'example_mirroreduser',
                                 'example_defaultuser']
        # The tables present on all non-default public schema's are all the mirrored tables.
        other_public_tables = ['django_migrations',  'example_type', 'example_supertype', 'django_content_type',
                               'auth_group', 'auth_permission', 'auth_group_permissions', 'example_mirroreduser']
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
