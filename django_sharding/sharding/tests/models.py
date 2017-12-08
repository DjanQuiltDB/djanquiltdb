from unittest import mock

from django.test import TestCase, SimpleTestCase, override_settings

from example.models import Shard
from sharding.models import BaseShard
from sharding.utils import get_shard_class, use_shard, ShardingMode
from sharding.tests.app_config import DummyShard


class GetShardTestCase(SimpleTestCase):
    @override_settings(SHARDING={'SHARD_CLASS': 'sharding.tests.app_config.DummyShard'})
    def test_get_shard(self):
        """
        Case: Get shard class.
        Expected: Class reference of classname given in the settings.
        """
        self.assertEqual(get_shard_class(), DummyShard)


class BaseShardTestCase(TestCase):
    @mock.patch('sharding.utils.create_schema_on_node')
    @mock.patch('sharding.models.models.Model.save')
    def test_save(self, mock_save, mock_create_schema):
        """
        Case: Call the save method from a just created BaseShard model
        Expected: Create_schema and super().mock are called
        """
        shard = Shard(alias='test_shard', schema_name='test_schema', node_name='default')
        shard.save()
        self.assertTrue(mock_save.called)
        self.assertTrue(mock_create_schema.called)
        mock_create_schema.assert_called_with(schema_name='test_schema', node_name='default', migrate=True)

    @mock.patch('sharding.utils.create_schema_on_node')
    def test_save_while_already_created(self, mock_create_schema):
        """
        Case: Call the save method from the BaseShard model which already exists
        Expected: Create_schema and super().mock are NOT called
        """
        shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='default')
        self.assertTrue(mock_create_schema.called)  # called when created
        mock_create_schema.reset_mock()
        shard.save()
        self.assertFalse(mock_create_schema.called)

    @mock.patch('sharding.utils.create_schema_on_node')
    def test_save_on_correct_node(self, mock_create_schema):
        """
        Case: Call the save method from the BaseShard model on the node the schema will reside.
        Expected: Create_schema is called
        """
        with use_shard(node_name='default', schema_name='public'):  # Shard objects are always on public
            shard = Shard(alias='test_shard', schema_name='test_schema', node_name='default')
            shard.save()
            self.assertTrue(mock_create_schema.called)
            mock_create_schema.assert_called_with(schema_name='test_schema', node_name='default', migrate=True)

    @mock.patch('sharding.utils.create_schema_on_node')
    @mock.patch('sharding.models.models.Model.save')
    def test_save_on_different_node_for_non_mirrored(self, mock_save, mock_create_schema):
        """
        Case: Call the save method from the BaseShard model on any other node than where the schema belongs to.
              The Shard model is NOT mirrored.
        Expected: Create_schema is called, and the object is saved.
        """
        with use_shard(node_name='other', schema_name='public'):  # Shard objects are always on public
            shard = Shard(alias='test_shard', schema_name='test_schema', node_name='default')
            shard.save()
            self.assertTrue(mock_create_schema.called)
            self.assertTrue(mock_save.called)

    @mock.patch('sharding.utils.create_schema_on_node')
    @mock.patch('sharding.models.models.Model.save')
    def test_save_on_different_node_for_mirrored_on_wrong_node(self, mock_save, mock_create_schema):
        """
        Case: Call the save method from the BaseShard model on any other node than where the schema belongs to.
              The Shard model IS mirrored.
        Expected: Create_schema is NOT called, but the object is still saved.
        """
        with use_shard(node_name='other', schema_name='public'):  # Shard objects are always on public
            shard = Shard(alias='test_shard', schema_name='test_schema', node_name='default')
            shard.sharding_mode = ShardingMode.MIRRORED
            shard.save()
            self.assertFalse(mock_create_schema.called)
            self.assertTrue(mock_save.called)

    @mock.patch('sharding.utils.create_schema_on_node')
    @mock.patch('sharding.models.models.Model.save')
    def test_save_on_different_node_for_mirrored_on_correct_node(self, mock_save, mock_create_schema):
        """
        Case: Call the save method from the BaseShard model on the node than where the schema belongs to.
              The Shard model IS mirrored.
        Expected: Create_schema is called, and the object is saved.
        """
        with use_shard(node_name='other', schema_name='public'):  # Shard objects are always on public
            shard = Shard(alias='test_shard', schema_name='test_schema', node_name='other')
            shard.sharding_mode = ShardingMode.MIRRORED
            shard.save()
            self.assertTrue(mock_create_schema.called)
            self.assertTrue(mock_save.called)

    def test_clean(self):
        """
        Case: Call the clean method from the BaseShard model with a existing node_name
        Expected: No problems
        """
        shard = BaseShard(alias='test_shard', schema_name='test_schema', node_name='default')
        shard.clean()

    def test_clean_failure(self):
        """
        Case: Call the clean method from the BaseShard model with a nonexisting node_name
        Expected: A valueError to be raised
        """
        shard = BaseShard(alias='test_shard', schema_name='test_schema', node_name='nonexisting')
        with self.assertRaises(ValueError):
            shard.clean()

    @mock.patch('sharding.utils.create_schema_on_node')
    @mock.patch('sharding.models.models.Model.save')
    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'NEW_SHARD_NODE': 'other'})
    def test_use_settings_node(self, mock_save, mock_create_schema):
        """
        Case: Call the save method without setting a node_name.
        Expected: Create_schema called with the node_name given in the settings. Also applied to the Shard object.
        """
        with use_shard(node_name='other', schema_name='public'):
            shard = Shard.objects.create(alias='test_shard', schema_name='test_schema')

        mock_create_schema.assert_called_with(schema_name='test_schema', node_name='other', migrate=True)
        self.assertEqual(shard.node_name, 'other')
        self.assertTrue(mock_save.called)

    @mock.patch('sharding.utils.create_schema_on_node')
    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_no_node_and_no_settings_node(self, mock_create_schema):
        """
        Case: Call the save method without setting a node_name and without one defined in settings.
        Expected: ValueError to be raised
        """
        with self.assertRaises(ValueError):
            Shard.objects.create(alias='test_shard', schema_name='test_schema')
        self.assertFalse(mock_create_schema.called)
