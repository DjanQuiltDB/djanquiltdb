from unittest import mock

from django.test import TestCase, SimpleTestCase, override_settings

from sharding.models import get_node_class, get_shard_class, BaseShard
from sharding.tests.app_config import DummyNode, DummyShard


class GetShardTestCase(SimpleTestCase):
    @override_settings(SHARDING={'SHARD_CLASS': 'sharding.tests.app_config.DummyShard'})
    def test_get_shard(self):
        """
        Case: get shard class.
        Expected: Class reference of classname given in the settings.
        """
        self.assertEqual(get_shard_class(), DummyShard)

    @override_settings(SHARDING={'NODE_CLASS': 'sharding.tests.app_config.DummyNode'})
    def test_get_node(self):
        """
        Case: get node class.
        Expected: Class reference of classname given in the settings.
        """
        self.assertEqual(get_node_class(), DummyNode)


class BaseShardTestCase(TestCase):
    @mock.patch('sharding.models.create_schema')
    @mock.patch('sharding.models.models.Model.save')
    def test_save(self, mock_save, mock_create_schema):
        """
        Case: call the save method from the BaseShard model
        Expected: create_schema and super().mock are called
        """
        shard = BaseShard(alias='test_shard', schema_name='test_schema')
        shard.save()
        self.assertTrue(mock_save.called)
        self.assertTrue(mock_create_schema.called)
