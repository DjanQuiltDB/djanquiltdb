from unittest import mock

from django.test import TestCase, SimpleTestCase, override_settings

from example.models import Shard, Organization, User
from sharding import ShardingMode, State
from sharding.models import BaseShard
from sharding.tests.utils import ShardingTestCase
from sharding.utils import get_shard_class, use_shard, create_template_schema
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
    def test_save_with_pk(self, mock_create_schema):
        """
        Case: Create a shard object with a given pk
        Expected: Create_schema and super().mock are called
        """
        shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='default', pk=123)
        self.assertTrue(mock_create_schema.called)  # mock_create_schema is called when created.
        self.assertEqual(shard.pk, 123)
        mock_create_schema.reset_mock()
        shard.save()
        self.assertFalse(mock_create_schema.called)

    @mock.patch('sharding.utils.create_schema_on_node')
    def test_save_while_already_created(self, mock_create_schema):
        """
        Case: Call the save method from the BaseShard model which already exists
        Expected: Create_schema and super().mock are NOT called
        """
        shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='default')
        self.assertTrue(mock_create_schema.called)  # mock_create_schema is called when created.
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


class ShardedModelMethodUseShardTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()

    def test_post_init(self):
        """
        Case: Initialize model instance in a use shard context
        Expected: _schema_name, _node_name and _shard to be set on the instance
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)

        with use_shard(shard):
            org = Organization.objects.create(name='The Empire')
            user = User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=org)

        self.assertEqual(org._schema_name, 'empire_schema')
        self.assertEqual(org._node_name, 'default')
        self.assertEqual(user._schema_name, 'empire_schema')
        self.assertEqual(user._node_name, 'default')

    def test_model_method(self):
        """
        Case: Use a model method to query a related object that is living on the same shard as the object, while being
              in a different shard
        Expected: The model method is performed in the shard the model instance is living on
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)
        other_shard = Shard.objects.create(alias='dantooine', schema_name='alliance_schema', node_name='default',
                                           state=State.ACTIVE)

        with use_shard(shard):
            org = Organization.objects.create(name='The Empire')
            user = User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=org)

        # Now switch to another shard where the user and organization are not living
        with use_shard(other_shard):
            # And this one uses the user and organization are living on (it would give a DoesNotExist if it would be
            # performed on other_shard)
            self.assertEqual(user.get_organization_name(), 'The Empire')

    def test_override_model_use_shard(self):
        """
        Case: Use a model method while being a shard with override_model_use_shard=True
        Expected: The model method is not performed in a use_shard context of the shard the model instance is living on
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)
        other_shard = Shard.objects.create(alias='dantooine', schema_name='alliance_schema', node_name='default',
                                           state=State.ACTIVE)

        with use_shard(shard):
            org = Organization.objects.create(name='The Empire')
            user = User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=org)

        with use_shard(other_shard):
            other_org = Organization.objects.create(name='The Rebel Alliance')

        self.assertEqual(org.id, other_org.id)

        # Now switch to another shard where the user and organization are not living
        with use_shard(other_shard, override_model_use_shard=True):
            # Since we provided override_model_use_shard=True, this one now queries the organization that is living on
            # other_shard with the same ID as the organization that is living on shard.
            self.assertEqual(user.get_organization_name(), 'The Rebel Alliance')
