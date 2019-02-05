from unittest import mock

from django.db.models.signals import post_init
from django.test import SimpleTestCase, override_settings

from example.models import Shard, Organization, User, Type
from sharding import ShardingMode, State
from sharding.models import BaseShard
from sharding.router import get_active_connection
from sharding.tests import ShardingTestCase
from sharding.tests.app_config import DummyShard
from sharding.utils import get_shard_class, use_shard, create_template_schema, create_schema_on_node


class GetShardTestCase(SimpleTestCase):
    @override_settings(SHARDING={'SHARD_CLASS': 'sharding.tests.app_config.DummyShard'})
    def test_get_shard(self):
        """
        Case: Get shard class.
        Expected: Class reference of classname given in the settings.
        """
        self.assertEqual(get_shard_class(), DummyShard)


class BaseShardTestCase(ShardingTestCase):
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

    def test_create_schema_exists(self):
        """
        Case: Save a new shard while the schema already exists
        Expected: create_schema_on_node not called
        """
        create_template_schema(node_name='default')
        create_schema_on_node(node_name='default', schema_name='test_schema')

        with mock.patch('sharding.utils.create_schema_on_node') as mock_create_schema:
            shard = Shard(alias='test_shard', schema_name='test_schema', node_name='default')
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
        Case: Call the save method from the BaseShard model on the node the schema belongs to.
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

    def test_use(self):
        """
        Case: Call Shard.use()
        Expected: Get use_shard context manager with the correct shard
        """
        shard = Shard(alias='test_shard', schema_name='test_schema', node_name='default', state=State.ACTIVE)
        use_shard_context_manager = shard.use()
        self.assertIsInstance(use_shard_context_manager, use_shard)
        self.assertEqual(use_shard_context_manager.options.shard_id, shard.id)

    @mock.patch('sharding.models.delete_schema')
    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_delete_from_db(self, mock_delete_schema):
        """
        Case: Delete a Shard with delete_from_db=True
        Expected: delete_schema called
        """
        create_template_schema()

        shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='default')
        shard.delete(delete_from_db=True)

        mock_delete_schema.assert_called_with(schema_name='test_schema', node_name='default')

    @mock.patch('sharding.models.delete_schema')
    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_delete(self, mock_delete_schema):
        """
        Case: Delete a Shard with delete_from_db=False
        Expected: delete_schema not called
        """
        create_template_schema()

        shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='default')
        shard.delete()

        self.assertFalse(mock_delete_schema.called)


class MirroredModelTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()

    def post_init(self):
        """
        Case: Create a mirrored model
        Expected: _shard attribute is not set on the mirrored model
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)

        with use_shard(shard):
            type_ = Type.objects.create(name='test')

        self.assertFalse(hasattr(type_, '_shard'))


class ShardedModelMethodUseShardTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()

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

    def test_override_class_method_use_shard(self):
        """
        Case: Use a model method while being a shard with override_class_method_use_shard=True
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
        with use_shard(other_shard, override_class_method_use_shard=True):
            # Since we provided override_class_method_use_shard=True, this one now queries the organization that is
            # living on other_shard with the same ID as the organization that is living on shard.
            self.assertEqual(user.get_organization_name(), 'The Rebel Alliance')

    def test_already_in_shard(self):
        """
        Case: Call a sharded model instance method in the same use_shard context as the user was retrieved with
        Expected: use_shard for the model method not called, since we are already in that shard
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)

        with use_shard(shard):
            org = Organization.objects.create(name='The Empire')
            user = User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=org)

            with mock.patch('sharding.options.use_shard') as mock_use_shard:
                user.get_organization_name()
                self.assertFalse(mock_use_shard.called)

    def test_related_model(self):
        """
        Case: Get a related model instance outside a use_shard context
        Expected: Related model retrieved from the same shard the current model is living on
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)

        with shard.use():
            organization = Organization.objects.create(name='The Empire')
            User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=organization)

        # Retrieve the object, so we're sure that `organization` wasn't cached on the model instance already
        user = User.objects.using(shard).get()

        self.assertEqual(user.organization, organization)


class ShardedModelFromDbTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()

    def test(self):
        """
        Case: For a sharded model, get an instance from the database with .using() while having a post_init signal
              coupled.
        Expected: The model is instantiated in a use_shard context due to the sharding.decorators._from_db decorator and
                  thus the active connection inside the post_init signal is the same connection as the object was
                  retrieved with.
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)
        other_shard = Shard.objects.create(alias='dantooine', schema_name='alliance_schema', node_name='default',
                                           state=State.ACTIVE)

        with shard.use():
            organization = Organization.objects.create(name='The Empire')
            user = User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=organization)

        using_connection = shard  # We're going to retrieve the User object with this shard

        def post_init_signal(instance, **kwargs):
            # The current active connection is the one we retrievd the instance with
            self.assertEqual(get_active_connection().node_name, using_connection.node_name)
            self.assertEqual(get_active_connection().schema_name, using_connection.schema_name)
            self.assertEqual(get_active_connection().shard_id, using_connection.id)

            # And if we get a related object, that related object's _state.db is equal to the active connection
            self.assertEqual(instance.organization._state.db, get_active_connection())

        def disconnect():
            # For proper test isolation, we disconnect the signal
            post_init.disconnect(post_init_signal, sender=User)

        post_init.connect(post_init_signal, sender=User)
        self.addCleanup(disconnect)  # Make sure the signal is disconnected, even if the test fails

        with other_shard.use():  # Enter a different shard, to be sure that we aren't in the correct shard already
            User.objects.using(using_connection).get(pk=user.pk)
