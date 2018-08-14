import pickle
from unittest import mock

from django.db import IntegrityError
from django.test import TestCase, SimpleTestCase, override_settings

from example.models import Shard, Organization, User, OrganizationShards, Type, Cake, SuperType
from sharding import ShardingMode, State
from sharding.exceptions import ShardingError
from sharding.models import BaseShard
from sharding.options import get_shard_from_instance_options
from sharding.tests.utils import ShardingTestCase
from sharding.utils import get_shard_class, use_shard, create_template_schema, use_shard_for, StateException, \
    create_schema_on_node
from sharding.tests.app_config import DummyShard


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

    def test_use(self):
        """
        Case: Call Shard.use()
        Expected: Get use_shard context manager with the correct shard
        """
        shard = Shard(alias='test_shard', schema_name='test_schema', node_name='default', state=State.ACTIVE)
        use_shard_context_manager = shard.use()
        self.assertIsInstance(use_shard_context_manager, use_shard)
        self.assertEqual(use_shard_context_manager.shard, shard)

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

    def test_post_init(self):
        """
        Case: Initialize model instance in a use shard context
        Expected: _shard to be set on the instance
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)

        with use_shard(shard):
            org = Organization.objects.create(name='The Empire')
            user = User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=org)

        self.assertEqual(org._shard.schema_name, 'empire_schema')
        self.assertEqual(org._shard.node_name, 'default')
        self.assertEqual(org._shard.id, shard.id)
        self.assertEqual(org._shard.mapping_value, None)
        self.assertEqual(org._shard.active_only_schemas, True)
        self.assertEqual(org._shard.lock, True)

        self.assertEqual(user._shard.schema_name, 'empire_schema')
        self.assertEqual(user._shard.node_name, 'default')
        self.assertEqual(user._shard.id, shard.id)
        self.assertEqual(user._shard.mapping_value, None)
        self.assertEqual(user._shard.active_only_schemas, True)
        self.assertEqual(user._shard.lock, True)

    def test_post_init_use_shard_for(self):
        """
        Case: Initialize model instance in a use shard_for context
        Expected: _shard to be set on the instance, with mapping value equal to the organization ID
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)

        with use_shard(shard):
            org = Organization.objects.create(name='The Empire')
            User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=org)

        OrganizationShards.objects.create(organization_id=org.id, shard_id=shard.id)

        with use_shard_for(org.id):
            org = Organization.objects.get()  # Only one entry, so we can do this safely
            user = User.objects.get()  # Only one entry, so we can do this safely

        self.assertEqual(org._shard.schema_name, 'empire_schema')
        self.assertEqual(org._shard.node_name, 'default')
        self.assertEqual(org._shard.id, shard.id)
        self.assertEqual(org._shard.mapping_value, org.id)
        self.assertEqual(org._shard.active_only_schemas, True)
        self.assertEqual(org._shard.lock, True)

        self.assertEqual(user._shard.schema_name, 'empire_schema')
        self.assertEqual(user._shard.node_name, 'default')
        self.assertEqual(user._shard.id, shard.id)
        self.assertEqual(user._shard.mapping_value, org.id)
        self.assertEqual(user._shard.active_only_schemas, True)
        self.assertEqual(user._shard.lock, True)

    def test_post_init_schema_name(self):
        """
        Case: Initialize model instance in a use shard context with providing the node name and schema name
        Expected: _shard to be set on the instance, but shard id is not known
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)

        with use_shard(node_name=shard.node_name, schema_name=shard.schema_name):
            org = Organization.objects.create(name='The Empire')
            user = User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=org)

        self.assertEqual(org._shard.schema_name, 'empire_schema')
        self.assertEqual(org._shard.node_name, 'default')
        self.assertEqual(org._shard.id, None)
        self.assertEqual(org._shard.mapping_value, None)
        self.assertEqual(org._shard.active_only_schemas, True)
        self.assertEqual(org._shard.lock, True)

        self.assertEqual(user._shard.schema_name, 'empire_schema')
        self.assertEqual(user._shard.node_name, 'default')
        self.assertEqual(user._shard.id, None)
        self.assertEqual(user._shard.mapping_value, None)
        self.assertEqual(user._shard.active_only_schemas, True)
        self.assertEqual(org._shard.lock, True)

        with self.assertRaisesMessage(ShardingError, 'Shard ID is not known for this instance'):
            get_shard_from_instance_options(org._shard)

        with self.assertRaisesMessage(ShardingError, 'Shard ID is not known for this instance'):
            get_shard_from_instance_options(user._shard)

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

    def test_no_shard_set(self):
        """
        Case: Get a sharded model instance and try to call a method while having schema name or node name not set
        Expected: ShardingError raises
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)

        with use_shard(shard):
            org = Organization.objects.create(name='The Empire')

        org._shard.schema_name = None

        with self.assertRaises(ShardingError):
            org.save()

        org._shard.node_name = None

        with self.assertRaises(ShardingError):
            org.save()

        org._shard.schema_name = 'empire_schema'

        with self.assertRaises(ShardingError):
            org.save()

    def test_different_shard_set(self):
        """
        Case: Get a sharded model instance and have a node name that is different than the instance._state.db
        Expected: ShardingError raises
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)

        with use_shard(shard):
            org = Organization.objects.create(name='The Empire')

        org._shard.node_name = 'not_default'

        with self.assertRaises(ShardingError):
            org.save()

    def test_use_shard_for(self):
        """
        Case: Get a sharded model instance with use_shard_for and while having that instance, set the organization shard
              to in maintenance
        Expected: The model method on the sharded model instance will raise a StateException
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)
        other_shard = Shard.objects.create(alias='dantooine', schema_name='alliance_schema', node_name='default',
                                           state=State.ACTIVE)

        with use_shard(shard):
            org = Organization.objects.create(name='The Empire')
            user = User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=org)

        org_shard = OrganizationShards.objects.create(organization_id=org.id, shard_id=shard.id,
                                                      state=State.MAINTENANCE)

        # We can now still do this because we entered the shard through use_shard instead of use_shard_for
        user.get_organization_name()

        # Now set the organization shard to active, so we can retrieve the user
        org_shard.state = State.ACTIVE
        org_shard.save(update_fields=['state'])

        # Now fetch the user with use_shard_for
        with use_shard_for(org.id):
            user = User.objects.get()  # Only one entry, so we can do this safely

        # Now set the organization shard in maintenance
        org_shard.state = State.MAINTENANCE
        org_shard.save(update_fields=['state'])

        with use_shard(other_shard):
            with self.assertRaisesMessage(StateException, 'Mapping object OrganizationShards object state is '
                                                          'Maintenance'):
                user.get_organization_name()

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

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.acquire_advisory_lock')
    def test_no_lock(self, mock_acquire_lock):
        """
        Case: Use a model method while being a shard with lock=False
        Expected: No lock is set when calling the model function
        """
        shard = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                     state=State.ACTIVE)
        other_shard = Shard.objects.create(alias='dantooine', schema_name='alliance_schema', node_name='default',
                                           state=State.ACTIVE)

        with use_shard(shard, lock=False):  # No lock
            org = Organization.objects.create(name='The Empire')
            user = User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=org)

        # Now switch to another shard where the user and organization are not living
        with use_shard(other_shard):
            user.get_organization_name()  # Should not set a lock

        # Only the use_shard(other_shard) should have set a lock.
        mock_acquire_lock.assert_called_once_with(key='shard_{}'.format(other_shard.id), shared=True)


class QuerySetTestCase(TestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard = Shard.objects.create(alias='death_star', schema_name='test_empire_schema', node_name='default',
                                          state=State.ACTIVE)

    def test(self):
        """
        Case: Initialize a queryset in a use_shard context and evaluate it outside a use shard context
        Expected: Queryset saves the shard it is initialized in and can be evaluated outside a use_shard context
        """
        with use_shard(self.shard):
            organization = Organization.objects.create(name='The Empire')
            all_organizations = Organization.objects.all()  # Lazy here

        self.assertEqual(all_organizations._shard.id, self.shard.id)
        self.assertEqual(list(all_organizations), [organization])  # Evaluated here, outside the use_shard context

    def test_queryset(self):
        """
        Case: Initialize a queryset that is explicitly defined on a model in a use_shard context and evaluate it
              outside a use shard context
        Expected: Queryset saves the shard it is initialized in and can be evaluated outside a use_shard context
        """
        with use_shard(self.shard):
            cake = Cake.objects.create(name='Chocolate cake')
            Cake.objects.create(name='Apple pie')
            all_chocolate_cakes = Cake.objects.chocolate()  # Lazy here

        self.assertEqual(all_chocolate_cakes._shard.id, self.shard.id)
        self.assertEqual(list(all_chocolate_cakes), [cake])  # Evaluated here, outside the use_shard context

    def test_override_queryset_method(self):
        """
        Case: Have a custom QuerySet with a method that overrides a method that also exist on normal QuerySets
        Expected: Method correctly copied to the dynamically created QuerySet
        """
        with use_shard(self.shard):
            Cake.objects.create(name='Chocolate cake')

            with self.assertRaises(IntegrityError):
                Cake.objects.delete()

            self.assertTrue(Cake.objects.exists())

            Cake.objects.delete(force=True)

            self.assertFalse(Cake.objects.exists())

    def test_mirrored_model(self):
        """
        Case: Initialize a queryset for a mirrored model in a use_shard context
        Expected: Shard is not saved, since the queryset is associated with a mirrored model
        """
        SuperType.objects.create(name='Character')

        with use_shard(self.shard):
            all_super_types = SuperType.objects.all()

        self.assertFalse(hasattr(all_super_types, '_shard'))

        pickle.dumps(all_super_types)

    def test_pickle(self):
        """
        Case: Pickle a QuerySet that is sharding aware and after unpickle it
        Expected: After unpickling the QuerySet, it should give the same results as before pickling
        """
        with use_shard(self.shard):
            Organization.objects.create(name='The Empire')
            all_organizations = Organization.objects.all()  # Lazy here

        dump = pickle.dumps(all_organizations)  # Should not trigger an exception

        self.assertEqual(list(pickle.loads(dump)), list(all_organizations))

    def test_unset_shard_options(self):
        """
        Case: Being able to unset the shard options, so we can run the QuerySet in a different shard
        Expected: _shard attribute is deleted on QuerySet and we are able to run the queryset in a different shard
        """
        other_shard = Shard.objects.create(alias='other_shard', schema_name='test_other_schema', node_name='default',
                                           state=State.ACTIVE)

        with use_shard(self.shard):
            Organization.objects.create(name='The Empire')
            all_organizations = Organization.objects.all()  # Lazy here

        self.assertEqual(all_organizations._shard.id, self.shard.id)
        all_organizations.unset_shard_options()
        self.assertFalse(hasattr(all_organizations, '_shard'))

        with use_shard(other_shard):
            organization = Organization.objects.create(name='Zizou')
            self.assertEqual(list(all_organizations), [organization])  # We need to do this in a shard now
