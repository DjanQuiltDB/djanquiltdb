import inspect
from unittest import mock

from django.db.models import Model
from django.db.models.signals import post_init
from django.test import SimpleTestCase, override_settings
from django.test.utils import isolate_apps
from django.utils import timezone

from djanquiltdb import State
from djanquiltdb.apps import make_shard_aware_from_db
from djanquiltdb.decorators import shard_aware_from_db
from djanquiltdb.options import ShardOptions
from djanquiltdb.utils import create_schema_on_node, create_template_schema, get_shard_class, use_shard
from djanquiltdb_tests import ShardingTestCase
from djanquiltdb_tests.app_config import DummyShard
from example.models import Organization, ProxyCake, Shard, Type, User


class MakeShardAwareFromDbTestCase(SimpleTestCase):
    def test_from_db_without_a_function_behind_it(self):
        """
        Case: Make a shard-aware from_db for a model whose from_db takes the db as its first argument. Try a plain
              function (like one from the deprecated class_method_use_shard_from_db_arg) and a staticmethod.
        Expected: A classmethod (Django inspects from_db as one). It calls the function with the db and the row, without
                  the class.
        """
        calls = []

        def from_db(db, field_names, values):
            calls.append((db, field_names, values))
            return 'instance'

        for attribute in (from_db, staticmethod(from_db)):
            with self.subTest(attribute=type(attribute).__name__):
                model = type('Sharded', (), {'from_db': attribute})

                wrapped = make_shard_aware_from_db(model.from_db)
                model.from_db = wrapped

                self.assertIsInstance(wrapped, classmethod)
                self.assertEqual(model.from_db('default', ['id'], [1]), 'instance')

        self.assertEqual(calls, [('default', ['id'], [1])] * 2)

    def test_from_db_classmethod(self):
        """
        Case: Make a shard-aware from_db for a model whose from_db is a classmethod (like Django's own).
        Expected: It wraps the function behind the classmethod.
        """
        wrapped = make_shard_aware_from_db(Model.from_db)

        self.assertIs(wrapped.__func__.__decorator__[1].arguments['func'], Model.from_db.__func__)


class GetShardTestCase(SimpleTestCase):
    @override_settings(QUILT_DB={'SHARD_CLASS': 'djanquiltdb_tests.app_config.DummyShard'})
    def test_get_shard(self):
        """
        Case: Get shard class.
        Expected: Class reference of classname given in the settings.
        """
        self.assertEqual(get_shard_class(), DummyShard)


class BaseShardTestCase(ShardingTestCase):
    @mock.patch('djanquiltdb.utils.create_schema_on_node')
    @mock.patch('djanquiltdb.models.models.Model.save')
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

    @mock.patch('djanquiltdb.utils.create_schema_on_node')
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

    @mock.patch('djanquiltdb.utils.create_schema_on_node')
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

        with mock.patch('djanquiltdb.utils.create_schema_on_node') as mock_create_schema:
            shard = Shard(alias='test_shard', schema_name='test_schema', node_name='default')
            shard.save()
            self.assertFalse(mock_create_schema.called)

    @mock.patch('djanquiltdb.utils.create_schema_on_node')
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

    @mock.patch('djanquiltdb.utils.create_schema_on_node')
    @mock.patch('djanquiltdb.models.models.Model.save')
    def test_save_on_different_node_for_non_mirrored(self, mock_save, mock_create_schema):
        """
        Case: Call the save method from the BaseShard model on any other node than where the schema belongs to.
        Expected: Create_schema is called, and the object is saved.
        """
        with use_shard(node_name='other', schema_name='public'):  # Shard objects are always on public
            shard = Shard(alias='test_shard', schema_name='test_schema', node_name='default')
            shard.save()
            self.assertTrue(mock_create_schema.called)
            self.assertTrue(mock_save.called)

    def test_clean(self):
        """
        Case: Call the clean method from the Shard model with an existing node_name
        Expected: No problems
        """
        shard = Shard(alias='test_shard', schema_name='test_schema', node_name='default')
        shard.clean()

    def test_clean_failure(self):
        """
        Case: Call the clean method from the Shard model with a non-existing node_name
        Expected: A valueError to be raised
        """
        shard = Shard(alias='test_shard', schema_name='test_schema', node_name='nonexisting')
        with self.assertRaises(ValueError):
            shard.clean()

    @mock.patch('djanquiltdb.utils.create_schema_on_node')
    @mock.patch('djanquiltdb.models.models.Model.save')
    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard', 'NEW_SHARD_NODE': 'other'})
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

    @mock.patch('djanquiltdb.utils.create_schema_on_node')
    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
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

    @mock.patch('djanquiltdb.models.delete_schema')
    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_delete_from_db(self, mock_delete_schema):
        """
        Case: Delete a Shard with delete_from_db=True
        Expected: delete_schema called
        """
        create_template_schema()

        shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='default')
        shard.delete(delete_from_db=True)

        mock_delete_schema.assert_called_with(schema_name='test_schema', node_name='default')

    @mock.patch('djanquiltdb.models.delete_schema')
    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
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
        shard = Shard.objects.create(
            alias='death_star', schema_name='empire_schema', node_name='default', state=State.ACTIVE
        )

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
        shard = Shard.objects.create(
            alias='death_star', schema_name='empire_schema', node_name='default', state=State.ACTIVE
        )
        other_shard = Shard.objects.create(
            alias='dantooine', schema_name='alliance_schema', node_name='default', state=State.ACTIVE
        )

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
        shard = Shard.objects.create(
            alias='death_star', schema_name='empire_schema', node_name='default', state=State.ACTIVE
        )
        other_shard = Shard.objects.create(
            alias='dantooine', schema_name='alliance_schema', node_name='default', state=State.ACTIVE
        )

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
        shard = Shard.objects.create(
            alias='death_star', schema_name='empire_schema', node_name='default', state=State.ACTIVE
        )

        with use_shard(shard):
            org = Organization.objects.create(name='The Empire')
            user = User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=org)

            with mock.patch('djanquiltdb.options.use_shard') as mock_use_shard:
                user.get_organization_name()
                self.assertFalse(mock_use_shard.called)

    def test_related_model(self):
        """
        Case: Get a related model instance outside a use_shard context
        Expected: Related model retrieved from the same shard the current model is living on
        """
        shard = Shard.objects.create(
            alias='death_star', schema_name='empire_schema', node_name='default', state=State.ACTIVE
        )

        with shard.use():
            organization = Organization.objects.create(name='The Empire')
            User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw', organization=organization)

        # Retrieve the object, so we're sure that `organization` wasn't cached on the model instance already
        user = User.objects.using(shard).get()

        self.assertEqual(user.organization, organization)


class ShardedModelFromDbUseShardTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()

        self.addCleanup(self.disconnect_signals)

        self.shard = Shard.objects.create(
            alias='death_star', schema_name='empire_schema', node_name='default', state=State.ACTIVE
        )

        def post_init_signal(instance, *args, **kwargs):
            from django.db import connection

            self.assertIsInstance(connection.db_alias, ShardOptions)
            self.assertEqual(connection.db_alias.node_name, self.shard.node_name)
            self.assertEqual(connection.db_alias.schema_name, self.shard.schema_name)
            self.assertEqual(connection.db_alias.shard_id, self.shard.id)

        self.post_init_signal = post_init_signal

        with self.shard.use():
            Organization.objects.create(name='zero')
            ProxyCake.objects.create(name='zero')

    def disconnect_signals(self):
        post_init.disconnect(self.post_init_signal, sender='example.Organization')

    def test_from_db_is_a_classmethod(self):
        """
        Case: Look up the shard-aware from_db on a concrete and a proxy sharded model.
        Expected: A classmethod bound to the model, like Django's own. (Django 6.1.1 reads __func__ from it on every
                  query, and a plain function has no __func__.)
        """
        for model in (Organization, ProxyCake):
            with self.subTest(model=model.__name__):
                self.assertIsInstance(inspect.getattr_static(model, 'from_db'), classmethod)
                self.assertIs(model.from_db.__self__, model)

    @isolate_apps('example')
    def test_inherited_from_db_creates_the_subclass(self):
        """
        Case: Load a row through a subclass of a sharded model. The subclass is defined after the sharded models are set
              up, so it has no from_db of its own and inherits the one of its parent.
        Expected: An instance of the subclass (Django's own from_db also creates an instance of the class it is called
                  on). The instance is built in the shard the row comes from.
        """

        class LateOrganization(Organization):
            class Meta:
                proxy = True
                app_label = 'example'

        organization = LateOrganization.from_db(self.shard, ['id', 'name', 'created_at'], [1, 'Hope', timezone.now()])

        self.assertIs(type(organization), LateOrganization)
        self.assertEqual(organization.name, 'Hope')

    def test_from_db_keeps_its_decorator_reference(self):
        """
        Case: Look up the function behind the from_db of a sharded model.
        Expected: It names its decorator and the function it wraps, like the other methods added to sharded models. This
                  makes the wrapping recognisable.
        """
        decorator, bound_arguments = inspect.getattr_static(Organization, 'from_db').__func__.__decorator__

        self.assertIs(decorator, shard_aware_from_db)
        self.assertIs(bound_arguments.arguments['func'], Model.from_db.__func__)

    def test_only(self):
        """
        Case: Instantiate a model with .only, with and without a post_init signal
        Expected: Only the requested fields have a value
        """
        now = timezone.now()
        with self.subTest('No signals'):
            with self.shard.use():
                id = Organization.objects.create(name='Hope', created_at=now).id
                organization = Organization.objects.only('id', 'created_at').get(id=id)

            self.assertEqual(organization.__dict__.get('created_at'), now)
            self.assertIsNone(organization.__dict__.get('name'))

        with self.subTest('With post_init signal'):
            post_init.connect(self.post_init_signal, sender='example.Organization', weak=False)

            with self.shard.use():
                id = Organization.objects.create(name='Hope', created_at=now).id
                organization = Organization.objects.only('id', 'created_at').get(id=id)

            self.assertEqual(organization.__dict__.get('created_at'), now)
            self.assertIsNone(organization.__dict__.get('name'))

    def test_with_db(self):
        """
        Case: Instantiate a model in various ways, with a post_init signal present
        Expected: The active connection for the signals should always point to the correct shard,
                  if retrieved from the db
        """
        post_init.connect(self.post_init_signal, sender='example.Organization', weak=False)

        with self.subTest('Create directly within context'):
            with self.shard.use():
                Organization.objects.create(name='one')

        with self.subTest('Create indirectly within context'):
            with self.shard.use():
                o = Organization(name='two')
                o.save()

        with self.subTest('Request within context'):
            with self.shard.use():
                Organization.objects.get(name='zero')

        with self.subTest('Refresh within context'):
            with self.shard.use():
                object = Organization.objects.get(name='zero')
                object.refresh_from_db()

        with self.subTest('Create directly on no shard at all'):
            # Creating an instance of a sharded model without selecting a shard is not a thing.
            with self.assertRaises(AssertionError):
                Organization.objects.create(name='three')

        with self.subTest('Create directly outside context'):
            # In this scenario, the post_init signal is not run in a sharded context, and the router receives no hints,
            # since the object._state is not set on time. No from_db is called either in this flow.
            with self.assertRaises(AssertionError):
                Organization.objects.using(self.shard).create(name='three')

        with self.subTest('Create indirectly outside context'):
            # In this scenario, the post_init signal is not run in a sharded context, and the router receives no hints,
            # since the object._state is not set on time. No from_db is called either in this flow.
            with self.assertRaises(AssertionError):
                o = Organization(name='four')
                o.save(using=self.shard)

        with self.subTest('Request outside context'):
            Organization.objects.using(self.shard).get(name='zero')

        with self.subTest('Refresh outside context'):
            with self.shard.use():
                object = Organization.objects.get(name='zero')

            object.refresh_from_db()

    def test_with_db_proxy_model(self):
        """
        Case: Instantiate a Proxy Model in various ways, with a post_init signal present
        Expected: The active connection for the signals should always point to the correct shard,
                  if retrieved from the db
        """
        post_init.connect(self.post_init_signal, sender='example.Organization', weak=False)

        with self.subTest('Create directly within context'):
            with self.shard.use():
                object = ProxyCake.objects.create(name='one')
                self.assertIsInstance(object, ProxyCake)

        with self.subTest('Create indirectly within context'):
            with self.shard.use():
                object = ProxyCake(name='two')
                object.save()

        with self.subTest('Request within context'):
            with self.shard.use():
                object = ProxyCake.objects.get(name='zero')
                self.assertIsInstance(object, ProxyCake)

        with self.subTest('Refresh within context'):
            with self.shard.use():
                object = ProxyCake.objects.get(name='zero')
                object.refresh_from_db()
                self.assertIsInstance(object, ProxyCake)

        with self.subTest('Request outside context'):
            object = ProxyCake.objects.using(self.shard).get(name='zero')
            self.assertIsInstance(object, ProxyCake)

        with self.subTest('Refresh outside context'):
            with self.shard.use():
                object = ProxyCake.objects.get(name='zero')

            object.refresh_from_db()
            self.assertIsInstance(object, ProxyCake)
