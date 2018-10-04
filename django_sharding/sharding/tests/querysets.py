import pickle

from django.db import IntegrityError

from example.models import Shard, Organization, Cake, SuperType
from sharding import State
from sharding.options import ShardOptions
from sharding.tests import ShardingTestCase
from sharding.utils import create_template_schema, use_shard


class QuerySetTestCase(ShardingTestCase):
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
        with use_shard(self.shard) as env:
            organization = Organization.objects.create(name='The Empire')
            all_organizations = Organization.objects.all()  # Lazy here

        self.assertEqual(all_organizations._hints, {'_shard_options': env.options})
        self.assertEqual(list(all_organizations), [organization])  # Evaluated here, outside the use_shard context

    def test_queryset(self):
        """
        Case: Initialize a queryset that is explicitly defined on a model in a use_shard context and evaluate it
              outside a use shard context
        Expected: Queryset saves the shard it is initialized in and can be evaluated outside a use_shard context
        """
        with use_shard(self.shard) as env:
            cake = Cake.objects.create(name='Chocolate cake')
            Cake.objects.create(name='Apple pie')
            all_chocolate_cakes = Cake.objects.chocolate()  # Lazy here

        self.assertEqual(all_chocolate_cakes._hints, {'_shard_options': env.options})
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


class UsingTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard = Shard.objects.create(alias='test', schema_name='test_schema', node_name='default',
                                          state=State.ACTIVE)

    def test_pipe_node_name_schema_name(self):
        """
        Case: Create a sharded model instance with using('<node_name>|<schema_name>') and after retrieve all the entries
        Expected: Model instance created on the specific shard and when retrieving all entries, it's returned as well
        """
        organization = Organization.objects.using('default|test_schema').create(name='Foo')
        self.assertEqual(organization._state.db, 'default|test_schema')

        self.assertEqual(list(Organization.objects.using('default|test_schema').all()), [organization])

    def test_tuple_node_name_schema_name(self):
        """
        Case: Create a sharded model instance with using(('<node_name>, '<schema_name>')) and after retrieve all the
              entries
        Expected: Model instance created on the specific shard and when retrieving all entries, it's returned as well
        """
        organization = Organization.objects.using(('default', 'test_schema')).create(name='Foo')
        self.assertEqual(organization._state.db, ('default', 'test_schema'))

        self.assertEqual(list(Organization.objects.using(('default', 'test_schema')).all()), [organization])

    def test_shard_instance(self):
        """
        Case: Create a sharded model instance with using(<shard_instance>) and after retrieve all the entries
        Expected: Model instance created on the specific shard and when retrieving all entries, it's returned as well
        """
        organization = Organization.objects.using(self.shard).create(name='Foo')
        self.assertEqual(organization._state.db, self.shard)

        self.assertEqual(list(Organization.objects.using(self.shard).all()), [organization])

    def test_shard_options_instance(self):
        """
        Case: Create a sharded model instance with using(<shard_options>) and after retrieve all the entries
        Expected: Model instance created on the specific shard and when retrieving all entries, it's returned as well
        """
        shard_options = ShardOptions.from_shard(self.shard)
        organization = Organization.objects.using(shard_options).create(name='Foo')
        self.assertEqual(organization._state.db, shard_options)

        self.assertEqual(list(Organization.objects.using(shard_options).all()), [organization])
