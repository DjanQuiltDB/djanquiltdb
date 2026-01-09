import pickle  # nosec

from django.db import IntegrityError
from example.models import Cake, Organization, Shard, Suborganization, SuperType, User

from djanquiltdb import State
from djanquiltdb.options import ShardOptions
from djanquiltdb.tests import ShardingTestCase
from djanquiltdb.utils import create_template_schema, use_shard


class QuerySetTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard = Shard.objects.create(
            alias='death_star', schema_name='test_empire_schema', node_name='default', state=State.ACTIVE
        )

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

        self.assertEqual(list(pickle.loads(dump)), list(all_organizations))  # nosec

    def test_prefetch_related(self):
        """
        Case: In shard B, initialize a new queryset of a related object that's retrieved in shard A, and call a
              prefetch_related of related objects
        Expected: Despite the fact that the queryset is initialized in a different shard context, the results are
                  retrieved from the shard the related object was living on, as expected.
        """
        other_shard = Shard.objects.create(
            alias='other', schema_name='test_other', node_name='default', state=State.ACTIVE
        )

        with use_shard(self.shard):
            organization1 = Organization.objects.create(name='Foo')
            organization2 = Organization.objects.create(name='Bar')
            organization3 = Organization.objects.create(name='Baz')

            Suborganization.objects.create(parent=organization1, child=organization2)
            Suborganization.objects.create(parent=organization1, child=organization3)

        with use_shard(other_shard):
            suborganizations = organization1.parent.prefetch_related('child').all()

        self.assertCountEqual(list(map(lambda x: x.child, suborganizations)), [organization2, organization3])

    def test_m2m_with_manager_filter(self):
        """
        Case: Add and remove Cakes to a User's cake property, it being M2M with a filter pre-applied by the Manager
        Expected: Cakes are successfully added to the user's cake relation, and then successfully removed
        """
        with use_shard(self.shard):
            # Create a user and a couple of cakes
            user = User.objects.create(name='Test User', email='test@example.com')
            cake1 = Cake.objects.create(name='Chocolate cake')
            cake2 = Cake.objects.create(name='Vanilla cake')

            # Initially, user should have no cakes
            self.assertEqual(user.cake.count(), 0)
            self.assertCountEqual(list(user.cake.all()), [])

        # Add both cakes to the user's M2M relation
        user.cake.add(cake1, cake2)

        # Verify both cakes were added
        self.assertEqual(user.cake.count(), 2)
        self.assertCountEqual(list(user.cake.all()), [cake1, cake2])
        self.assertCountEqual(list(user.cake.values_list('id', flat=True)), [cake1.id, cake2.id])

        # Remove one cake from the relation
        user.cake.remove(cake1)

        # Verify only cake2 remains
        self.assertEqual(user.cake.count(), 1)
        self.assertCountEqual(list(user.cake.all()), [cake2])
        self.assertEqual(list(user.cake.values_list('id', flat=True)), [cake2.id])

        # Remove the remaining cake
        user.cake.remove(cake2)

        # Verify no cakes remain
        self.assertEqual(user.cake.count(), 0)
        self.assertCountEqual(list(user.cake.all()), [])

        # Add both cakes to the user's M2M relation
        user.cake.add(cake1, cake2)

        # Verify both cakes were added
        self.assertEqual(user.cake.count(), 2)
        self.assertCountEqual(list(user.cake.all()), [cake1, cake2])
        self.assertCountEqual(list(user.cake.values_list('id', flat=True)), [cake1.id, cake2.id])

        user.cake.remove(cake1.id, cake2.id)

        # Verify no cakes remain
        self.assertEqual(user.cake.count(), 0)
        self.assertCountEqual(list(user.cake.all()), [])

        # Add both cakes to the user's M2M relation
        user.cake.add(cake1, cake2)

        # Verify both cakes were added
        self.assertEqual(user.cake.count(), 2)
        self.assertCountEqual(list(user.cake.all()), [cake1, cake2])
        self.assertCountEqual(list(user.cake.values_list('id', flat=True)), [cake1.id, cake2.id])

        user.cake.clear()

        # Verify no cakes remain
        self.assertEqual(user.cake.count(), 0)
        self.assertCountEqual(list(user.cake.all()), [])


class UsingTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard = Shard.objects.create(
            alias='test', schema_name='test_schema', node_name='default', state=State.ACTIVE
        )

    def test_pipe_node_name_schema_name(self):
        """
        Case: Create a sharded model instance with using('<node_name>|<schema_name>') and after retrieve all the entries
        Expected: Model instance created on the specific shard and when retrieving all entries, it's returned as well
        """
        organization = Organization.objects.using('default|test_schema').create(name='Foo')
        self.assertEqual(organization._state.db, 'default|test_schema')

        self.assertEqual(list(Organization.objects.using('default|test_schema').all()), [organization])

        # Double-check, to make sure it ended up in the correct shard
        with use_shard(self.shard):
            self.assertEqual(list(Organization.objects.all()), [organization])

    def test_tuple_node_name_schema_name(self):
        """
        Case: Create a sharded model instance with using(('<node_name>, '<schema_name>')) and after retrieve all the
              entries
        Expected: Model instance created on the specific shard and when retrieving all entries, it's returned as well
        """
        organization = Organization.objects.using(('default', 'test_schema')).create(name='Foo')
        self.assertEqual(organization._state.db, ('default', 'test_schema'))

        self.assertEqual(list(Organization.objects.using(('default', 'test_schema')).all()), [organization])

        # Double-check, to make sure it ended up in the correct shard
        with use_shard(self.shard):
            self.assertEqual(list(Organization.objects.all()), [organization])

    def test_shard_instance(self):
        """
        Case: Create a sharded model instance with using(<shard_instance>) and after retrieve all the entries
        Expected: Model instance created on the specific shard and when retrieving all entries, it's returned as well
        """
        organization = Organization.objects.using(self.shard).create(name='Foo')
        self.assertEqual(organization._state.db, self.shard)

        self.assertEqual(list(Organization.objects.using(self.shard).all()), [organization])

        # Double-check, to make sure it ended up in the correct shard
        with use_shard(self.shard):
            self.assertEqual(list(Organization.objects.all()), [organization])

    def test_shard_options_instance(self):
        """
        Case: Create a sharded model instance with using(<shard_options>) and after retrieve all the entries
        Expected: Model instance created on the specific shard and when retrieving all entries, it's returned as well
        """
        shard_options = ShardOptions.from_shard(self.shard)
        organization = Organization.objects.using(shard_options).create(name='Foo')
        self.assertEqual(organization._state.db, shard_options)

        self.assertEqual(list(Organization.objects.using(shard_options).all()), [organization])

        # Double-check, to make sure it ended up in the correct shard
        with use_shard(self.shard):
            self.assertEqual(list(Organization.objects.all()), [organization])
