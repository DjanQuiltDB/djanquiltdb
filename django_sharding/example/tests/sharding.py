from unittest import mock

from example.models import Organization, Type, User, OrganizationShards, Shard
from sharding.utils import use_shard, create_template_schema
from sharding.tests.utils import ShardingTestCase
from sharding.utils import State


class ShardingExampleTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()

        # tables for the 'public' schema (in this case: Type) are not mirrored yet. So stick to default node for now.
        self.shard1 = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                           state=State.ACTIVE)
        self.shard2 = Shard.objects.create(alias='dantooine', schema_name='alliance_schema', node_name='default',
                                           state=State.ACTIVE)

        # default shard
        self.type1 = Type.objects.create(name='Leader')
        self.type2 = Type.objects.create(name='Admiral')

        with use_shard(self.shard1):
            self.org1 = Organization.objects.create(name='The Empire')
            self.user1 = User.objects.create(name='Sheev Palpatine', email='s.palpatine@sith.sw',
                                             organization=self.org1, type=self.type1)

        with use_shard(self.shard2):
            self.org2 = Organization.objects.create(name='The Rebel Alliance')
            self.user2 = User.objects.create(name='Mon Mothma', email='m.mothma@alliance.sw', organization=self.org2,
                                             type=self.type1)
            self.user3 = User.objects.create(name='Ackbar', email='itsatrap@alliance.sw', organization=self.org2,
                                             type=self.type2)

        OrganizationShards.objects.create(organization_id=self.org1.id, shard=self.shard1)
        OrganizationShards.objects.create(organization_id=self.org2.id, shard=self.shard2)

    def test_schema_separation(self):
        """
        Case: Make organization and example on both shards.
        Expected: Shard 1 not to contain the data of shard 2, and vice versa.
        """
        with use_shard(self.shard1):
            self.assertCountEqual(Organization.objects.values_list('name', flat=True), ['The Empire'])
            self.assertCountEqual(User.objects.values_list('name', flat=True), ['Sheev Palpatine'])
            self.assertCountEqual(Type.objects.values_list('name', flat=True), [])

        with use_shard(self.shard2):
            self.assertCountEqual(Organization.objects.values_list('name', flat=True), ['The Rebel Alliance'])
            self.assertCountEqual(User.objects.values_list('name', flat=True), ['Mon Mothma', 'Ackbar'])
            self.assertCountEqual(Type.objects.values_list('name', flat=True), [])


class MappingQuerySetTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        with mock.patch('sharding.utils.create_schema_on_node'):
            self.shard1 = Shard.objects.create(alias='death_star', schema_name='empire_schema', node_name='default',
                                               state=State.ACTIVE)
            self.shard2 = Shard.objects.create(alias='death_star_MK2', schema_name='empire_schema', node_name='default',
                                               state=State.MAINTENANCE)

        self.org_shard1 = OrganizationShards.objects.create(organization_id=1, shard=self.shard1)
        self.org_shard2 = OrganizationShards.objects.create(organization_id=2, shard=self.shard1)
        self.org_shard3 = OrganizationShards.objects.create(organization_id=3, shard=self.shard2)

    def test_get_shard_for(self):
        """
        Case: Make two entries in the mapping table, and get one using for_target.
        Expected: Only the targeted shard to be returned
        """
        self.assertEqual(OrganizationShards.objects.for_target(1), self.org_shard1)

    def test_active(self):
        self.assertCountEqual(OrganizationShards.objects.active().all(),
                              [self.org_shard1, self.org_shard2])

    def test_in_maintenance(self):
        self.assertCountEqual(OrganizationShards.objects.in_maintenance().all(),
                              [self.org_shard3])
