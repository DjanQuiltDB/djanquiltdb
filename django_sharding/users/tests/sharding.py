from django.db import models, connection
from django.test import TestCase
from django.conf import settings

from users.models import Organization, Type, User
from shardingtest.models import Shard, Node


class ShardingTestCase(TestCase):
    def setUp(self):
        super().setUp()

        print(settings.DATABASES)

        self.node = Node.objects.create(uri=settings.DATABASES['default']['HOST'])
        self.shard1 = Shard.objects.create(alias='death_star', schema_name='empire_schema', node=self.node)
        self.shard2 = Shard.objects.create(alias='dantooine', schema_name='alliance_schema', node=self.node)

        connection.set_schema_to_public()
        self.type1 = Type.objects.create(name="Leader")
        self.type2 = Type.objects.create(name="Admiral")

        connection.set_schema(self.shard1.schema_name)
        self.org1 = Organization.objects.create(name="The Empire")
        self.user1 = User.objects.create(name="Sheev Palpatine", email="s.palpatine@sith.sw", organization=self.org1,
                                         type=self.type1)

        connection.set_schema(self.shard2.schema_name)
        self.org2 = Organization.objects.create(name="The Rebel Alliance")
        self.user2 = User.objects.create(name="Mon Mothma", email="m.mothma@alliance.sw", organization=self.org2,
                                         type=self.type1)
        self.user3 = User.objects.create(name="Ackbar", email="itsatrap@alliance.sw", organization=self.org2,
                                         type=self.type2)

    def test_le(self):
        print(self.user1.created_at)
        print(self.user2.created_at)
        print(self.user3.created_at)
        print("--")
        connection.set_schema(self.shard1.schema_name)
        print(Organization.objects.values_list('name'))
        self.assertCountEqual(Organization.objects.values_list('name', flat=True), ['The Empire'])
        print(User.objects.values_list('name'))
        self.assertCountEqual(User.objects.values_list('name', flat=True), ['Sheev Palpatine'])
        print(Type.objects.values_list('name'))
        self.assertCountEqual(Type.objects.values_list('name', flat=True), [])

        connection.set_schema(self.shard2.schema_name)
        print(Organization.objects.values_list('name'))
        self.assertCountEqual(Organization.objects.values_list('name', flat=True), ['The Rebel Alliance'])
        print(User.objects.values_list('name'))
        self.assertCountEqual(User.objects.values_list('name', flat=True), ['Mon Mothma', 'Ackbar'])
        print(Type.objects.values_list('name'))
        self.assertCountEqual(Type.objects.values_list('name', flat=True), [])
        print('--')
        print(self.user3.type.name, self.user3.type_id)
        print("test done")