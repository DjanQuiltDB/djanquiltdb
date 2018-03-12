from unittest import mock

from example.models import Type, Organization, User, Shard, SuperType, Statement
from sharding import State
from sharding.collector import SimpleCollector
from sharding.tests.utils import ShardingTestCase
from sharding.utils import use_shard, create_template_schema


class GetShardTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema('default')
        self.test_shard = Shard.objects.create(alias='other', node_name='default', schema_name='test_other_schema',
                                               state=State.ACTIVE)
        super_type = SuperType.objects.create(name='Animals')
        type = Type.objects.create(name='Birds', super=super_type)

        with use_shard(self.test_shard):
            self.organization = Organization.objects.create(name='Field')
            self.user_1 = User.objects.create(organization=self.organization, name='Geese', email='g@b.gak', type=type)
            self.user_2 = User.objects.create(organization=self.organization, name='Koot', email='k@b.koo', type=type)
            self.statement_1 = Statement.objects.create(content='Waaargh', user=self.user_1)
            self.statement_2 = Statement.objects.create(content='Gak gak gak', user=self.user_1)
            self.statement_3 = Statement.objects.create(content='Koo', user=self.user_2)

            # other organization, not to be collected
            o = Organization.objects.create(name='Beach')
            u = User.objects.create(organization=o, name='Seagull', email='s@b.mine', type=type)
            Statement.objects.create(content='Mine', user=u)

    def test(self):
        """
        Case: call the collector on some test data
        Expected: The correct objects to be returned.
        """
        with use_shard(self.test_shard) as env:
            collector = SimpleCollector(connection=env.connection, verbose=False)
            collector.collect([self.organization])
            self.assertEqual(collector.data,
                             {Organization: {self.organization.id},
                              User: {self.user_1.id, self.user_2.id},
                              Statement: {self.statement_1.id, self.statement_2.id, self.statement_3.id}})

    def test_collect(self):
        """
        Case: Call collect on with an object
        Expected: collect to be called on the related objects.
        """
        with use_shard(self.test_shard) as env:
            collector = SimpleCollector(connection=env.connection)
            with mock.patch('sharding.collector.SimpleCollector.collect', wraps=collector.collect) as mock_collect:
                collector.collect([self.organization])
                self.assertEqual(mock_collect.call_count, 3)
