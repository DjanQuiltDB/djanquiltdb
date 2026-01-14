from unittest import mock

from example.models import Organization, Shard, Statement, SuperType, Type, User

from djanquiltdb import State
from djanquiltdb.collector import SimpleCollector
from djanquiltdb.tests import ShardingTestCase
from djanquiltdb.utils import create_template_schema, use_shard


class SimpleCollectorTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema('default')
        self.test_shard = Shard.objects.create(
            alias='other', node_name='default', schema_name='test_other_schema', state=State.ACTIVE
        )
        super_type = SuperType.objects.create(name='Animals')
        type = Type.objects.create(name='Birds', super=super_type)

        with use_shard(self.test_shard):
            self.organization = Organization.objects.create(name='Field')
            self.user_1 = User.objects.create(organization=self.organization, name='Geese', email='g@b.gak', type=type)
            self.user_2 = User.objects.create(organization=self.organization, name='Koot', email='k@b.koo', type=type)
            self.statement_1 = Statement.objects.create(content='Waaargh', user=self.user_1)
            self.statement_2 = Statement.objects.create(content='Gak gak gak', user=self.user_1)
            self.statement_3 = Statement.objects.create(content='Koo', user=self.user_2)

            # Other organization, not to be collected
            other_organization = Organization.objects.create(name='Beach')
            other_user = User.objects.create(
                organization=other_organization, name='Seagull', email='s@b.mine', type=type
            )
            Statement.objects.create(content='Mine', user=other_user)

    def test(self):
        """
        Case: Call the collector on some test data
        Expected: The correct objects to be returned.
        """
        with use_shard(self.test_shard) as env:
            collector = SimpleCollector(connection=env.connection, verbose=False)

            collector.collect([self.organization])

            self.assertEqual(
                collector.data,
                {
                    Organization: {self.organization},
                    User: {self.user_1, self.user_2},
                    Statement: {self.statement_1, self.statement_2, self.statement_3},
                },
            )

    def test_collect(self):
        """
        Case: Call collect on with an object
        Expected: Collect to be called on the related objects.
        """
        with use_shard(self.test_shard) as env:
            collector = SimpleCollector(connection=env.connection)
            with mock.patch('djanquiltdb.collector.SimpleCollector.collect', wraps=collector.collect) as mock_collect:
                collector.collect([self.organization])
                self.assertEqual(mock_collect.call_count, 3)
