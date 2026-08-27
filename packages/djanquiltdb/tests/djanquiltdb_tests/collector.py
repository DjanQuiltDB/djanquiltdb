from unittest import mock

from django.test.utils import CaptureQueriesContext

from djanquiltdb import State
from djanquiltdb.collector import SimpleCollector
from djanquiltdb.utils import create_template_schema, use_shard
from djanquiltdb_tests import ShardingTestCase
from example.models import DetailedReport, Organization, Report, Shard, Statement, SuperType, Type, User


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


class SimpleCollectorInheritanceTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema('default')
        self.test_shard = Shard.objects.create(
            alias='other', node_name='default', schema_name='test_other_schema', state=State.ACTIVE
        )

        with use_shard(self.test_shard):
            self.organization = Organization.objects.create(name='Field')
            self.report = Report.objects.create(organization=self.organization, title='Sightings')
            self.detailed_report = DetailedReport.objects.create(
                organization=self.organization, title='Sightings, annotated', detail='Three geese, one koot.'
            )

    def collect(self):
        """
        Collect the organization's tree and return the collector.
        """
        with use_shard(self.test_shard) as env:
            collector = SimpleCollector(connection=env.connection, verbose=False)
            collector.collect([self.organization])

        return collector

    def count_collect_queries(self, detailed_report_count):
        """
        Collect a tree of its own holding the given number of DetailedReports, and return how many queries that took.
        """
        with use_shard(self.test_shard) as env:
            organization = Organization.objects.create(name='Counted {}'.format(detailed_report_count))
            for index in range(detailed_report_count):
                DetailedReport.objects.create(
                    organization=organization, title='Sighting {}'.format(index), detail='Gak.'
                )

            collector = SimpleCollector(connection=env.connection, verbose=False)
            with CaptureQueriesContext(env.connection) as queries:
                collector.collect([organization])

        return len(queries)

    def test(self):
        """
        Case: Collect an organization whose tree holds a multi-table inheritance child.
        Expected: Both the child's own table and the parent table it inherits from are collected.
        """
        collector = self.collect()

        self.assertEqual(collector.data[DetailedReport], {self.detailed_report})
        self.assertEqual({report.pk for report in collector.data[Report]}, {self.report.pk, self.detailed_report.pk})

    def test_the_parent_table_depends_on_the_child_table(self):
        """
        Case: Collect an organization whose tree holds a multi-table inheritance child.
        Expected: The parent table depends on the child table, not the other way around, so that the rows
                  pointing at the parent row are the first to go.
        """
        collector = self.collect()

        self.assertEqual(collector.dependencies[Report], {DetailedReport})
        self.assertEqual(collector.dependencies[DetailedReport], set())

    def test_the_child_table_sorts_before_the_parent_table(self):
        """
        Case: Sort the collected data of an organization whose tree holds a multi-table inheritance child.
        Expected: The child table comes first, the order Django deletes multi-table inheritance in.
        """
        collector = self.collect()
        collector.sort()

        models = list(collector.data)
        self.assertLess(models.index(DetailedReport), models.index(Report))

    def test_collecting_parents_does_not_query_per_object(self):
        """
        Case: Collect a tree holding one multi-table inheritance child, and one holding five.
        Expected: The same number of queries either way. The parent instances are built from data the child
                  rows already carry, so they cost nothing to reach.
        """
        self.assertEqual(self.count_collect_queries(1), self.count_collect_queries(5))

    def test_delete(self):
        """
        Case: Delete everything collected for an organization whose tree holds a multi-table inheritance child.
        Expected: Both the child table and the parent table are emptied.
        """
        collector = self.collect()

        with use_shard(self.test_shard):
            collector.delete()

            self.assertFalse(DetailedReport.objects.exists())
            self.assertFalse(Report.objects.exists())
            self.assertFalse(Organization.objects.exists())
