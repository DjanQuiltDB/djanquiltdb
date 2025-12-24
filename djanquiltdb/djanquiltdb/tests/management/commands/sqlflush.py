from unittest import mock

from django.core.management import call_command

from example.models import Shard, Type, Organization
from djanquiltdb import State
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.tests import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard


@mock.patch('djanquiltdb.management.commands.sqlflush.sql_flush')
class SQLFlushTestCase(ShardingTransactionTestCase):
    def setUp(self):
        create_template_schema()
        create_template_schema('other')

        self.shard1 = Shard.objects.create(alias='sinaloa', schema_name='el_chapo', node_name='default',
                                           state=State.ACTIVE)

        self.shard2 = Shard.objects.create(alias='medellin', schema_name='pablo_escobar', node_name='other',
                                           state=State.ACTIVE)

        with use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME):
            Type.objects.create(name='narco')

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME):
            Type.objects.create(name='narco')

        with use_shard(self.shard1):
            Organization.objects.create(name='Federation')

        with use_shard(self.shard2):
            Organization.objects.create(name='Isodor')

        self.stdout = mock.Mock()

    def call_command(self, **options):
        call_command('sqlflush', verbosity=0, stdout=self.stdout, **options)

    def test(self, mock_sql_flush):
        """
        Case: Call the sqlflush command without extra options
        Expected: All schemas to be printed
        """
        self.call_command()

        self.assertEqual(mock_sql_flush.call_count, 6)

        self.stdout.write.assert_has_calls([
            mock.call('SQL on other|template\n'),
            mock.call('SQL on other\n'),
            mock.call('SQL on default|template\n'),
            mock.call('SQL on default|el_chapo\n'),
            mock.call('SQL on other|pablo_escobar\n'),
            mock.call('SQL on default\n'),
        ], any_order=True)

    def test_single_database(self, mock_sql_flush):
        """
        Case: Call the sqlflush command with a single database specified
        Expected: SQL printed for all schemas on that database, other schemas not printed
        """
        self.call_command(database='default')

        self.assertEqual(mock_sql_flush.call_count, 3)

        self.stdout.write.assert_has_calls([
            mock.call('SQL on default|template\n'),
            mock.call('SQL on default|el_chapo\n'),
            mock.call('SQL on default\n'),
        ], any_order=True)

    def test_single_schema(self, mock_sql_flush):
        """
        Case: Call the sqlflush command with a single database and single schema specified
        Expected: SQL printed for a single schema only
        """
        self.call_command(database='default', schema_name='el_chapo')

        self.assertEqual(mock_sql_flush.call_count, 1)

        self.stdout.write.assert_has_calls([
            mock.call('SQL on default|el_chapo\n')
        ])
