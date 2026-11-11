from io import StringIO
from unittest import mock

from django.core.management import CommandError, call_command

from djanquiltdb import State
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import create_template_schema, use_shard
from djanquiltdb_tests import ShardingTransactionTestCase
from example.models import Organization, Shard, Type


@mock.patch('djanquiltdb.management.commands.sqlflush.sql_flush')
class SQLFlushTestCase(ShardingTransactionTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        create_template_schema('other')

        self.shard1 = Shard.objects.create(
            alias='sinaloa', schema_name='el_chapo', node_name='default', state=State.ACTIVE
        )

        self.shard2 = Shard.objects.create(
            alias='medellin', schema_name='pablo_escobar', node_name='other', state=State.ACTIVE
        )

        with use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME):
            Type.objects.create(name='narco')

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME):
            Type.objects.create(name='narco')

        with use_shard(self.shard1):
            Organization.objects.create(name='Federation')

        with use_shard(self.shard2):
            Organization.objects.create(name='Isodor')

        self.stdout = mock.Mock()

    def put_sinaloa_in_maintenance(self):
        self.shard1.state = State.MAINTENANCE
        self.shard1.save(update_fields=['state'])

        # The flush after each test does not flush a shard in maintenance.
        self.addCleanup(Shard.objects.filter(pk=self.shard1.pk).update, state=State.ACTIVE)

    def call_command(self, **options):
        call_command('sqlflush', verbosity=0, stdout=self.stdout, **options)

    def test(self, mock_sql_flush):
        """
        Case: Call the sqlflush command without extra options
        Expected: All schemas to be printed
        """
        self.call_command()

        self.assertEqual(mock_sql_flush.call_count, 6)

        self.stdout.write.assert_has_calls(
            [
                mock.call('SQL on other|template\n'),
                mock.call('SQL on other\n'),
                mock.call('SQL on default|template\n'),
                mock.call('SQL on default|el_chapo\n'),
                mock.call('SQL on other|pablo_escobar\n'),
                mock.call('SQL on default\n'),
            ],
            any_order=True,
        )

    def test_a_mirrored_shard_table_prints_each_shard_once(self, mock_sql_flush):
        """
        Case: The shard table is mirrored, so both nodes hold both registry rows; call the sqlflush
              command without extra options.
        Expected: Each schema's SQL printed exactly once instead of once per node holding the registry.
        """
        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME) as env:
            cursor = env.connection.cursor()
            for shard in (self.shard1, self.shard2):
                # Mirrored writes route to the primary, so mirror the registry rows with plain SQL.
                cursor.execute(
                    'INSERT INTO "{}" (id, alias, schema_name, node_name, state) VALUES (%s, %s, %s, %s, %s)'.format(
                        Shard._meta.db_table
                    ),
                    [shard.id, shard.alias, shard.schema_name, shard.node_name, shard.state],
                )

        self.call_command()

        self.assertEqual(mock_sql_flush.call_count, 6)

    def test_a_shard_in_maintenance_is_named_in_the_error_and_the_other_schemas_are_printed(self, mock_sql_flush):
        """
        Case: Call the sqlflush command without extra options while shard sinaloa is in maintenance.
        Expected: CommandError that names sinaloa and says to pass --ignore-maintenance, after the SQL of every other
                  schema is printed. The SQL of sinaloa is not printed.
        """
        self.put_sinaloa_in_maintenance()

        with self.assertRaisesMessage(CommandError, 'Shard sinaloa: it is in maintenance.') as raised:
            self.call_command()

        self.assertIn('--ignore-maintenance', str(raised.exception))
        self.assertEqual(mock_sql_flush.call_count, 5)
        self.stdout.write.assert_has_calls(
            [
                mock.call('SQL on other|template\n'),
                mock.call('SQL on other\n'),
                mock.call('SQL on default|template\n'),
                mock.call('SQL on other|pablo_escobar\n'),
                mock.call('SQL on default\n'),
            ],
            any_order=True,
        )
        self.assertNotIn(mock.call('SQL on default|el_chapo\n'), self.stdout.write.call_args_list)

    def test_ignore_maintenance_prints_a_shard_in_maintenance(self, mock_sql_flush):
        """
        Case: Call the sqlflush command with --ignore-maintenance while shard sinaloa is in maintenance.
        Expected: The SQL of every schema is printed, sinaloa included.
        """
        self.put_sinaloa_in_maintenance()

        self.call_command(ignore_maintenance=True)

        self.assertEqual(mock_sql_flush.call_count, 6)
        self.stdout.write.assert_has_calls([mock.call('SQL on default|el_chapo\n')])

    def test_single_database(self, mock_sql_flush):
        """
        Case: Call the sqlflush command with a single database specified
        Expected: SQL printed for all schemas on that database, other schemas not printed
        """
        self.call_command(database='default')

        self.assertEqual(mock_sql_flush.call_count, 3)

        self.stdout.write.assert_has_calls(
            [
                mock.call('SQL on default|template\n'),
                mock.call('SQL on default|el_chapo\n'),
                mock.call('SQL on default\n'),
            ],
            any_order=True,
        )

    def test_single_schema(self, mock_sql_flush):
        """
        Case: Call the sqlflush command with a single database and single schema specified
        Expected: SQL printed for a single schema only
        """
        self.call_command(database='default', schema_name='el_chapo')

        self.assertEqual(mock_sql_flush.call_count, 1)

        self.stdout.write.assert_has_calls([mock.call('SQL on default|el_chapo\n')])


class SQLFlushOutputTestCase(ShardingTransactionTestCase):
    def test_prints_the_flush_sql_of_a_schema(self):
        """
        Case: Call the sqlflush command for a single schema, with Django's own sql_flush producing the SQL.
        Expected: The output has the TRUNCATE statement for the sharded tables of that schema.
        """
        create_template_schema()
        shard = Shard.objects.create(alias='sinaloa', schema_name='el_chapo', node_name='default', state=State.ACTIVE)
        stdout = StringIO()

        call_command('sqlflush', database='default', schema_name=shard.schema_name, verbosity=0, stdout=stdout)

        self.assertIn('SQL on default|el_chapo', stdout.getvalue())
        self.assertIn('TRUNCATE', stdout.getvalue())
        self.assertIn(Organization._meta.db_table, stdout.getvalue())

    def test_the_statement_for_the_public_schema_cascades_as_in_flush(self):
        """
        Case: Call the sqlflush command for every schema of a node, with Django's own sql_flush producing the SQL.
        Expected: The statement for the public schema has CASCADE, as the one that flush runs. The statements for the
                  template schema and the shard do not.
        """
        create_template_schema()
        Shard.objects.create(alias='sinaloa', schema_name='el_chapo', node_name='default', state=State.ACTIVE)
        stdout = StringIO()

        call_command('sqlflush', database='default', verbosity=0, stdout=stdout)

        sql_by_schema = {
            section.split('\n', 1)[0]: section.split('\n', 1)[1]
            for section in stdout.getvalue().split('SQL on ')
            if section
        }
        self.assertEqual(set(sql_by_schema), {'default|template', 'default|el_chapo', 'default'})
        self.assertIn('CASCADE', sql_by_schema['default'])
        self.assertNotIn('CASCADE', sql_by_schema['default|template'])
        self.assertNotIn('CASCADE', sql_by_schema['default|el_chapo'])
