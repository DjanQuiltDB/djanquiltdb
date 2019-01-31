from unittest import mock

import django
from django.core.management import call_command

from example.models import Shard
from sharding import State
from sharding.options import ShardOptions
from sharding.tests import ShardingTestCase
from sharding.utils import create_template_schema


class LoadDataTestCase(ShardingTestCase):
    def assertDatabaseString(self, database):
        shard_options = ShardOptions.from_alias(database)
        with mock.patch('sharding.management.commands.loaddata.LoadDataCommand.handle') as mock_handle:
            call_command('loaddata', 'foo', database=database)
            call_args = ('foo',)
            call_kwargs = dict(
                database='{}|{}'.format(shard_options.node_name, shard_options.schema_name),
                verbosity=1,
                traceback=False,
                no_color=False,
                app_label=None,
                pythonpath=None,
                skip_checks=True,
                ignore=False,
                settings=None
            )

            if django.VERSION >= (1, 11):
                call_kwargs['exclude'] = []

            mock_handle.assert_called_once_with(*call_args, **call_kwargs)

    def test_database_tuple(self):
        """
        Case: Call loaddata with database being a tuple of node name and schema name
        Expected: Original loaddata command called with database being a string of node name and schema name separated
                  with a pipe
        """
        self.assertDatabaseString(database=('default', 'test_schema'))

    def test_database_shard(self):
        """
        Case: Call loaddata with database being a shard instance
        Expected: Original loaddata command called with database being a string of node name and schema name separated
                  with a pipe
        """
        create_template_schema()
        shard = Shard.objects.create(node_name='default', schema_name='test_schema', alias='schema', state=State.ACTIVE)
        self.assertDatabaseString(database=shard)

    def test_shard_options(self):
        """
        Case: Call loaddata with database being a ShardOptions instance
        Expected: Original loaddata command called with database being a string of node name and schema name separated
                  with a pipe
        """
        shard_options = ShardOptions(node_name='default', schema_name='test_schema')
        self.assertDatabaseString(database=shard_options)
