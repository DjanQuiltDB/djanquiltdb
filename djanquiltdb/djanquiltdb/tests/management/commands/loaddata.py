import json
import os
import tempfile
from unittest import mock

from django.core.management import call_command
from django.db import connection, models
from example.models import Organization, Shard, Statement, SuperType, Type, User

from djanquiltdb import State
from djanquiltdb.options import ShardOptions
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.tests import ShardingTestCase
from djanquiltdb.utils import create_template_schema, use_shard_for


class LoadDataTestCase(ShardingTestCase):
    def assertDatabaseString(self, database):
        shard_options = ShardOptions.from_alias(database)
        with mock.patch('djanquiltdb.management.commands.loaddata.LoadDataCommand.handle') as mock_handle:
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
                settings=None,
                exclude=[],
            )

            # Different versions of Django call loaddata with different arguments.
            try:
                mock_handle.assert_called_once_with(*call_args, **call_kwargs)
            except AssertionError:
                call_kwargs['force_color'] = False
                call_kwargs['format'] = None
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

    def test_reset_sequences(self):
        """
        Case: Load fixtures into public and sharded schema using loaddata
        Expected: Sequences are reset after loading fixtures, so new inserts get correct IDs
        Tests sharded models, mirrored models, and public models
        """
        fixture_data = [
            {
                'model': 'example.Shard',
                'pk': 1,
                'fields': {
                    'id': 1,
                    'node_name': 'default',
                    'schema_name': 'test_shard',
                    'alias': 'test_shard',
                    'state': 'A',
                },
            },
            {
                'model': 'example.OrganizationShard',
                'pk': 1,
                'fields': {
                    'id': 1,
                    'organization_id': 1,
                    'shard_id': 1,
                    'state': 'A',
                },
                '_schema': 'test_shard',
            },
            {
                'model': 'example.Organization',
                'pk': 1,
                'fields': {'name': 'Test Org 1'},
                '_schema': 'test_shard',
            },
            {
                'model': 'example.Organization',
                'pk': 5,
                'fields': {'name': 'Test Org 5'},
                '_schema': 'test_shard',
            },
            {
                'model': 'example.User',
                'pk': 1,
                'fields': {'name': 'Test User', 'email': 'test@example.com', 'password': 'pbkdf2_sha256$20000$test'},
                '_schema': 'test_shard',
            },
            {
                'model': 'example.Statement',
                'pk': 10,
                'fields': {'content': 'Test Statement', 'user': 1, 'offset': 0},
                '_schema': 'test_shard',
            },
            {
                'model': 'example.Type',
                'pk': 1,
                'fields': {'name': 'Mirrored Type 1'},
            },
            {
                'model': 'example.Type',
                'pk': 3,
                'fields': {'name': 'Mirrored Type 3'},
            },
            {
                'model': 'example.SuperType',
                'pk': 1,
                'fields': {'name': 'Public Type 1'},
            },
            {
                'model': 'example.SuperType',
                'pk': 7,
                'fields': {'name': 'Public Type 7'},
            },
        ]

        # Create temporary fixture file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(fixture_data, f)
            fixture_path = f.name

        try:
            # Load the fixture
            call_command('loaddata', fixture_path, verbosity=0)

            # Verify data was loaded in sharded schema
            with use_shard_for(1):
                self.assertEqual(Organization.objects.count(), 2)
                self.assertEqual(Statement.objects.count(), 1)
                max_org_id = Organization.objects.aggregate(max_id=models.Max('id'))['max_id']
                max_stmt_id = Statement.objects.aggregate(max_id=models.Max('id'))['max_id']

            # Verify data was loaded in public schema (mirrored and public models)
            self.assertEqual(Type.objects.count(), 2)
            self.assertEqual(SuperType.objects.count(), 2)
            max_type_id = Type.objects.aggregate(max_id=models.Max('id'))['max_id']
            max_supertype_id = SuperType.objects.aggregate(max_id=models.Max('id'))['max_id']

            # Verify sequences are reset correctly for sharded models
            with use_shard_for(1) as env:
                cursor = env.connection.cursor()

                # Check Organization sequence
                cursor.execute(
                    """
                    SELECT pg_get_serial_sequence(%s, 'id')
                """,
                    ['{}.{}'.format('test_shard', Organization._meta.db_table)],
                )
                org_seq_result = cursor.fetchone()
                if org_seq_result and org_seq_result[0]:
                    seq_name = org_seq_result[0]
                    cursor.execute('SELECT last_value, is_called FROM {}'.format(seq_name))
                    seq_info = cursor.fetchone()
                    if seq_info:
                        last_value, is_called = seq_info
                        self.assertEqual(last_value, max_org_id, 'Organization sequence should be set to max ID')
                        self.assertTrue(is_called, 'Sequence should be marked as called')

                # Check Statement sequence
                cursor.execute(
                    """
                    SELECT pg_get_serial_sequence(%s, 'id')
                """,
                    ['{}.{}'.format('test_shard', Statement._meta.db_table)],
                )
                stmt_seq_result = cursor.fetchone()
                if stmt_seq_result and stmt_seq_result[0]:
                    seq_name = stmt_seq_result[0]
                    cursor.execute('SELECT last_value, is_called FROM {}'.format(seq_name))
                    seq_info = cursor.fetchone()
                    if seq_info:
                        last_value, is_called = seq_info
                        self.assertEqual(last_value, max_stmt_id, 'Statement sequence should be set to max ID')
                        self.assertTrue(is_called, 'Sequence should be marked as called')

            # Verify sequences are reset correctly for mirrored and public models (in public schema)
            cursor = connection.cursor()

            # Check Type (mirrored model) sequence
            cursor.execute(
                """
                SELECT pg_get_serial_sequence(%s, 'id')
            """,
                ['{}.{}'.format(PUBLIC_SCHEMA_NAME, Type._meta.db_table)],
            )
            type_seq_result = cursor.fetchone()
            if type_seq_result and type_seq_result[0]:
                seq_name = type_seq_result[0]
                cursor.execute('SELECT last_value, is_called FROM {}'.format(seq_name))
                seq_info = cursor.fetchone()
                if seq_info:
                    last_value, is_called = seq_info
                    self.assertEqual(last_value, max_type_id, 'Type (mirrored) sequence should be set to max ID')
                    self.assertTrue(is_called, 'Sequence should be marked as called')

            # Check SuperType (public model) sequence
            cursor.execute(
                """
                SELECT pg_get_serial_sequence(%s, 'id')
            """,
                ['{}.{}'.format(PUBLIC_SCHEMA_NAME, SuperType._meta.db_table)],
            )
            supertype_seq_result = cursor.fetchone()
            if supertype_seq_result and supertype_seq_result[0]:
                seq_name = supertype_seq_result[0]
                cursor.execute('SELECT last_value, is_called FROM {}'.format(seq_name))
                seq_info = cursor.fetchone()
                if seq_info:
                    last_value, is_called = seq_info
                    self.assertEqual(
                        last_value, max_supertype_id, 'SuperType (public) sequence should be set to max ID'
                    )
                    self.assertTrue(is_called, 'Sequence should be marked as called')

            # Verify new inserts get correct IDs for sharded models
            with use_shard_for(1):
                new_org = Organization.objects.create(name='New Org')
                self.assertEqual(new_org.id, max_org_id + 1, 'New organization should get next ID after max')

                # Get the user that was created in the fixture
                user = User.objects.get(pk=1)
                new_stmt = Statement.objects.create(content='New Statement', user=user, offset=0)
                self.assertEqual(new_stmt.id, max_stmt_id + 1, 'New statement should get next ID after max')

            # Verify new inserts get correct IDs for mirrored and public models (in public schema)
            new_type = Type.objects.create(name='New Mirrored Type')
            self.assertEqual(new_type.id, max_type_id + 1, 'New Type (mirrored) should get next ID after max')

            new_supertype = SuperType.objects.create(name='New Public Type')
            self.assertEqual(
                new_supertype.id, max_supertype_id + 1, 'New SuperType (public) should get next ID after max'
            )

        finally:
            # Clean up fixture file
            try:
                os.unlink(fixture_path)
            except OSError:
                pass
