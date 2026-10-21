import io
import json
import os
import tempfile
from unittest import mock

from django.core import serializers
from django.core.management import call_command
from django.db import DatabaseError, connection, models
from django.db.models.signals import post_migrate, post_save

from djanquiltdb import State
from djanquiltdb.options import ShardOptions
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import create_template_schema, get_template_name, migrate_schema, use_shard, use_shard_for
from djanquiltdb_tests import ShardingTestCase, ShardingTransactionTestCase
from example.models import CakeType, CoatingType, Organization, Shard, Statement, SuperType, Type, User


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

    def test_compressed_fixtures_load(self):
        """
        Case: Load a gzip-compressed JSON fixture through the schema-aware loader.
        Expected: The entries load.
        """
        import gzip

        fixture_data = [{'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'Compressed'}}]
        with tempfile.NamedTemporaryFile(suffix='.json.gz', delete=False) as f:
            with gzip.open(f.name, 'wt') as gz:
                json.dump(fixture_data, gz)
            fixture_path = f.name
        self.addCleanup(os.unlink, fixture_path)

        call_command('loaddata', fixture_path, verbosity=0)

        self.assertEqual(SuperType.objects.get(pk=1).name, 'Compressed')

    def test_database_option_targets_the_named_node(self):
        """
        Case: Load a fixture with --database naming the other node.
        Expected: The entries land on the other node's public schema, and the default node stays untouched.
        """
        fixture_data = [{'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'Elsewhere'}}]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(fixture_data, f)
            fixture_path = f.name
        self.addCleanup(os.unlink, fixture_path)

        call_command('loaddata', fixture_path, database='other', verbosity=0)

        self.assertEqual(SuperType.objects.using('other').count(), 1)
        self.assertEqual(SuperType.objects.count(), 0)

    def write_fixture(self, fixture_data):
        return self.write_fixture_text(json.dumps(fixture_data), suffix='.json')

    def write_fixture_text(self, text, suffix):
        with tempfile.NamedTemporaryFile(mode='w', suffix=suffix, delete=False) as f:
            f.write(text)
        self.addCleanup(os.unlink, f.name)
        return f.name

    def create_shard(self):
        create_template_schema()
        return Shard.objects.create(node_name='default', schema_name='test_schema', alias='schema', state=State.ACTIVE)

    def test_a_fixture_for_the_public_schema_leaves_the_template_schema_alone(self):
        """
        Case: The template schema exists, and a fixture without _schema entries is loaded.
        Expected: The entries load, and loaddata neither creates nor migrates the template schema.
        """
        create_template_schema()
        fixture_path = self.write_fixture([{'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'Public'}}])

        with (
            mock.patch('djanquiltdb.management.commands.loaddata.create_template_schema') as mock_create_template,
            mock.patch('djanquiltdb.management.commands.loaddata.migrate_schema') as mock_migrate_schema,
        ):
            call_command('loaddata', fixture_path, verbosity=0)

        self.assertEqual(SuperType.objects.get(pk=1).name, 'Public')
        self.assertFalse(mock_create_template.called)
        self.assertFalse(mock_migrate_schema.called)

    def test_a_fixture_for_an_existing_shard_leaves_the_template_schema_alone(self):
        """
        Case: A fixture with _schema entries for the schema of an existing shard is loaded.
        Expected: The entries load into the shard, and loaddata neither creates nor migrates the template schema.
        """
        create_template_schema()
        shard = Shard.objects.create(node_name='default', schema_name='test_schema', alias='schema', state=State.ACTIVE)
        fixture_path = self.write_fixture(
            [{'model': 'example.Organization', 'pk': 1, 'fields': {'name': 'Sharded'}, '_schema': shard.schema_name}]
        )

        with (
            mock.patch('djanquiltdb.management.commands.loaddata.create_template_schema') as mock_create_template,
            mock.patch('djanquiltdb.management.commands.loaddata.migrate_schema') as mock_migrate_schema,
        ):
            call_command('loaddata', fixture_path, verbosity=0)

        with use_shard(shard):
            self.assertEqual(Organization.objects.get(pk=1).name, 'Sharded')
        self.assertFalse(mock_create_template.called)
        self.assertFalse(mock_migrate_schema.called)

    def test_a_fixture_for_a_new_schema_creates_the_template_schema_and_the_schema(self):
        """
        Case: Neither the template schema nor the schema that the _schema entries of a fixture name exist, and the
              fixture is loaded.
        Expected: The template schema and the schema are created, and the entries load into the schema.
        """
        fixture_path = self.write_fixture(
            [{'model': 'example.Organization', 'pk': 1, 'fields': {'name': 'New'}, '_schema': 'new_schema'}]
        )

        call_command('loaddata', fixture_path, verbosity=0)

        self.assertEqual(connection.get_ps_schema(get_template_name()), get_template_name())
        with use_shard(node_name='default', schema_name='new_schema'):
            self.assertEqual(Organization.objects.get(pk=1).name, 'New')

    def test_a_new_schema_after_the_entries_of_another_schema_is_created_outside_that_schema(self):
        """
        Case: The first entry of a fixture is for the schema of an existing shard, and the second entry is for a schema
              that does not exist. A post_migrate receiver records the database that it is sent for.
        Expected: The migration of the new schema sends post_migrate for the default node, and not for the shard that
                  loaddata entered for the first entry.
        """
        shard = self.create_shard()
        fixture_path = self.write_fixture(
            [
                {'model': 'example.Organization', 'pk': 1, 'fields': {'name': 'Sharded'}, '_schema': shard.schema_name},
                {'model': 'example.Organization', 'pk': 1, 'fields': {'name': 'New'}, '_schema': 'new_schema'},
            ]
        )
        databases_of_post_migrate = []

        def record_the_database(using, **kwargs):
            databases_of_post_migrate.append(using)

        post_migrate.connect(record_the_database)
        self.addCleanup(post_migrate.disconnect, record_the_database)

        call_command('loaddata', fixture_path, verbosity=0)

        self.assertTrue(databases_of_post_migrate)
        self.assertEqual(set(databases_of_post_migrate), {'default'})
        with use_shard(node_name='default', schema_name='new_schema'):
            self.assertEqual(Organization.objects.get(pk=1).name, 'New')

    def test_a_template_schema_that_loaddata_creates_is_migrated_once(self):
        """
        Case: Neither the template schema nor the schema that the _schema entry of a fixture names exist, and the
              fixture is loaded.
        Expected: loaddata migrates the template schema once, after create_template_schema() has created it without
                  migrating it.
        """
        fixture_path = self.write_fixture(
            [{'model': 'example.Organization', 'pk': 1, 'fields': {'name': 'New'}, '_schema': 'new_schema'}]
        )

        with (
            mock.patch('djanquiltdb.utils.migrate_schema') as mock_migrate_in_create_template_schema,
            mock.patch(
                'djanquiltdb.management.commands.loaddata.migrate_schema', wraps=migrate_schema
            ) as mock_migrate_in_loaddata,
        ):
            call_command('loaddata', fixture_path, verbosity=0)

        self.assertFalse(mock_migrate_in_create_template_schema.called)
        self.assertEqual(mock_migrate_in_loaddata.call_count, 1)

    def test_excluded_models_are_not_loaded(self):
        """
        Case: A fixture has an Organization entry for a shard and a SuperType entry for the public schema, and is loaded
              with --exclude example.Organization.
        Expected: The SuperType loads, and the Organization does not.
        """
        shard = self.create_shard()
        fixture_path = self.write_fixture(
            [
                {
                    'model': 'example.Organization',
                    'pk': 1,
                    'fields': {'name': 'Excluded'},
                    '_schema': shard.schema_name,
                },
                {'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'Loaded'}},
            ]
        )

        call_command('loaddata', fixture_path, exclude=['example.Organization'], verbosity=0)

        self.assertTrue(SuperType.objects.filter(pk=1).exists())
        with use_shard(shard):
            self.assertFalse(Organization.objects.exists())

    def test_an_entry_whose_model_is_not_in_its_schema_is_skipped(self):
        """
        Case: A fixture without _schema entries has an Organization, which is a sharded model, and a SuperType, and is
              loaded into the public schema.
        Expected: The SuperType loads, the Organization is skipped, and the output counts both, as Django's loaddata
                  counts an object that the router does not allow.
        """
        fixture_path = self.write_fixture(
            [
                {'model': 'example.Organization', 'pk': 1, 'fields': {'name': 'Sharded'}},
                {'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'Public'}},
            ]
        )
        output = io.StringIO()

        call_command('loaddata', fixture_path, verbosity=1, stdout=output)

        self.assertTrue(SuperType.objects.filter(pk=1).exists())
        self.assertEqual(output.getvalue(), 'Installed 1 object(s) (of 2) from 1 fixture(s)\n')

    def test_a_natural_key_is_looked_up_on_the_node_that_database_names(self):
        """
        Case: A CakeType exists only on the other node, and a fixture with a CoatingType whose type is the natural key
              of that CakeType is loaded with --database other.
        Expected: The CoatingType loads on the other node, with that CakeType as its type.
        """
        CakeType.objects.using('other').create(name='layered')
        fixture_path = self.write_fixture(
            [
                {
                    'model': 'example.CoatingType',
                    'pk': 1,
                    'fields': {'hash': 'abc', 'type': ['layered'], 'super_type': []},
                }
            ]
        )

        call_command('loaddata', fixture_path, database='other', verbosity=0)

        self.assertEqual(CoatingType.objects.using('other').get(pk=1).type.name, 'layered')

    def test_a_natural_key_that_names_a_later_entry_loads(self):
        """
        Case: A fixture has a Type whose super is the natural key of a SuperType that the fixture lists after it. The
              super of a Type can be empty, which Django's loaddata needs to save the Type before its SuperType.
        Expected: Both load, and the Type has that SuperType as its super.
        """
        fixture_path = self.write_fixture(
            [
                {'model': 'example.Type', 'pk': 1, 'fields': {'name': 'Admiral', 'super': ['Character']}},
                {'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'Character'}},
            ]
        )

        call_command('loaddata', fixture_path, verbosity=0)

        self.assertEqual(Type.objects.get(pk=1).super.name, 'Character')

    def test_a_forward_reference_before_an_excluded_entry_with_a_forward_reference_is_resolved(self):
        """
        Case: A fixture has a Type whose super is the natural key of a SuperType that the fixture lists later, then a
              CoatingType whose super types name that SuperType too, and then the SuperType. It is loaded with
              --exclude example.CoatingType.
        Expected: The Type has that SuperType as its super, and no CoatingType is loaded. Django's save_obj() keeps the
                  deferred fields of an object that it saves, and not those of an excluded object.
        """
        CakeType.objects.create(name='layered')
        fixture_path = self.write_fixture(
            [
                {'model': 'example.Type', 'pk': 1, 'fields': {'name': 'Admiral', 'super': ['Character']}},
                {
                    'model': 'example.CoatingType',
                    'pk': 1,
                    'fields': {'hash': 'abc', 'type': ['layered'], 'super_type': [['Character']]},
                },
                {'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'Character'}},
            ]
        )

        call_command('loaddata', fixture_path, exclude=['example.CoatingType'], verbosity=0)

        self.assertEqual(Type.objects.get(pk=1).super.name, 'Character')
        self.assertFalse(CoatingType.objects.exists())

    def test_an_excluded_entry_with_a_natural_key_that_names_a_later_entry_is_skipped(self):
        """
        Case: A fixture has a Type whose super is the natural key of a SuperType that the fixture lists after it, and is
              loaded with --exclude example.Type.
        Expected: The SuperType loads, and the Type does not.
        """
        fixture_path = self.write_fixture(
            [
                {'model': 'example.Type', 'pk': 1, 'fields': {'name': 'Admiral', 'super': ['Character']}},
                {'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'Character'}},
            ]
        )

        call_command('loaddata', fixture_path, exclude=['example.Type'], verbosity=0)

        self.assertTrue(SuperType.objects.filter(name='Character').exists())
        self.assertFalse(Type.objects.exists())

    def test_a_json_lines_fixture_loads_into_its_schemas(self):
        """
        Case: A JSON Lines fixture has an Organization entry for a shard and a SuperType entry for the public schema.
        Expected: Each entry loads into its schema.
        """
        shard = self.create_shard()
        fixture_path = self.write_fixture_text(
            '\n'.join(
                [
                    json.dumps(
                        {
                            'model': 'example.Organization',
                            'pk': 1,
                            'fields': {'name': 'Lines'},
                            '_schema': shard.schema_name,
                        }
                    ),
                    json.dumps({'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'Lines'}}),
                ]
            ),
            suffix='.jsonl',
        )

        call_command('loaddata', fixture_path, verbosity=0)

        self.assertTrue(SuperType.objects.filter(pk=1).exists())
        with use_shard(shard):
            self.assertTrue(Organization.objects.filter(pk=1).exists())

    def test_a_fixture_from_stdin_loads(self):
        """
        Case: A JSON fixture is read from stdin, with '-' as the fixture and --format json.
        Expected: The entries load.
        """
        fixture = json.dumps([{'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'Piped'}}])

        with mock.patch('sys.stdin', io.StringIO(fixture)):
            call_command('loaddata', '-', format='json', verbosity=0)

        self.assertEqual(SuperType.objects.get(pk=1).name, 'Piped')

    def test_an_xml_fixture_loads_into_the_schema_that_database_names(self):
        """
        Case: An XML fixture with a SuperType is loaded with --database other.
        Expected: The SuperType loads into the public schema of the other node.
        """
        fixture_path = self.write_fixture_text(
            serializers.serialize('xml', [SuperType(pk=1, name='Markup')]), suffix='.xml'
        )

        call_command('loaddata', fixture_path, database='other', verbosity=0)

        self.assertEqual(SuperType.objects.using('other').get(pk=1).name, 'Markup')
        self.assertFalse(SuperType.objects.exists())

    def test_entries_are_saved_in_the_order_of_the_fixture(self):
        """
        Case: A fixture lists a SuperType for the public schema, an Organization for a shard, a second SuperType and a
              second Organization, in that order.
        Expected: The entries are saved in the order of the fixture.
        """
        shard = self.create_shard()
        saved_entries = []

        def record_saved_entry(sender, instance, raw, **kwargs):
            if raw:
                saved_entries.append((sender._meta.label, instance.pk))

        post_save.connect(record_saved_entry, weak=False)
        self.addCleanup(post_save.disconnect, record_saved_entry)
        fixture_path = self.write_fixture(
            [
                {'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'First'}},
                {'model': 'example.Organization', 'pk': 1, 'fields': {'name': 'Second'}, '_schema': shard.schema_name},
                {'model': 'example.SuperType', 'pk': 2, 'fields': {'name': 'Third'}},
                {'model': 'example.Organization', 'pk': 2, 'fields': {'name': 'Fourth'}, '_schema': shard.schema_name},
            ]
        )

        call_command('loaddata', fixture_path, verbosity=0)

        self.assertEqual(
            saved_entries,
            [
                ('example.SuperType', 1),
                ('example.Organization', 1),
                ('example.SuperType', 2),
                ('example.Organization', 2),
            ],
        )

    def test_the_output_is_the_line_of_django(self):
        """
        Case: A fixture with a SuperType for the public schema and an Organization for a shard is loaded with
              verbosity 1.
        Expected: The output is Django's own line, which counts both entries and the fixture.
        """
        shard = self.create_shard()
        fixture_path = self.write_fixture(
            [
                {'model': 'example.SuperType', 'pk': 1, 'fields': {'name': 'Public'}},
                {'model': 'example.Organization', 'pk': 1, 'fields': {'name': 'Sharded'}, '_schema': shard.schema_name},
            ]
        )
        output = io.StringIO()

        call_command('loaddata', fixture_path, verbosity=1, stdout=output)

        self.assertEqual(output.getvalue(), 'Installed 2 object(s) from 1 fixture(s)\n')

    def test_a_fixture_without_entries_warns(self):
        """
        Case: A JSON fixture without entries is loaded.
        Expected: A RuntimeWarning says that the fixture has no data, as Django's loaddata warns.
        """
        fixture_path = self.write_fixture([])

        with self.assertWarnsMessage(RuntimeWarning, 'No fixture data found for'):
            call_command('loaddata', fixture_path, verbosity=0)

    def test_an_error_names_the_fixture_file_and_the_entry(self):
        """
        Case: A fixture has an Organization for a shard whose name is longer than the column allows.
        Expected: The error names the fixture file and the Organization with its primary key.
        """
        shard = self.create_shard()
        fixture_path = self.write_fixture(
            [{'model': 'example.Organization', 'pk': 7, 'fields': {'name': 'x' * 101}, '_schema': shard.schema_name}]
        )

        with self.assertRaises(DatabaseError) as raised:
            call_command('loaddata', fixture_path, verbosity=0)

        self.assertIn("Problem installing fixture '{}'".format(fixture_path), str(raised.exception))
        self.assertIn('Could not load example.Organization(pk=7)', str(raised.exception))

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


class LoadDataTransactionTestCase(ShardingTransactionTestCase):
    def test_forward_references_within_a_fixture_load(self):
        """
        Case: A fixture lists a sharded object before the object its foreign key points at.
        Expected: The load succeeds.
        """
        fixture_data = [
            {
                'model': 'example.Shard',
                'pk': 1,
                'fields': {'node_name': 'default', 'schema_name': 'test_shard', 'alias': 'test_shard', 'state': 'A'},
            },
            {
                'model': 'example.Statement',
                'pk': 1,
                'fields': {'content': 'Forward!', 'user': 1, 'offset': 0},
                '_schema': 'test_shard',
            },
            {
                'model': 'example.User',
                'pk': 1,
                'fields': {'name': 'Late User', 'email': 'late@example.com', 'password': 'x'},
                '_schema': 'test_shard',
            },
        ]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(fixture_data, f)
            fixture_path = f.name
        self.addCleanup(os.unlink, fixture_path)

        call_command('loaddata', fixture_path, verbosity=0)

        with use_shard(Shard.objects.get(alias='test_shard')):
            self.assertEqual(Statement.objects.get(pk=1).user_id, 1)
