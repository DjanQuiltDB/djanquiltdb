import json
import os
from contextlib import contextmanager
from unittest import mock

from django.db import connections
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.postgresql.creation import DatabaseCreation as PostgresDatabaseCreation
from django.db.migrations.recorder import MigrationRecorder
from django.db.utils import load_backend
from django.test import SimpleTestCase

from djanquiltdb import ShardingMode, State
from djanquiltdb.db import connection
from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.management.executor import shared_migration_states_enabled
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME, get_database_creation_class
from djanquiltdb.postgresql_backend.creation import DatabaseCreation, TemplateDatabaseCreation
from djanquiltdb.utils import create_template_schema, get_sharding_mode, get_template_name, use_shard
from djanquiltdb_tests import ShardingTestCase, ShardingTransactionTestCase
from example.models import Cake, Shard, SuperType


class DatabaseCreationClassTestCase(ShardingTransactionTestCase):
    @contextmanager
    def new_connection(self, alias):
        """Creates a new connection and deletes it afterwards"""
        db = connections.databases[alias]
        backend = load_backend(db['ENGINE'])

        try:
            conn = backend.DatabaseWrapper(db, alias)
            yield conn
        finally:
            del conn

    @override_sharding_setting('DATABASE_CREATION_CLASS')
    def test_default(self):
        """
        Case: Call get_database_creation_class without having DATABASE_CREATION_CLASS set and then check the connection
              creation class
        Expected: Returns the default, which is django.db.backends.postgresql.creation.DatabaseCreation
        """
        self.assertEqual(get_database_creation_class(), DatabaseCreation)
        with self.new_connection('default') as conn:
            self.assertEqual(conn.creation.__class__, DatabaseCreation)

    @override_sharding_setting(
        'DATABASE_CREATION_CLASS', 'djanquiltdb.postgresql_backend.creation.TemplateDatabaseCreation'
    )
    def test_database_creation_class_set(self):
        """
        Case: Call get_database_creation_class with having DATABASE_CREATION_CLASS set and then check the connection
              creation class
        Expected: Returns sharding.postgresql_backend.creation.DatabaseCreation
        """
        self.assertEqual(get_database_creation_class(), TemplateDatabaseCreation)
        with self.new_connection('default') as conn:
            self.assertEqual(conn.creation.__class__, TemplateDatabaseCreation)


@mock.patch.object(BaseDatabaseCreation, 'create_test_db', autospec=True)
@override_sharding_setting('SHARED_TEST_MIGRATION_STATES', True)
class CreateTestDbTestCase(SimpleTestCase):
    def migrate_shares_states_while_creating(self, mock_create_test_db, **kwargs):
        """
        Create a test database through DatabaseCreation. Return whether migrate shares project states between schemas
        while Django builds the database.
        """
        sharing = []
        mock_create_test_db.side_effect = lambda *args, **kwargs: sharing.append(shared_migration_states_enabled())

        DatabaseCreation(connection).create_test_db(verbosity=0, **kwargs)

        self.assertFalse(shared_migration_states_enabled())
        return sharing[0]

    def test_new_test_database_shares_states(self, mock_create_test_db):
        """
        Case: Create a new test database with QUILT_DB['SHARED_TEST_MIGRATION_STATES'] set to True.
        Expected: migrate shares project states between schemas while Django builds the database, and stops sharing
                  afterwards.
        """
        self.assertTrue(self.migrate_shares_states_while_creating(mock_create_test_db))

    def test_kept_test_database_does_not_share_states(self, mock_create_test_db):
        """
        Case: Create a test database with keepdb (this migrates a database kept from an earlier run).
        Expected: migrate does not share the project states. It never shares them for a database that already holds
                  schemas.
        """
        self.assertFalse(self.migrate_shares_states_while_creating(mock_create_test_db, keepdb=True))

    @override_sharding_setting('SHARED_TEST_MIGRATION_STATES')
    def test_sharing_is_off_by_default(self, mock_create_test_db):
        """
        Case: Create a new test database without QUILT_DB['SHARED_TEST_MIGRATION_STATES'] set.
        Expected: migrate does not share the project states.
        """
        self.assertFalse(self.migrate_shares_states_while_creating(mock_create_test_db))

    @override_sharding_setting('SHARED_TEST_MIGRATION_STATES', False)
    def test_setting_turns_sharing_off(self, mock_create_test_db):
        """
        Case: Create a new test database with QUILT_DB['SHARED_TEST_MIGRATION_STATES'] set to False.
        Expected: migrate does not share the project states.
        """
        self.assertFalse(self.migrate_shares_states_while_creating(mock_create_test_db))

    def test_serialize_left_to_django_when_not_given(self, mock_create_test_db):
        """
        Case: Create a new test database without passing serialize (like Django's setup_databases).
        Expected: Django's create_test_db does not get serialize either, so its own default applies. It does not warn
                  about the deprecated argument and does not serialize the database.
        """
        DatabaseCreation(connection).create_test_db(verbosity=0)

        self.assertNotIn('serialize', mock_create_test_db.call_args.kwargs)

    def test_serialize_passed_on_when_given(self, mock_create_test_db):
        """
        Case: Create a new test database passing serialize as True, and as False.
        Expected: Django's create_test_db gets the same value.
        """
        for serialize in (True, False):
            with self.subTest(serialize=serialize):
                DatabaseCreation(connection).create_test_db(verbosity=0, serialize=serialize)

                self.assertIs(mock_create_test_db.call_args.kwargs['serialize'], serialize)

    def test_returns_what_django_returns(self, mock_create_test_db):
        """
        Case: Create a new test database.
        Expected: The name Django's create_test_db returns.
        """
        mock_create_test_db.return_value = 'test_db'

        self.assertEqual(DatabaseCreation(connection).create_test_db(verbosity=0), 'test_db')


class DatabaseCreationTestCase(ShardingTestCase):
    available_apps = None  # We want to have all apps in here

    def setUp(self):
        super().setUp()

        self.creation = DatabaseCreation(connection)
        self.test_serialized_contents = json.loads(self.creation.serialize_db_to_string())

    def test_serialize_public(self):
        """
        Case: Call serialize_db_to_string without having shards or a template schema.
        Expected: Public schema serialized and returned, containing only instances that are from sharded models.
        """
        serialized_contents = json.loads(self.creation.serialize_db_to_string())

        self.assertEqual(list(serialized_contents.keys()), [PUBLIC_SCHEMA_NAME])

        # All instance models are mirrored models only
        for data in serialized_contents[PUBLIC_SCHEMA_NAME]:
            self.assertEqual(get_sharding_mode(*data['model'].split('.')), ShardingMode.MIRRORED)

    def test_serialize_with_template_schema(self):
        """
        Case: Call serialize_db_to_string while having a template schema.
        Expected: Both public schema and template schema are serialized, template schema contains no instances.
        """
        create_template_schema(self.creation.connection.alias)
        serialized_contents = json.loads(self.creation.serialize_db_to_string())
        template_name = get_template_name()

        self.assertCountEqual(list(serialized_contents.keys()), [PUBLIC_SCHEMA_NAME, template_name])
        self.assertEqual(serialized_contents[template_name], [])

    def test_serialize_with_shard(self):
        """
        Case: Call serialize_db_to_string while having a template schema and a shard.
        Expected: Shard object that has been saved on the public schema is also in the public serialized contents,
                  serialized content of the shard contains no data when there are no instances saved on the shard but
                  does contain instances when there is data on the shard.
        """
        create_template_schema(self.creation.connection.alias)
        shard = Shard.objects.create(
            node_name=self.creation.connection.alias, schema_name='test_schema', alias='schema', state=State.ACTIVE
        )
        serialized_contents = json.loads(self.creation.serialize_db_to_string())

        self.test_serialized_contents[PUBLIC_SCHEMA_NAME].append(
            {
                'pk': shard.pk,
                'fields': {
                    'schema_name': shard.schema_name,
                    'node_name': shard.node_name,
                    'alias': shard.alias,
                    'state': shard.state,
                },
                'model': 'example.shard',
            }
        )

        self.assertCountEqual(
            serialized_contents[PUBLIC_SCHEMA_NAME], self.test_serialized_contents[PUBLIC_SCHEMA_NAME]
        )
        self.assertEqual(serialized_contents[shard.schema_name], [])

        # Now create something on the shard
        with shard.use():
            cake = Cake.objects.create(name='Strawberry cake')

        serialized_contents = json.loads(self.creation.serialize_db_to_string())

        self.assertCountEqual(
            serialized_contents[PUBLIC_SCHEMA_NAME], self.test_serialized_contents[PUBLIC_SCHEMA_NAME]
        )  # No changes here

        # But the shard now has instances that has been serialized
        self.assertEqual(
            serialized_contents[shard.schema_name],
            [
                {
                    'pk': cake.pk,
                    'fields': {
                        'name': cake.name,
                        'type': None,
                        'coating_type': None,
                    },
                    'model': 'example.cake',
                }
            ],
        )

    def test_serialize_with_inactive_shard(self):
        """
        Case: Call serialize_db_to_string while having an inactive shard.
        Expected: Shard content still serialized.
        """
        create_template_schema(self.creation.connection.alias)
        shard = Shard.objects.create(
            node_name=self.creation.connection.alias, schema_name='test_schema', alias='schema', state=State.MAINTENANCE
        )
        serialized_contents = json.loads(self.creation.serialize_db_to_string())

        self.assertIn(shard.schema_name, serialized_contents)

    def test_deserialize(self):
        """
        Case: Have serialized data and call deserialize_db_from_string.
        Expected: All objects created on the correct schema.
        """
        create_template_schema(self.creation.connection.alias)
        template_name = get_template_name()
        shard = Shard.objects.create(
            node_name=self.creation.connection.alias, schema_name='test_schema', alias='schema', state=State.ACTIVE
        )
        serialized_contents = {
            shard.schema_name: [
                {
                    'pk': 1,
                    'fields': {
                        'name': 'Bebinca',
                    },
                    'model': 'example.cake',
                }
            ],
            template_name: [
                {
                    'pk': 37,
                    'fields': {
                        'name': 'Baumkuchen',
                    },
                    'model': 'example.cake',
                }
            ],
            PUBLIC_SCHEMA_NAME: [
                {
                    'pk': 5,
                    'fields': {
                        'name': 'Cake',
                    },
                    'model': 'example.supertype',
                }
            ],
        }

        self.creation.deserialize_db_from_string(json.dumps(serialized_contents))

        with shard.use():
            self.assertTrue(Cake.objects.filter(name='Bebinca').exists())

        with use_shard(node_name=self.creation.connection.alias, schema_name=template_name):
            self.assertTrue(Cake.objects.filter(name='Baumkuchen').exists())

        with use_shard(node_name=self.creation.connection.alias, schema_name=PUBLIC_SCHEMA_NAME):
            self.assertTrue(SuperType.objects.filter(name='Cake').exists())


class MigrationStateHashTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        self.creation = DatabaseCreation(connections['default'])

    def test_same_migrations_give_the_same_state(self):
        """
        Case: Get the migration state of the test database twice.
        Expected: The same digest both times.
        """
        state = self.creation.get_migration_state_hash()

        self.assertRegex(state, '^[0-9a-f]{64}$')
        self.assertEqual(self.creation.get_migration_state_hash(), state)

    def test_migration_recorded_in_another_schema_changes_the_state(self):
        """
        Case: Get the migration state of the test database, record a migration in its template schema, and get the state
              again.
        Expected: The digests differ. The state covers the migrations of every schema, not only the public schema.
        """
        create_template_schema('default', migrate=False)
        state = self.creation.get_migration_state_hash()

        with use_shard(node_name='default', schema_name=get_template_name()) as env:
            MigrationRecorder(env.connection).record_applied('example', '0099_recorded')

        self.assertNotEqual(self.creation.get_migration_state_hash(), state)


MIGRATION_STATE_HASH = 'a' * 64


class CloneTestDbTestCase(ShardingTestCase):
    """
    Tests for cloning the test database for a parallel test run. A scratch database stands in for the clone. A mock
    replaces Django's own cloning and creates the scratch database when it does not exist. (Under keepdb, Django also
    keeps an existing clone and creates a missing one.)
    """

    def setUp(self):
        super().setUp()

        self.creation = DatabaseCreation(connections['default'])
        # The suite itself runs in parallel clones, so each process needs its own scratch database.
        self.scratch_database_name = 'test_clone_scratch_{}'.format(os.getpid())

        mock.patch.object(
            DatabaseCreation, 'get_test_db_clone_settings', return_value={'NAME': self.scratch_database_name}
        ).start()
        mock.patch.object(DatabaseCreation, 'get_migration_state_hash', return_value=MIGRATION_STATE_HASH).start()
        self.mock_clone_test_db = mock.patch.object(
            PostgresDatabaseCreation, '_clone_test_db', autospec=True, side_effect=self.fake_django_clone
        ).start()
        self.addCleanup(mock.patch.stopall)

        self.drop_scratch_database()
        self.addCleanup(self.drop_scratch_database)

    def execute_on_server(self, sql, params=None):
        with self.creation._nodb_cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone() if cursor.description else None

    def scratch_database_oid(self):
        """
        Return the oid of the scratch database, or None when it does not exist. A recreated database gets a new oid.
        """
        row = self.execute_on_server('SELECT oid FROM pg_database WHERE datname = %s', [self.scratch_database_name])
        return row and row[0]

    def scratch_database_comment(self):
        return self.execute_on_server(
            "SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = %s",
            [self.scratch_database_name],
        )[0]

    def create_scratch_database(self, comment=None):
        self.execute_on_server('CREATE DATABASE {}'.format(self.creation._quote_name(self.scratch_database_name)))
        if comment is not None:
            self.execute_on_server(
                "COMMENT ON DATABASE {} IS '{}'".format(self.creation._quote_name(self.scratch_database_name), comment)
            )

    def drop_scratch_database(self):
        self.execute_on_server(
            'DROP DATABASE IF EXISTS {}'.format(self.creation._quote_name(self.scratch_database_name))
        )

    def fake_django_clone(self, creation, suffix, verbosity, keepdb=False):
        if self.scratch_database_oid() is None:
            self.create_scratch_database()

    def test_kept_clone_at_the_same_state_is_kept(self):
        """
        Case: Clone the test database with keepdb. The clone exists and has the migration state of the test database.
        Expected: The clone is kept. Django gets the existing database, which still has that state.
        """
        self.create_scratch_database(comment=MIGRATION_STATE_HASH)
        oid = self.scratch_database_oid()

        self.creation._clone_test_db('1', verbosity=0, keepdb=True)

        self.assertEqual(self.scratch_database_oid(), oid)
        self.mock_clone_test_db.assert_called_once_with(self.creation, '1', 0, True)
        self.assertEqual(self.scratch_database_comment(), MIGRATION_STATE_HASH)

    def test_kept_clone_at_another_state_is_cloned_again(self):
        """
        Case: Clone the test database with keepdb. The clone exists, but has another migration state or no state at all.
        Expected: The clone is dropped, and Django clones the test database again. The new clone has the state of the
                  test database.
        """
        for comment in ('b' * 64, None):
            with self.subTest(comment=comment):
                self.drop_scratch_database()
                self.create_scratch_database(comment=comment)
                oid = self.scratch_database_oid()

                self.creation._clone_test_db('1', verbosity=0, keepdb=True)

                self.assertNotIn(self.scratch_database_oid(), (oid, None))
                self.assertEqual(self.scratch_database_comment(), MIGRATION_STATE_HASH)

    def test_missing_clone_is_cloned(self):
        """
        Case: Clone the test database with keepdb. The clone does not exist.
        Expected: Django clones the test database. The clone has the state of the test database.
        """
        self.creation._clone_test_db('1', verbosity=0, keepdb=True)

        self.assertIsNotNone(self.scratch_database_oid())
        self.assertEqual(self.scratch_database_comment(), MIGRATION_STATE_HASH)

    def test_clone_without_keepdb_is_left_to_django(self):
        """
        Case: Clone the test database without keepdb. A clone exists with another migration state.
        Expected: The clone is not dropped before Django gets it, because Django clones the test database again anyway
                  without keepdb. The clone then has the state of the test database.
        """
        self.create_scratch_database(comment='b' * 64)
        oid = self.scratch_database_oid()

        self.creation._clone_test_db('1', verbosity=0, keepdb=False)

        self.assertEqual(self.scratch_database_oid(), oid)
        self.mock_clone_test_db.assert_called_once_with(self.creation, '1', 0, False)
        self.assertEqual(self.scratch_database_comment(), MIGRATION_STATE_HASH)
