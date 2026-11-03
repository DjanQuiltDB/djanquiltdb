from unittest import mock

from django.core.management import CommandError, call_command
from django.db import DatabaseError
from django.db.backends.base.operations import BaseDatabaseOperations

from djanquiltdb import State
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import create_template_schema, use_shard
from djanquiltdb_tests import OverrideMirroredRoutingMixin, ShardingTransactionTestCase
from example.models import Organization, Shard, Type


class FlushTestCase(OverrideMirroredRoutingMixin, ShardingTransactionTestCase):
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

    def test(self):
        """
        Case: Call the flush command without extra options
        Expected: All schemas to be flushed
        """
        call_command('flush', verbosity=0, interactive=False)

        with use_shard(node_name='default'):
            self.assertFalse(Type.objects.exists())

        with use_shard(node_name='other'):
            self.assertFalse(Type.objects.exists())

        with use_shard(self.shard1):
            self.assertFalse(Organization.objects.exists())

        with use_shard(self.shard2):
            self.assertFalse(Organization.objects.exists())

    def test_single_database(self):
        """
        Case: Call the flush command with a single database specified
        Expected: All schemas on that database flushed, other schemas not touched
        """
        call_command('flush', verbosity=0, interactive=False, database='default')

        with use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertFalse(Type.objects.exists())

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertTrue(Type.objects.exists())

        with use_shard(self.shard1):
            self.assertFalse(Organization.objects.exists())

        with use_shard(self.shard2):
            self.assertTrue(Organization.objects.exists())

    def test_single_schema(self):
        """
        Case: Call the flush command with a single database and single schema specified
        Expected: Only that schema to be flushed, other schemas not touched
        """
        call_command('flush', verbosity=0, interactive=False, database='default', schema_name='el_chapo')

        with use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertTrue(Type.objects.exists())

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertTrue(Type.objects.exists())

        with use_shard(self.shard1):
            self.assertFalse(Organization.objects.exists())

        with use_shard(self.shard2):
            self.assertTrue(Organization.objects.exists())

    def test_a_shard_in_maintenance_is_named_in_the_error_and_the_other_schemas_are_flushed(self):
        """
        Case: Call the flush command for the database default while its shard sinaloa is in maintenance
        Expected: CommandError that names sinaloa and says to pass --ignore-maintenance, after the public schema of
                  default is flushed. Shard sinaloa keeps its data, and the other database is not touched
        """
        self.shard1.state = State.MAINTENANCE
        self.shard1.save(update_fields=['state'])

        # The flush of the public schema of default deletes the registry row of sinaloa on that node, so the row is
        # made active again only where it still exists.
        self.addCleanup(Shard.objects.filter(pk=self.shard1.pk).update, state=State.ACTIVE)

        with self.assertRaisesMessage(CommandError, 'Shard sinaloa: it is in maintenance.'):
            call_command('flush', verbosity=0, interactive=False, database='default')

        with use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertFalse(Type.objects.exists())

        with use_shard(self.shard1, active_only_schemas=False):
            self.assertTrue(Organization.objects.exists())

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertTrue(Type.objects.exists())

        with use_shard(self.shard2):
            self.assertTrue(Organization.objects.exists())

    def test_a_shard_that_cannot_be_flushed_does_not_stop_the_other_schemas(self):
        """
        Case: The flush SQL fails with a DatabaseError in the schema of sinaloa, and the flush command is called for all
              databases
        Expected: CommandError that names sinaloa and the error of its flush, after every other schema is flushed
        """
        execute_sql_flush = BaseDatabaseOperations.execute_sql_flush

        def execute_sql_flush_unless_el_chapo(operations, sql_list):
            if operations.connection.get_schema() == 'el_chapo':
                raise DatabaseError('The tables of el_chapo are locked.')

            return execute_sql_flush(operations, sql_list)

        with mock.patch.object(BaseDatabaseOperations, 'execute_sql_flush', execute_sql_flush_unless_el_chapo):
            with self.assertRaisesMessage(CommandError, 'Shard(s) sinaloa could not be flushed.') as raised:
                call_command('flush', verbosity=0, interactive=False)

        self.assertIn('Shard sinaloa: Database ', str(raised.exception))
        self.assertIn("couldn't be flushed", str(raised.exception))

        with use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertFalse(Type.objects.exists())

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertFalse(Type.objects.exists())

        with use_shard(self.shard2):
            self.assertFalse(Organization.objects.exists())

        with use_shard(self.shard1):
            self.assertTrue(Organization.objects.exists())

    def test_flush_succeeds_with_ignore_maintenance_flag(self):
        """
        Case: Call the flush command with --ignore-maintenance flag when a shard is in maintenance mode
        Expected: Shard flushed successfully despite maintenance status
        """
        self.shard1.state = State.MAINTENANCE
        self.shard1.save(update_fields=['state'])

        # Store shard ID before flush (since flush will delete it)
        shard1_id = self.shard1.id

        try:
            # Should not raise StateException
            call_command('flush', verbosity=0, interactive=False, database='default', ignore_maintenance=True)

            # Verify shard was flushed
            with use_shard(self.shard1, active_only_schemas=False):
                self.assertFalse(Organization.objects.exists())

            # Verify shard2 on other database was NOT flushed (we only flushed 'default')
            with use_shard(self.shard2):
                self.assertTrue(Organization.objects.exists())
        finally:
            # Reset shard state to ACTIVE for teardown if it still exists
            # (it might have been deleted during flush, but will be recreated in setUp)
            try:
                shard = Shard.objects.get(id=shard1_id)
                shard.state = State.ACTIVE
                shard.save(update_fields=['state'])
            except Shard.DoesNotExist:
                # Shard was deleted during flush, will be recreated in next setUp
                pass

    def test_flush_with_ignore_maintenance_all_databases(self):
        """
        Case: Call the flush command with --ignore-maintenance flag for all databases with maintenance shards
        Expected: All shards flushed successfully including maintenance ones
        """
        self.shard1.state = State.MAINTENANCE
        self.shard1.save(update_fields=['state'])

        self.shard2.state = State.MAINTENANCE
        self.shard2.save(update_fields=['state'])

        # Store shard IDs before flush (since flush will delete them)
        shard1_id = self.shard1.id
        shard2_id = self.shard2.id

        try:
            # Should not raise StateException
            call_command('flush', verbosity=0, interactive=False, ignore_maintenance=True)

            # Verify all shards were flushed
            with use_shard(self.shard1, active_only_schemas=False):
                self.assertFalse(Organization.objects.exists())

            with use_shard(self.shard2, active_only_schemas=False):
                self.assertFalse(Organization.objects.exists())

            # Verify public schemas were also flushed
            with use_shard(node_name='default'):
                self.assertFalse(Type.objects.exists())

            with use_shard(node_name='other'):
                self.assertFalse(Type.objects.exists())
        finally:
            # Reset shard states to ACTIVE for teardown if they still exist
            # (they might have been deleted during flush, but will be recreated in setUp)
            try:
                shard1 = Shard.objects.get(id=shard1_id)
                shard1.state = State.ACTIVE
                shard1.save(update_fields=['state'])
            except Shard.DoesNotExist:
                # Shard was deleted during flush, will be recreated in next setUp
                pass

            try:
                shard2 = Shard.objects.get(id=shard2_id)
                shard2.state = State.ACTIVE
                shard2.save(update_fields=['state'])
            except Shard.DoesNotExist:
                # Shard was deleted during flush, will be recreated in next setUp
                pass
