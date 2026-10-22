from django.core.management import call_command
from django.db import ProgrammingError, connections, models, transaction
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.state import ModelState
from django.test import override_settings

from djanquiltdb.db import connection
from djanquiltdb.operations import CreateMissingMirroredTable
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import use_shard
from example.models import Shard
from migration_tests.models import MirroredModel, SuperMirroredModel
from migration_tests.tests.hinted_migrations import SHARD_SCHEMA, TEMPLATE_SCHEMA, MigrationPlacementTestCase

MIRRORED_LATER_MODULES = {'migration_tests': 'migration_tests.test_migrations_mirrored_later'}


class CreateMissingMirroredTableTestCase(MigrationPlacementTestCase):
    def relation_exists_on_node(self, node_name, db_name, schema_name):
        """
        Whether a table or a view of that name is in that schema of that node.
        """
        with connections[node_name].cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM pg_catalog.pg_class cls
                JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
                WHERE cls.relname = %s AND nsp.nspname = %s
                """,
                [db_name, schema_name],
            )
            return cursor.fetchone()[0] == 1

    def constraint_columns(self, node_name, db_name):
        """
        The columns of each constraint and index of the table in the public schema of the node, with the kind of each.
        """
        connection = connections[node_name]
        with connection.cursor() as cursor:
            constraints = connection.introspection.get_constraints(cursor, db_name)

        return sorted(
            (
                tuple(constraint['columns']),
                constraint['primary_key'],
                constraint['unique'],
                constraint['foreign_key'] or (),
                constraint['index'],
            )
            for constraint in constraints.values()
        )

    def drop_table_on_other_node(self, db_name):
        with connections['other'].cursor() as cursor:
            cursor.execute('DROP TABLE "{}"."{}" CASCADE'.format(PUBLIC_SCHEMA_NAME, db_name))

    def migrate_the_schemas_to_the_initial_migration(self):
        """
        Migrates the templates and the shard, which the test case creates without tables, to the initial migration,
        while every node has the tables of the mirrored models that the sharded models refer to.
        """
        call_command('migrate', 'migration_tests', '0001_migrations_test_init', verbosity=0)

    @override_settings(MIGRATION_MODULES=MIRRORED_LATER_MODULES)
    def test_migrate_creates_the_tables_on_the_node_that_does_not_have_them(self):
        """
        Case: Drop the tables of SuperMirroredModel and MirroredModel on the other node, as the CreateModel of a model
              that was not mirrored yet leaves them, add rows on the default node, and migrate a migration with
              CreateMissingMirroredTable for both models.
        Expected: Both tables in the public schema of the other node, with the same constraints and indexes as on the
                  default node, and in no shard or template schema. The tables on the default node keep their rows.
        """
        db_names = [SuperMirroredModel._meta.db_table, MirroredModel._meta.db_table]

        super_mirrored = SuperMirroredModel.objects.using('default').create(name='super')
        MirroredModel.objects.using('default').create(name='mirrored', super=super_mirrored)

        self.migrate_the_schemas_to_the_initial_migration()
        for db_name in reversed(db_names):
            self.drop_table_on_other_node(db_name)

        call_command('migrate', 'migration_tests', verbosity=0)

        for db_name in db_names:
            with self.subTest(db_name):
                self.assertTrue(self.relation_exists_on_node('other', db_name, PUBLIC_SCHEMA_NAME))
                self.assertFalse(self.relation_exists_on_node('other', db_name, TEMPLATE_SCHEMA))
                self.assertFalse(self.relation_exists_on_node('default', db_name, TEMPLATE_SCHEMA))
                self.assertFalse(self.relation_exists_on_node('default', db_name, SHARD_SCHEMA))
                self.assertEqual(self.constraint_columns('other', db_name), self.constraint_columns('default', db_name))

        self.assertEqual(list(MirroredModel.objects.using('default').values_list('name', flat=True)), ['mirrored'])
        self.assertFalse(MirroredModel.objects.using('other').exists())

    @override_settings(MIGRATION_MODULES=MIRRORED_LATER_MODULES)
    def test_migrate_backwards_keeps_the_tables(self):
        """
        Case: Drop the table of MirroredModel on the other node, migrate the migration with CreateMissingMirroredTable,
              and migrate back to the migration before it.
        Expected: The table stays on both nodes.
        """
        db_name = MirroredModel._meta.db_table
        self.migrate_the_schemas_to_the_initial_migration()
        self.drop_table_on_other_node(db_name)

        call_command('migrate', 'migration_tests', verbosity=0)
        call_command('migrate', 'migration_tests', '0001_migrations_test_init', verbosity=0)

        self.assertTrue(self.relation_exists_on_node('default', db_name, PUBLIC_SCHEMA_NAME))
        self.assertTrue(self.relation_exists_on_node('other', db_name, PUBLIC_SCHEMA_NAME))

    def test_upgrade_of_a_shard_model_that_was_not_mirrored(self):
        """
        Case: Drop the shard registry on the other node, as a Shard model that was not mirrored leaves it, and enter a
              shard on the default node while the other node is active. Then run CreateMissingMirroredTable for the
              Shard model on the other node, copy the row of the shard to it as the replication of the project does,
              and enter the shard again.
        Expected: The first entry raises ProgrammingError, because the other node has no shard registry. After the
                  upgrade, the shard is entered.
        """
        shard = Shard.objects.get(alias='sina')
        self.drop_table_on_other_node(Shard._meta.db_table)

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME):
            with (
                self.assertRaisesMessage(ProgrammingError, 'relation "example_shard" does not exist'),
                transaction.atomic(using='other'),
            ):
                with use_shard(shard):
                    pass

        state = MigrationLoader(connections['default']).project_state()
        with connections['other'].schema_editor() as schema_editor:
            CreateMissingMirroredTable('Shard').database_forwards('example', schema_editor, state, state)

        Shard.objects.using('other').bulk_create([shard])

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME):
            with use_shard(shard), connection.cursor() as cursor:
                cursor.execute('SELECT current_schema()')
                self.assertEqual(cursor.fetchone()[0], SHARD_SCHEMA)

    def test_a_node_that_has_the_table_is_left_as_it_is(self):
        """
        Case: Run CreateMissingMirroredTable for MirroredModel on the default node, which has the table with a row.
        Expected: No statement changes the table, and the row is still there.
        """
        MirroredModel.objects.using('default').create(name='mirrored')
        state = MigrationLoader(connections['default']).project_state()

        with connections['default'].schema_editor(collect_sql=True) as schema_editor:
            CreateMissingMirroredTable('MirroredModel').database_forwards(
                'migration_tests', schema_editor, state, state
            )

        self.assertEqual(schema_editor.collected_sql, [])
        self.assertEqual(list(MirroredModel.objects.using('default').values_list('name', flat=True)), ['mirrored'])

    def test_a_model_that_is_not_mirrored_is_refused(self):
        """
        Case: Run CreateMissingMirroredTable for ShardedModel, which is sharded, on the other node.
        Expected: ValueError, because the operation only creates the table of a mirrored model.
        """
        state = MigrationLoader(connections['default']).project_state()

        with (
            self.assertRaisesMessage(ValueError, 'migration_tests.ShardedModel is not mirrored'),
            connections['other'].schema_editor() as schema_editor,
        ):
            CreateMissingMirroredTable('ShardedModel').database_forwards('migration_tests', schema_editor, state, state)

    def test_a_table_that_the_default_node_does_not_have_is_refused(self):
        """
        Case: Drop the table of MirroredModel on both nodes, and run CreateMissingMirroredTable for it on the other
              node.
        Expected: ValueError, because a model that was not mirrored has its table on the default node, so this model
                  was not one.
        """
        with connections['default'].cursor() as cursor:
            cursor.execute('DROP TABLE "{}"."{}" CASCADE'.format(PUBLIC_SCHEMA_NAME, MirroredModel._meta.db_table))
        self.drop_table_on_other_node(MirroredModel._meta.db_table)

        state = MigrationLoader(connections['default']).project_state()

        with (
            self.assertRaisesMessage(ValueError, 'the default node has no table for migration_tests.MirroredModel'),
            connections['other'].schema_editor() as schema_editor,
        ):
            CreateMissingMirroredTable('MirroredModel').database_forwards(
                'migration_tests', schema_editor, state, state
            )

    def test_a_node_that_has_the_table_is_left_as_it_is_while_the_default_node_has_none(self):
        """
        Case: Drop the table of MirroredModel on the default node, as in a new environment where the other node is
              migrated before the default node, and run CreateMissingMirroredTable for it on the other node, which has
              the table.
        Expected: No error and no statement, because the other node needs no new table.
        """
        with connections['default'].cursor() as cursor:
            cursor.execute('DROP TABLE "{}"."{}" CASCADE'.format(PUBLIC_SCHEMA_NAME, MirroredModel._meta.db_table))

        state = MigrationLoader(connections['default']).project_state()

        with connections['other'].schema_editor(collect_sql=True) as schema_editor:
            CreateMissingMirroredTable('MirroredModel').database_forwards(
                'migration_tests', schema_editor, state, state
            )

        self.assertEqual(schema_editor.collected_sql, [])

    def test_a_model_that_the_app_registry_does_not_have_is_skipped(self):
        """
        Case: Run CreateMissingMirroredTable on the other node for RenamedAwayModel, which the migration state has and
              the app registry does not, as for a model that a later migration renames or deletes.
        Expected: No table is created, and a warning is logged, as the router logs for an operation on such a model.
        """
        state = MigrationLoader(connections['default']).project_state()
        state.add_model(ModelState('migration_tests', 'RenamedAwayModel', [('id', models.AutoField(primary_key=True))]))

        with (
            self.assertLogs('djanquiltdb.operations', level='WARNING'),
            connections['other'].schema_editor(collect_sql=True) as schema_editor,
        ):
            CreateMissingMirroredTable('RenamedAwayModel').database_forwards(
                'migration_tests', schema_editor, state, state
            )

        self.assertEqual(schema_editor.collected_sql, [])

    def test_describe_and_migration_name_fragment(self):
        """
        Case: Describe CreateMissingMirroredTable and ask for the fragment that makemigrations names a migration after.
        Expected: Both name the model.
        """
        operation = CreateMissingMirroredTable('MirroredModel')

        self.assertEqual(operation.describe(), 'Create the table of MirroredModel on each node that does not have it')
        self.assertEqual(operation.migration_name_fragment, 'create_missing_table_mirroredmodel')

    def test_deconstruct(self):
        """
        Case: Deconstruct CreateMissingMirroredTable, as the migration writer does.
        Expected: The name of the class and the model name as the only argument.
        """
        self.assertEqual(
            CreateMissingMirroredTable('MirroredModel').deconstruct(),
            ('CreateMissingMirroredTable', ('MirroredModel',), {}),
        )
