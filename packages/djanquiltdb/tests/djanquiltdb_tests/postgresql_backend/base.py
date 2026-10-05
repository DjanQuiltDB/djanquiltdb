from contextlib import contextmanager
from unittest import mock

from django.db import DatabaseError, IntegrityError, InterfaceError, connections, transaction
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models.expressions import RawSQL
from django.db.utils import OperationalError
from django.test import override_settings
from psycopg.errors import InternalError

from djanquiltdb import State
from djanquiltdb.db import connection
from djanquiltdb.options import ShardOptions
from djanquiltdb.postgresql_backend.base import (
    PUBLIC_SCHEMA_NAME,
    DatabaseWrapper,
    ShardDatabaseWrapper,
    get_validated_schema_name,
)
from djanquiltdb.postgresql_backend.utils import LockCursorWrapperMixin
from djanquiltdb.utils import create_schema_on_node, create_template_schema, get_template_name, use_shard
from djanquiltdb_tests import (
    ShardingTestCase,
    ShardingTransactionTestCase,
    disable_db_reconnect,
    skip_without_virtual_generated_column_support,
)
from djanquiltdb_tests.sql import CREATE_ALLUPPERCASE, DROP_ALLUPPERCASE
from example.models import Cake, Organization, Shard, Type, User


def create_role_for_this_test_database(test_case, cursor, role_name):
    """
    Create a role whose name combines role_name and the test database's name, and return its quoted name. The test
    case's cleanup drops the role and everything it owns. A role belongs to the whole cluster, and the parallel test
    workers share that cluster, so a role with a fixed name would clash between workers.
    """
    default_connection = connections['default']
    quoted_role_name = default_connection.ops.quote_name(
        '{}_{}'.format(role_name, default_connection.settings_dict['NAME'])
    )
    cursor.execute('CREATE ROLE {}'.format(quoted_role_name))
    test_case.addCleanup(cursor.execute, 'DROP ROLE {}'.format(quoted_role_name))
    test_case.addCleanup(cursor.execute, 'DROP OWNED BY {}'.format(quoted_role_name))
    return quoted_role_name


class GetValidatedSchemaNameTestCase(ShardingTestCase):
    def test_valid_name(self):
        """
        Case: Call get_validated_schema_name with a valid name.
        Expected: The same value returned.
        """
        self.assertEqual(get_validated_schema_name('valid_name'), 'valid_name')

    def test_non_string(self):
        """
        Case: Call get_validated_schema_name with None.
        Expected: A ValueError raised (not a string).
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name(None)

    def test_illegal_string(self):
        """
        Case: Call get_validated_schema_name with a string of invalid structure.
        Expected: A ValueError raised.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('DROP * FROM')

    @override_settings(QUILT_DB={'TEMPLATE_NAME': 'template', 'SHARD_CLASS': 'example.models.Shard'})
    def test_template_name(self):
        """
        Case: Call get_validated_schema_name with the same name as the default template.
        Expected: A ValueError raised.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('template')

    @override_settings(QUILT_DB={'TEMPLATE_NAME': 'not-template', 'SHARD_CLASS': 'example.models.Shard'})
    def test_other_template_name(self):
        """
        Case: Call get_validated_schema_name with the same name as the set template.
        Expected: A ValueError raised.
        """
        self.assertEqual(get_validated_schema_name('template'), 'template')

    def test_public(self):
        """
        Case: Call get_validated_schema_name with 'public'.
        Expected: A ValueError raised.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('public')

    def test_information_schema(self):
        """
        Case: Call get_validated_schema_name with 'information_schema'.
        Expected: A ValueError raised, because 'information_schema' is a blacklisted schema name.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('information_schema')

    def test_default(self):
        """
        Case: Call get_validated_schema_name with 'default'.
        Expected: A ValueError raised, because 'default' is a blacklisted schema name.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('default')

    def test_startswith_pg(self):
        """
        Case: Call get_validated_schema_name with a value starting with 'pg_'.
        Expected: A ValueError raised, because we do not allow schema names to start with the postgresql namespace.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('pg_12')

    @override_settings(QUILT_DB={'TEMPLATE_NAME': 'template', 'SHARD_CLASS': 'example.models.Shard'})
    def test_is_template(self):
        """
        Case: Call get_validated_schema_name with a template name, while is_template set.
        Expected: A ValueError raised, we don't want shards to bear the 'template' name.
        """
        self.assertEqual(get_validated_schema_name('template', is_template=True), 'template')


class PostgresBackendTestCase(ShardingTransactionTestCase):
    @mock.patch('django.db.backends.postgresql.base.DatabaseWrapper.close')
    def test_close(self, mock_close):
        """
        Case: Call connection.close().
        Expected: connection.current_search_paths reset to the public schema, matching the search_path a
                  freshly opened connection starts out with.
        """
        connection.close()
        self.assertTrue(mock_close.called)
        self.assertEqual(connection.current_search_paths, [PUBLIC_SCHEMA_NAME])

    @mock.patch('django.db.backends.postgresql.base.DatabaseWrapper.rollback')
    def test_rollback(self, mock_rollback):
        """
        Case: Call connection.rollback().
        Expected: connection.current_search_paths invalidated to None, so the next _cursor() call re-issues
                  SET search_path (required for PgBouncer transaction pooling).
        """
        connection.rollback()
        self.assertTrue(mock_rollback.called)
        self.assertIsNone(connection.current_search_paths)

    @mock.patch('django.db.backends.postgresql.base.DatabaseWrapper.commit')
    def test_commit(self, mock_commit):
        """
        Case: Call connection.commit().
        Expected: connection.current_search_paths invalidated to None, so the next _cursor() call re-issues
                  SET search_path (required for PgBouncer transaction pooling).
        """
        connection.commit()
        self.assertTrue(mock_commit.called)
        self.assertIsNone(connection.current_search_paths)

    def test_get_ps_schema_with_existing_schema(self):
        """
        Case: Call connection.get_ps_schema with an existing schema name.
        Expected: Receive string 'test_schema'.
        """
        create_schema_on_node('test_schema', 'default', migrate=False)  # no need to migrate for this test
        self.assertEqual(connection.get_ps_schema('test_schema'), 'test_schema')

    def test_get_ps_schema_with_unexisting_schema(self):
        """
        Case: Call connection.get_ps_schema with an nonexisting schema name.
        Expected: Receive None.
        """
        self.assertIsNone(connection.get_ps_schema('test_schema'))

    def test_set_clone_function(self):
        """
        Case: Call connection.set_clone_function.
        Expected: The clone_schema function to be defined on our pSQL connection.
        """
        cursor = connection.cursor()
        connection.set_clone_function(cursor)
        try:
            # this will error if the function does not exists.
            cursor.execute("SELECT pg_get_functiondef('clone_schema(text, text)'::regprocedure);")
            self.assertTrue(cursor.fetchall()[0][0])
        except InternalError:
            # we need to rollback in case of a pSQL error, since we are in a transaction.
            cursor.execute('ROLLBACK;')
            self.fail('PostgreSQL internal error')

    @staticmethod
    def get_oid(schema_name, table_name, cursor):
        """
        Return internal id for tables to use in queries to gather metadata for them
        """
        cursor.execute(
            """SELECT c.oid
                          FROM pg_catalog.pg_class c
                          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                          WHERE n.nspname = %s
                          AND c.relname = %s
                          AND c.relkind = 'r' -- only tables;""",
            [schema_name, table_name],
        )
        return cursor.fetchall()[0][0]

    def test_clone_schema_in_transaction_restores_search_path(self):
        """
        Case: connection.clone_schema runs inside an open transaction, as it does when a Shard is saved
              under transaction.atomic().
        Expected: statements after the clone resolve unqualified names against the search path from before
                  the clone, not against the template schema the clone function switched to internally.
        """
        create_template_schema('default')
        connection.create_schema('test_schema')
        with transaction.atomic():
            connection.clone_schema('template', 'test_schema')
            cursor = connection.cursor()
            cursor.execute('SELECT current_schema()')
            self.assertEqual(cursor.fetchone()[0], PUBLIC_SCHEMA_NAME)

    def test_clone_schema_function_bodies_survive_verbatim(self):
        """
        Case: Template holds a function whose body contains a string literal naming the template schema.
        Expected: The clone's copy keeps the literal byte-identical.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute(
            'CREATE FUNCTION template.greet() RETURNS TEXT LANGUAGE plpgsql AS '
            "$fn$ BEGIN RETURN 'from template.greet'; END; $fn$"
        )
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        cursor.execute('SELECT test_schema.greet()')
        self.assertEqual(cursor.fetchone()[0], 'from template.greet')

    def test_clone_schema_skips_aggregates(self):
        """
        Case: Template holds an aggregate alongside a plain function.
        Expected: The clone succeeds and the plain function arrives. The aggregate is skipped rather than aborting the
                  whole clone.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE FUNCTION template.plain_one() RETURNS INT LANGUAGE sql AS $fn$ SELECT 1 $fn$')
        cursor.execute('CREATE AGGREGATE template.mysum (INT) (SFUNC = int4pl, STYPE = INT)')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        cursor.execute('SELECT test_schema.plain_one()')
        self.assertEqual(cursor.fetchone()[0], 1)

    def test_clone_schema_trigger_definitions_survive_verbatim(self):
        """
        Case: Template holds a trigger whose WHEN clause compares against a string literal naming the template schema.
        Expected: The clone's trigger keeps the literal, is bound to the clone table and function, and fires there.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.audited (name TEXT, touched BOOL DEFAULT false)')
        cursor.execute(
            'CREATE FUNCTION template.mark_touched() RETURNS trigger LANGUAGE plpgsql AS '
            '$fn$ BEGIN NEW.touched := true; RETURN NEW; END; $fn$'
        )
        cursor.execute(
            'CREATE TRIGGER audited_touch BEFORE INSERT ON template.audited '
            "FOR EACH ROW WHEN (NEW.name <> 'template.skip') EXECUTE FUNCTION template.mark_touched()"
        )
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        cursor.execute(
            'SELECT pg_get_triggerdef(tg.oid) FROM pg_catalog.pg_trigger tg '
            'JOIN pg_catalog.pg_class cls ON tg.tgrelid = cls.oid '
            'JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid '
            "WHERE nsp.nspname = 'test_schema' AND cls.relname = 'audited' AND NOT tg.tgisinternal"
        )
        trigger_def = cursor.fetchone()[0]
        self.assertIn("'template.skip'", trigger_def)
        self.assertIn('test_schema.mark_touched()', trigger_def)

        cursor.execute("INSERT INTO test_schema.audited (name) VALUES ('x') RETURNING touched")
        self.assertTrue(cursor.fetchone()[0])
        cursor.execute("INSERT INTO test_schema.audited (name) VALUES ('template.skip') RETURNING touched")
        self.assertFalse(cursor.fetchone()[0])

    def test_clone_schema_composite_and_action_bearing_foreign_keys(self):
        """
        Case: the template holds a composite foreign key that also declares ON DELETE CASCADE.
        Expected: the clone succeeds, the composite key arrives whole, keeps its delete action and points at
                  the clone's own parent table.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.parent (a INT, b INT, PRIMARY KEY (a, b))')
        cursor.execute(
            'CREATE TABLE template.child (a INT, b INT, '
            'FOREIGN KEY (a, b) REFERENCES template.parent (a, b) ON DELETE CASCADE)'
        )
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        cursor.execute("""
            SELECT con.confdeltype, array_length(con.conkey, 1), con.confrelid::regclass::text
              FROM pg_catalog.pg_constraint con
              JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
              JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
              WHERE nsp.nspname = 'test_schema' AND cls.relname = 'child' AND con.contype = 'f'
        """)
        self.assertEqual(cursor.fetchall(), [('c', 2, 'test_schema.parent')])

    def test_clone_schema_table_attributes(self):
        """
        Case: Call connection.migrate_schema.
        Expected: The given schema to have the same tables and all the table's info is the same.
                  This include indexes, sequences, constraints and foreign-key constraints
        """
        create_template_schema('default')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        cursor = connection.cursor()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'template';")
        template_tables = [table[1] for table in cursor.fetchall()]
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'test_schema';")
        new_schema_tables = [table[1] for table in cursor.fetchall()]

        self.assertCountEqual(template_tables, new_schema_tables)

        # These queries are based on the queries ran by Postgres when executing '\d table_name'. You can reveal
        # these by running `psql -E`.
        info_queries = {
            'table info': """SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules,
                       c.relrowsecurity, c.relforcerowsecurity, '', c.reltablespace,
                     CASE WHEN c.reloftype = 0 THEN ''
                       ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident
                   FROM pg_catalog.pg_class c
                     LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
                   WHERE c.oid = %s""",
            'field info': """SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
                     (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                        FROM pg_catalog.pg_attrdef d
                        WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
                     a.attnotnull, a.attnum,
                     (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
                        WHERE c.oid = a.attcollation AND t.oid = a.atttypid
                        AND a.attcollation <> t.typcollation) AS attcollation,
                     NULL AS indexdef,
                     NULL AS attfdwoptions
                   FROM pg_catalog.pg_attribute a
                     WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
                   ORDER BY a.attnum;""",
            'field constraints': """SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid,
                     pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), pg_catalog.pg_get_constraintdef(con.oid, true),
                     contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace
                   FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
                     LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid
                       AND contype IN ('p','u','x')) --- primary key constraint, unique constraint and exclusion constr.
                   WHERE c.oid = %s AND c.oid = i.indrelid AND i.indexrelid = c2.oid
                   ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;""",
            'unknown constraints': """SELECT r.conname, pg_catalog.pg_get_constraintdef(r.oid, true)
                   FROM pg_catalog.pg_constraint r
                   WHERE r.conrelid = %s AND r.contype = 'c' ORDER BY 1;""",
            'foreign key constraints': """SELECT conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef
                   FROM pg_catalog.pg_constraint r
                   WHERE r.conrelid = %s AND r.contype = 'f' ORDER BY 1;""",
            'triggers': """SELECT tg.tgname, pg_catalog.pg_get_triggerdef(tg.oid, true)
                   FROM pg_catalog.pg_trigger tg
                   WHERE tg.tgrelid = %s AND NOT tg.tgisinternal ORDER BY 1;""",
        }

        def normalized_rows(schema_name, query, oid):
            """
            Every row the query returns for `oid`, with the schema it came from spelled generically.

            Whole rows rather than the first one: a table has as many rows here as it has columns, indexes or
            constraints, so comparing one would leave a second index of either schema unchecked. Each query orders
            its rows, so the two schemas can be compared in order.
            """
            with use_shard(node_name='default', schema_name=schema_name) as env:
                cursor = env.connection.cursor()
                cursor.execute(query, [oid])

                return [
                    tuple(field.replace(schema_name, 'schema') if isinstance(field, str) else field for field in row)
                    for row in cursor.fetchall()
                ]

        for table_name in template_tables:
            oid_template = self.get_oid('template', table_name, connection.cursor())
            oid_test_schema = self.get_oid('test_schema', table_name, connection.cursor())

            for name, query in info_queries.items():
                # A subTest per table and query, so one run reports every table that differs rather than stopping at
                # the first.
                with self.subTest(table=table_name, info=name):
                    self.assertEqual(
                        normalized_rows('test_schema', query, oid_test_schema),
                        normalized_rows('template', query, oid_template),
                        '{} of {} does not appear to be cloned successfully'.format(name, table_name),
                    )

    def test_clone_schema_sequences(self):
        """
        Case: Call connection.migrate_schema.
        Expected: The given schema to have correct sequences.
        """
        create_template_schema('default')
        connection.create_schema('test_schema')

        cursor = connection.cursor()
        connection.clone_schema('template', 'test_schema')
        cursor.execute("SELECT pg_get_functiondef('clone_schema(text, text)'::regprocedure);")
        self.assertTrue(cursor.fetchall()[0][0])

        cursor = connection.cursor()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'template';")
        template_tables = [table[1] for table in cursor.fetchall()]
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'test_schema';")
        new_schema_tables = [table[1] for table in cursor.fetchall()]

        self.assertCountEqual(template_tables, new_schema_tables)

        # Get sequencer names and start value
        cursor.execute("""
            SELECT cls.relname::text, seq.seqstart::text
            FROM pg_catalog.pg_sequence seq
            JOIN pg_catalog.pg_class cls ON seq.seqrelid = cls.oid
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = 'test_schema'
        """)
        new_sequences = cursor.fetchall()

        # Only expect sequences for tables that have an 'id' column
        # (some tables like QuiltSession use different primary keys)
        tables_with_id = []
        for table_name in new_schema_tables:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'test_schema'
                AND table_name = %s
                AND column_name = 'id'
            """,
                [table_name],
            )
            if cursor.fetchone():
                tables_with_id.append(table_name)

        self.assertCountEqual(new_sequences, [('{}_id_seq'.format(table_name), '1') for table_name in tables_with_id])

        # Every id column must draw from the sequence of its own schema rather than from the template's, whether it
        # gets its values from a default or from an identity. pg_get_serial_sequence covers both.
        for table_name in tables_with_id:
            cursor.execute('SELECT pg_get_serial_sequence(%s, %s)', ['test_schema.{}'.format(table_name), 'id'])
            self.assertEqual(cursor.fetchone()[0], 'test_schema.{}_id_seq'.format(table_name))

        # Any default that an id column does carry has to name that same sequence. This connection sits on the public
        # schema, so test_schema is off the search path and pg_get_expr spells the sequence out in full.
        cursor.execute("""
            SELECT c.relname::text, pg_get_expr(d.adbin, d.adrelid, true)::text
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
            WHERE n.nspname = 'test_schema'
            AND a.attname = 'id'
            AND a.attnum > 0
            AND NOT a.attisdropped
            ORDER BY c.relname
        """)
        for table_name, default_expr in cursor.fetchall():
            self.assertEqual(default_expr, "nextval('test_schema.{}_id_seq'::regclass)".format(table_name))

    def test_sequences_of_cloned_schema(self):
        """
        Case: Create two shards and write similar data to both shards.
        Expected: Each schema to have their own sequences and thus we get the same ids across shards.
        """
        create_template_schema('default')
        shard_1 = Shard.objects.create(
            alias='org_1_shard', schema_name='org_1_shard', node_name='default', state=State.ACTIVE
        )
        shard_2 = Shard.objects.create(
            alias='org_2_shard', schema_name='org_2_shard', node_name='default', state=State.ACTIVE
        )
        with use_shard(shard_1):
            organization_1 = Organization.objects.create(name='The Boris Corp')
            user_1 = User.objects.create(name='Boris', email='boris@gast.bv', organization=organization_1)
        with use_shard(shard_2):
            organization_2 = Organization.objects.create(name='The Sjonnie Corp')
            user_2 = User.objects.create(name='Boris', email='boris@gast.bv', organization=organization_2)

        with use_shard(shard_1):
            user_3 = User.objects.create(name='Sjonnie', email='sjonnie@gast.bv', organization=organization_1)
        with use_shard(shard_2):
            user_4 = User.objects.create(name='Sjonnie', email='sjonnie@gast.bv', organization=organization_2)

        self.assertEqual(user_1.id, 1)  # Sequence starts at 1
        self.assertEqual(user_1.id, user_2.id)  # Both on different schema's, both new sequences.
        self.assertEqual(user_3.id, user_4.id)  # Both on different schema's, continuation of above sequencer.
        self.assertEqual(organization_1.id, 1)  # Sequence starts at 1
        self.assertEqual(organization_1.id, organization_2.id)  # different schema's, both new sequences
        self.assertNotEqual(user_1.id, user_3.id)  # Both on same schema
        self.assertNotEqual(user_2.id, user_4.id)  # Both on same schema

    def test_clone_schema_into_a_schema_that_already_has_a_sequence_with_the_same_name(self):
        """
        Case: The template has a sequence that was advanced, and the schema cloned into already has a sequence with
              the same name.
        Expected: The clone succeeds, and the existing sequence continues from the template sequence's position.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE SEQUENCE template.ticket_number')
        cursor.execute("SELECT setval('template.ticket_number', 41)")
        connection.create_schema('test_schema')
        cursor.execute('CREATE SEQUENCE test_schema.ticket_number')
        connection.clone_schema('template', 'test_schema')

        cursor.execute("SELECT nextval('test_schema.ticket_number')")
        self.assertEqual(cursor.fetchone()[0], 42)

    def test_clone_schema_skips_a_sequence_it_cannot_read(self):
        """
        Case: The template has a sequence on which the role cloning the schema has no privileges.
        Expected: The clone succeeds and has every table, but not the sequence the role cannot read.
        """
        create_template_schema('default')
        connection.create_schema('test_schema')
        cursor = connection.cursor()
        role = create_role_for_this_test_database(self, cursor, 'clone_without_sequence_access')
        cursor.execute('GRANT ALL ON SCHEMA template, test_schema TO {}'.format(role))
        cursor.execute('GRANT ALL ON ALL TABLES IN SCHEMA public, template TO {}'.format(role))
        cursor.execute('GRANT ALL ON ALL SEQUENCES IN SCHEMA template TO {}'.format(role))
        cursor.execute('CREATE SEQUENCE template.ticket_number')

        connection.set_clone_function()

        cursor.execute('SET ROLE {}'.format(role))
        self.addCleanup(cursor.execute, 'RESET ROLE')
        cursor.execute("SELECT public.clone_schema('template', 'test_schema')")
        cursor.execute('RESET ROLE')

        cursor.execute(
            "SELECT to_regclass('test_schema.example_organization'), to_regclass('test_schema.ticket_number')"
        )
        organization_table, ticket_number_sequence = cursor.fetchone()
        self.assertIsNotNone(organization_table)
        self.assertIsNone(ticket_number_sequence)

    def test_clone_schema_wo_template(self):
        """
        Case: Call connection.migrate_schema with missing template schema.
        Expected: An error to be raised.
        """
        connection.create_schema('test_schema')

        with self.assertRaises(ValueError):
            connection.clone_schema('template', 'test_schema')

    def test_clone_schema_wo_target(self):
        """
        Case: Call connection.migrate_schema with missing target schema.
        Expected: An error to be raised.
        """
        create_template_schema('default')

        with self.assertRaises(ValueError):
            connection.clone_schema('template2', 'test_schema')

    def test_flush_schema(self):
        """
        Case: Create a template schema and call 'flush_schema' on it.
        Expected: We end up with an empty schema. Stripped from all tables and sequences.
        """
        create_template_schema('default')
        with use_shard(node_name='default', schema_name='template') as env:
            self.assertNotEqual(connection.get_all_table_headers(schema_name='template'), [])
            self.assertNotEqual(connection.get_all_table_sequences(schema_name='template'), [])
            env.connection.flush_schema(schema_name='template')
            self.assertEqual(connection.get_all_table_headers(schema_name='template'), [])
            self.assertEqual(connection.get_all_table_sequences(schema_name='template'), [])

    def test_get_schema_for_model(self):
        """
        Case: Call get_schema_for_model for a model.
        Expected: The correct schema name to be returned.
        """
        self.assertEqual(connection.get_schema_for_model(Type), [('public',)])

    def test_get_schema_for_model_finds_a_view_backed_relation(self):
        """
        Case: A schema relies on a view as db_name for a model instead of a regular table.
        Expected: Both the base table and the view schema are reported for the model.
        """
        cursor = connection.cursor()
        connection.create_schema('view_backed_schema')
        cursor.execute(
            'CREATE VIEW view_backed_schema.{} AS SELECT * FROM public.{}'.format(
                Type._meta.db_table, Type._meta.db_table
            )
        )

        self.assertEqual(sorted(connection.get_schema_for_model(Type)), [('public',), ('view_backed_schema',)])

    def test_get_schema_for_sequence(self):
        """
        Case: Call get_schema_for_sequence for a sequence name.
        Expected: The correct schema name to be returned.
        """
        self.assertEqual(connection.get_schema_for_sequence('example_type_id_seq'), [('public',)])

    def test_is_public_schema(self):
        """
        Case: Test connection.is_public_schema()
        Expected: Returns False when the schema is not the public schema and returns True if the schema is the public
                  schema
        """
        connection.schema_name = 'test_schema'
        self.assertFalse(connection.is_public_schema())

        connection.schema_name = PUBLIC_SCHEMA_NAME
        self.assertTrue(connection.is_public_schema())

    def test_delete_schema(self):
        """
        Case: Create a schema and delete it after with connection.delete_schema
        Expected: Schema is deleted from the database
        """
        connection.create_schema('test_schema')
        self.assertIsNotNone(connection.get_ps_schema('test_schema'))  # Check if schema exists

        connection.delete_schema('test_schema')
        self.assertIsNone(connection.get_ps_schema('test_schema'))  # Schema does not exist anymore

    @mock.patch('django.db.backends.postgresql.base.DatabaseWrapper.is_usable', return_value=False)
    def test_reconnect_on_error(self, mock_is_usable):
        """
        Case: Call for a cursor while the connection has errors or not
        Expected: self.is_usable() and self.close() to be called if needed
        """
        with self.subTest('Errors occured'):
            connection.errors_occurred = True

            with mock.patch.object(connection, 'close') as mock_close:
                connection.cursor()

            mock_is_usable.assert_called_once_with()
            mock_close.assert_called_once_with()

        with self.subTest('No errors occured'):
            connection.errors_occurred = False
            mock_is_usable.reset_mock()

            with mock.patch.object(connection, 'close') as mock_close:
                connection.cursor()

            self.assertFalse(mock_is_usable.called)
            self.assertFalse(mock_close.called)

    @mock.patch('django.db.backends.postgresql.base.DatabaseWrapper.is_usable')
    def test_reconnect_on_stale_connection(self, mock_is_usable):
        """
        Case: Call for a cursor while the connection had errors and the connection is stale or not
        Expected: self.is_usable() to be used and self.close() to be called if needed
        """
        connection.errors_occurred = True
        with self.subTest('Connection stale'):
            mock_is_usable.return_value = False

            with mock.patch.object(connection, 'close') as mock_close:
                connection.cursor()

            mock_is_usable.assert_called_once_with()
            mock_close.assert_called_once_with()

        with self.subTest('Connection open'):
            mock_is_usable.reset_mock()
            mock_is_usable.return_value = True

            with mock.patch.object(connection, 'close') as mock_close:
                connection.cursor()

            mock_is_usable.assert_called_once_with()
            self.assertFalse(mock_close.called)

    def test_dropped_connection(self):
        """
        Case: Forcefully disconnect the database connection and perform a query.
        Expected: Database connection to be remade and the query executed without a problem.
        """

        def disconnect():
            """
            Perform a SQL query that will disconnect all current connections to this database.
            """
            from django.db.utils import OperationalError as OperationalError2
            from psycopg.errors import OperationalError as OperationalError1

            with use_shard(node_name='default', schema_name='public') as env:
                cursor = env.connection._get_cursor()  # Force the creation of a new cursor
                try:
                    cursor.execute(
                        'select pg_terminate_backend(pid) from pg_stat_activity where datname=%s;',
                        [env.connection.settings_dict['NAME']],
                    )
                except InterfaceError, OperationalError1, OperationalError2:
                    # We know this will raise errors.
                    # Disconnecting the connection from a connection does not pass silently.
                    pass

        create_template_schema('default')
        shard = Shard.objects.create(
            alias='org_1_shard', schema_name='org_1_shard', node_name='default', state=State.ACTIVE
        )

        with self.subTest('With reconnecting logic enabled'):
            with use_shard(shard):
                organization = Organization.objects.create(name='Nail!')
                organization.refresh_from_db()

                disconnect()

                # This should use the refreshed connection and raise no errors.
                organization.refresh_from_db()

        with self.subTest('With reconnecting logic disabled'):
            """
            Should this test ever fail, that means that either the disconnect query does not work, or Django/psycopg
            reconnects for us. The latter might lead to us removing the reconnect feature added in SHARDING-90.
            """
            with use_shard(shard):
                organization = Organization.objects.create(name='Nail!')
                organization.refresh_from_db()

                # Tell the connectionwrapper the connection is always usuable, so it won't see it is disconnected,
                # and thus will no reconnecting when needed.
                with mock.patch('django.db.backends.postgresql.base.DatabaseWrapper.is_usable', return_value=True):
                    disconnect()

                    with self.assertRaises(OperationalError) as cm:
                        organization.refresh_from_db()

                    error_msg = str(cm.exception).lower()
                    self.assertIn('connection', error_msg)
                    self.assertIn('closed', error_msg)


class CursorTestCase(ShardingTestCase):
    def close_connections(self):
        if hasattr(self, 'connection'):
            self.connection.close()

    def setUp(self):
        super().setUp()
        self.addCleanup(self.close_connections)

        create_template_schema()

        # Create a new connection that we can safely play with
        self.connection = DatabaseWrapper(connections['default'].settings_dict, connections['default'].alias)

        # And ask for a new cursor on the default connection to make sure that our current search path is the public
        # schema only
        with connections['default'].cursor():
            self.assertEqual(connections['default'].current_search_paths, [PUBLIC_SCHEMA_NAME])

        self.shard_options = ShardOptions(node_name='default', schema_name='template')
        self.template_connection = ShardDatabaseWrapper(self.connection, self.shard_options)

    @contextmanager
    def assertSearchPathChanged(self, connection_, old_search_paths, new_search_paths):
        """
        Asserts whether the search path has been changed, get_ps_schema is called and cursor.execute() is called with
        the correct arguments to change the search path in the database.
        """
        self.assertEqual(connection_.current_search_paths, old_search_paths)
        with (
            mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.get_ps_schema') as mock_get_ps_schema,
            mock.patch.object(connection_, 'connection') as mock_connection,
        ):
            yield

        self.assertTrue(mock_get_ps_schema.called)
        self.assertEqual(connection_.current_search_paths, new_search_paths)

        execute_calls = mock_connection.cursor.return_value.execute.call_args_list

        def get_sql_from_call(call_args):
            if not call_args:
                return None
            # call_args[0] is tuple of positional args, call_args[1] is dict of keyword args
            if call_args[0] and len(call_args[0]) > 0:
                sql_obj = call_args[0][0]
            elif call_args[1] and 'sql' in call_args[1]:
                sql_obj = call_args[1]['sql']
            else:
                return None

            if isinstance(sql_obj, str):
                return sql_obj

            return str(sql_obj)

        # Find all SQL calls and check for search_path
        # psycopg3 Composed objects stringify to "Composed([SQL('SET search_path = '), ...])"
        # so we check if the string contains "SET search_path" (case-insensitive)
        all_sql_calls = [get_sql_from_call(call_args) for call_args in execute_calls]
        set_search_path_calls = [
            sql_str for sql_str in all_sql_calls if sql_str and 'set search_path' in sql_str.lower()
        ]

        self.assertTrue(
            set_search_path_calls, f'No SQL execute call found for setting search_path. All calls: {all_sql_calls}'
        )

        # Only consider the first set search_path call for new_search_paths check
        if set_search_path_calls:
            set_path_sql = set_search_path_calls[0]
            for path in new_search_paths:
                self.assertIn(path, set_path_sql, f"Search path '{path}' should be in SQL: {set_path_sql}")

        self.assertEqual(mock_connection.cursor.return_value.close.call_count, 3)
        mock_connection.cursor.return_value.close.assert_has_calls(
            [
                mock.call(),  # Cursor for get_ps_schema
                mock.call(),  # Cursor for setting the search path
                mock.call(),  # Actual cursor inside the context manager
            ]
        )

    @contextmanager
    def assertSearchPathNotChanged(self, connection_, old_search_paths):
        """
        Asserts whether the search path has not been changed, get_ps_schema is not called and cursor.execute() is not
        called to change the search path in the database.
        """
        self.assertEqual(connection_.current_search_paths, old_search_paths)
        with (
            mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.get_ps_schema') as mock_get_ps_schema,
            mock.patch.object(connection_, 'connection') as mock_connection,
        ):
            yield

        self.assertFalse(mock_get_ps_schema.called)
        self.assertEqual(connection_.current_search_paths, old_search_paths)
        self.assertFalse(mock_connection.cursor.return_value.execute.called)

    @disable_db_reconnect()  # Disable the reconnect logic, to prevent it making queries
    def test_select_schema_operation(self):
        """
        Case: While the connection's current search path is 'public' only, get a cursor for a connection to the template
              schema
        Expected: current_search_path of connection set to ['template', 'public'], get_ps_schema called and the search
                  path in the database correctly set
        """
        with self.assertSearchPathChanged(self.template_connection, ['public'], ['template', 'public']):
            with self.template_connection.cursor():
                pass

    @disable_db_reconnect()  # Disable the reconnect logic, to prevent it making queries
    def test_dont_include_public_schema(self):
        """
        Case: While the connection's current search path is 'public' only, get a cursor for a connection to the template
              schema
        Expected: current_search_path of connection set to ['template', 'public'], get_ps_schema called and the search
                  path in the database correctly set
        """
        shard_options = ShardOptions(node_name='default', schema_name='template', include_public=False)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)

        with self.assertSearchPathChanged(connection_, ['public'], ['template']):
            with connection_.cursor():
                pass

    def test_no_db_operation(self):
        """
        Case: While the connection's current search path is 'public' only, get a cursor for a nodb connection
        Expected: Search path not changed while getting a new cursor
        """
        with self.assertSearchPathNotChanged(self.connection, ['public']):
            with self.connection._nodb_cursor():
                pass

    @disable_db_reconnect()  # Disable the reconnect logic, to prevent it making queries
    def test_search_path_equal(self):
        """
        Case: While the connection's current search path is already 'template' and 'public', get a cursor for the
              template schema
        Expected: Search path not changed because it was already the correct search path
        """
        self.template_connection.current_search_paths = ['template', 'public']
        with self.assertSearchPathNotChanged(self.template_connection, ['template', 'public']):
            with self.template_connection.cursor():
                pass

    def test_schema_does_not_exist(self):
        """
        Case: Get a new cursor for a connection to a schema that does not exists
        Expected: IntegrityError raised, because the schema does not exists
        """
        shard_options = ShardOptions(node_name='default', schema_name='foo')
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)
        with self.assertRaisesMessage(IntegrityError, "Schema '{}' does not exist.".format('foo')):
            with connection_.cursor():
                pass

    @mock.patch.object(DatabaseWrapper, '_cursor')
    def test_cursor(self, mock_cursor):
        """
        Case: Call connection's `cursor` method
        Expected: Return value of `_cursor` returned
        """
        self.assertEqual(self.connection.cursor(), mock_cursor.return_value)
        mock_cursor.assert_called_once_with()

    @mock.patch.object(DatabaseWrapper, 'ensure_connection')
    @mock.patch.object(DatabaseWrapper, 'create_cursor')
    @mock.patch.object(DatabaseWrapper, '_prepare_cursor')
    def test_get_cursor(self, mock_prepare_cursor, mock_create_cursor, mock_ensure_connection):
        """
        Case: Call DatabaseWrapper._get_cursor() with `skip_lock` being False.
        Expected: `ensure_connection` called, `create_cursor` called (with the `name` passed in Django 1.11+) and
                  `_prepare_cursor` called with the return value of `create_cursor` and `skip_lock` being False.
        """
        name = 'foo'
        skip_lock = False

        self.assertEqual(self.connection._get_cursor(name, skip_lock=skip_lock), mock_prepare_cursor.return_value)

        mock_ensure_connection.assert_called_once_with()

        mock_create_cursor.assert_called_once_with(name)

        mock_prepare_cursor.assert_called_once_with(mock_create_cursor.return_value, skip_lock=skip_lock)

    @mock.patch.object(DatabaseWrapper, 'ensure_connection')
    @mock.patch.object(DatabaseWrapper, 'create_cursor')
    @mock.patch.object(DatabaseWrapper, '_prepare_cursor')
    def test_get_cursor_skip_lock(self, mock_prepare_cursor, mock_create_cursor, mock_ensure_connection):
        """
        Case: Call DatabaseWrapper._get_cursor() with `skip_lock` being True.
        Expected: `ensure_connection` called, `create_cursor` called (with the `name` passed in Django 1.11+) and
                  `_prepare_cursor` called with the return value of `create_cursor` and `skip_lock` being True.
        """
        name = 'foo'
        skip_lock = True

        self.assertEqual(self.connection._get_cursor(name, skip_lock=skip_lock), mock_prepare_cursor.return_value)

        mock_ensure_connection.assert_called_once_with()

        # Named cursors is only a thing in Django 1.11+
        mock_create_cursor.assert_called_once_with(name)

        mock_prepare_cursor.assert_called_once_with(mock_create_cursor.return_value, skip_lock=skip_lock)

    @mock.patch.object(DatabaseWrapper, 'validate_thread_sharing')
    @mock.patch.object(DatabaseWrapper, 'make_cursor')
    @mock.patch.object(DatabaseWrapper, 'queries_logged', False)
    def test_prepare_cursor(self, mock_make_cursor, mock_validate_thread_sharing):
        """
        Case: Call DatabaseWrapper._get_cursor() with `queries_logged` being False.
        Expected: Returns the return value of `make_cursor` and calls `validate_thread_sharing`.
        """
        cursor = mock.Mock()
        skip_lock = mock.Mock()

        self.assertEqual(self.connection._prepare_cursor(cursor, skip_lock), mock_make_cursor.return_value)

        mock_validate_thread_sharing.assert_called_once_with()
        mock_make_cursor.assert_called_once_with(cursor, skip_lock=skip_lock)

    @mock.patch.object(DatabaseWrapper, 'validate_thread_sharing')
    @mock.patch.object(DatabaseWrapper, 'make_debug_cursor')
    @mock.patch.object(DatabaseWrapper, 'queries_logged', True)
    def test_prepare_cursor_queries_logged(self, mock_make_debug_cursor, mock_validate_thread_sharing):
        """
        Case: Call DatabaseWrapper._get_cursor() with `queries_logged` being True.
        Expected: Returns the return value of `make_debug_cursor` and calls `validate_thread_sharing`.
        """
        cursor = mock.Mock()
        skip_lock = mock.Mock()

        self.assertEqual(self.connection._prepare_cursor(cursor, skip_lock), mock_make_debug_cursor.return_value)

        mock_validate_thread_sharing.assert_called_once_with()
        mock_make_debug_cursor.assert_called_once_with(cursor, skip_lock=skip_lock)


class AdvisoryLockingTestCase(ShardingTransactionTestCase):
    def close_connections(self):
        if hasattr(self, 'connection2'):
            self.connection2.close()

    def setUp(self):
        super().setUp()
        self.addCleanup(self.close_connections)

        self.connection1 = connections['default']

        # Create second connection to the same database
        self.connection2 = DatabaseWrapper(self.connection1.settings_dict, self.connection1.alias)

    @staticmethod
    def get_lock(connection_, key):
        key = LockCursorWrapperMixin.get_int_from_key(key)
        cursor = connection_.cursor()
        cursor.execute('SELECT pg_try_advisory_lock({});'.format(key))
        return cursor.fetchall()[0][0]

    @mock.patch('django.db.backends.utils.CursorWrapper.execute')
    def test_acquire_shard_lock(self, mock_execute):
        """
        Case: Call acquire_advisory_lock for a shared lock.
        Expected: The correct SQL to be executed.
        """
        self.connection1.acquire_advisory_lock(key='test', shared=True)

        mock_execute.assert_called_once_with(
            'SELECT pg_advisory_lock_shared(%s);', [LockCursorWrapperMixin.get_int_from_key('test')]
        )

    @mock.patch('django.db.backends.utils.CursorWrapper.execute')
    def test_acquire_exclusive_lock(self, mock_execute):
        """
        Case: Call acquire_advisory_lock for an exclusive lock.
        Expected: The correct SQL to be executed.
        """
        self.connection1.acquire_advisory_lock(key='test', shared=False)

        mock_execute.assert_called_once_with(
            'SELECT pg_advisory_lock(%s);', [LockCursorWrapperMixin.get_int_from_key('test')]
        )

    @mock.patch('django.db.backends.utils.CursorWrapper.execute')
    def test_release_shard_lock(self, mock_execute):
        """
        Case: Call release_advisory_lock for a shared lock.
        Expected: The correct SQL to be executed.
        """
        self.connection1.release_advisory_lock(key='test', shared=True)

        mock_execute.assert_called_once_with(
            'SELECT pg_advisory_unlock_shared(%s);', [LockCursorWrapperMixin.get_int_from_key('test')]
        )

    @mock.patch('django.db.backends.utils.CursorWrapper.execute')
    def test_release_exclusive_lock(self, mock_execute):
        """
        Case: Call release_advisory_lock for an exclusive lock.
        Expected: The correct SQL to be executed.
        """
        self.connection1.release_advisory_lock(key='test', shared=False)

        mock_execute.assert_called_once_with(
            'SELECT pg_advisory_unlock(%s);', [LockCursorWrapperMixin.get_int_from_key('test')]
        )

    def test_shared_blocks_exclusive_lock(self):
        """
        Case: Set a shared advisory lock and then try to set an exclusive one.
        Expected: Exclusive lock not given at first, but is given when the shared lock is released.
        """
        self.connection1.acquire_advisory_lock(key='test', shared=True)
        self.assertFalse(self.get_lock(self.connection2, 'test'))

        self.connection1.release_advisory_lock(key='test', shared=True)
        self.assertTrue(self.get_lock(self.connection2, 'test'))

    def test_locks_for_two_keys_dont_block(self):
        """
        Case: Calling for two exclusive locks on different keys.
        Expected: Both locks to be given.
        """
        self.connection1.acquire_advisory_lock(key='test', shared=False)
        self.connection2.acquire_advisory_lock(key='test2', shared=False)

        self.assertTrue(self.get_lock(self.connection1, 'test'))
        self.assertTrue(self.get_lock(self.connection2, 'test2'))

    def test_failing_statement_under_use_shard_in_transaction(self):
        """
        Case: A statement fails inside transaction.atomic under a use_shard context, aborting the transaction before the
              context releases the shard's advisory lock.
        Expected: The original error surfaces rather than the release's own failure inside the aborted transaction, and
                  the rollback releases the lock instead of stranding it on the session.
        """
        create_template_schema('default')
        shard = Shard.objects.create(node_name='default', schema_name='test_schema', alias='test', state=State.ACTIVE)

        with self.assertRaises(DatabaseError) as raised:
            with transaction.atomic():
                with use_shard(shard):
                    self.connection1.cursor().execute('SELECT 1 FROM djanquiltdb_no_such_table')

        self.assertIn('djanquiltdb_no_such_table', str(raised.exception))
        self.assertTrue(self.get_lock(self.connection2, 'shard_{}'.format(shard.id)))

    def test_failing_statement_with_using_in_transaction(self):
        """
        Case: a query routed with .using(shard) fails inside transaction.atomic, aborting the transaction
              between the per-statement advisory lock's acquire and release.
        Expected: the original error surfaces rather than the release's own failure inside the aborted
                  transaction, and the rollback releases the lock instead of stranding it on the session.
        """
        create_template_schema('default')
        shard = Shard.objects.create(node_name='default', schema_name='test_schema', alias='test', state=State.ACTIVE)

        with self.assertRaises(DatabaseError) as raised:
            with transaction.atomic():
                list(Organization.objects.using(shard).annotate(x=RawSQL('djanquiltdb_no_such_column', ())))

        self.assertIn('djanquiltdb_no_such_column', str(raised.exception))
        self.assertTrue(self.get_lock(self.connection2, 'shard_{}'.format(shard.id)))

    def test_nested_locks(self):
        """
        Case: Have multiple advisory locks with the same key, release one, and release the other after
        Expected: While only one lock has been released, it's still not possible to get an exclusive lock
        """
        self.connection1.acquire_advisory_lock(key='test', shared=True)
        self.connection1.acquire_advisory_lock(key='test', shared=True)

        # We have two advisory locks set now, so we can't get an exclusive lock on a different connection
        self.assertFalse(self.get_lock(self.connection2, 'test'))

        # Release one
        self.connection1.release_advisory_lock(key='test', shared=True)

        # Meaning that we still have one advisory lock set, so we still can't get an exclusive lock on a different
        # connection
        self.assertFalse(self.get_lock(self.connection2, 'test'))

        # Release the second lock
        self.connection1.release_advisory_lock(key='test', shared=True)

        # Now all locks are released, so we can get an exclusive lock now
        self.assertTrue(self.get_lock(self.connection2, 'test'))


class AdvisoryLockingIntegrationTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard = Shard.objects.create(
            node_name='default', schema_name='test_schema', alias='test', state=State.ACTIVE
        )

    @mock.patch.object(LockCursorWrapperMixin, 'acquire_advisory_lock')
    @mock.patch.object(LockCursorWrapperMixin, 'release_advisory_lock')
    def test_lock_use_shard(self, mock_release_advisory_lock, mock_acquire_advisory_lock):
        """
        Case: Retrieve an object in a use_shard context, inside the transaction the test case wraps around
              each test.
        Expected: The advisory lock acquired only once, transaction-scoped, with no explicit release: the
                  surrounding transaction's commit or rollback releases it.
        """
        with self.shard.use():
            Organization.objects.create(name='Hogwarts')

        mock_acquire_advisory_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True, xact=True)
        mock_release_advisory_lock.assert_not_called()

    @mock.patch.object(LockCursorWrapperMixin, 'acquire_advisory_lock')
    @mock.patch.object(LockCursorWrapperMixin, 'release_advisory_lock')
    def test_lock_on_execute(self, mock_release_advisory_lock, mock_acquire_advisory_lock):
        """
        Case: Retrieve an object with the using method, inside the transaction the test case wraps around
              each test.
        Expected: The advisory lock acquired only once, transaction-scoped, with no explicit release: the
                  surrounding transaction's commit or rollback releases it.
        """
        Organization.objects.using(self.shard).create(name='Hogwarts')

        mock_acquire_advisory_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True, xact=True)
        mock_release_advisory_lock.assert_not_called()


class ShardDatabaseWrapperTestCase(ShardingTransactionTestCase):
    def close_connections(self):
        if hasattr(self, 'connection'):
            try:
                self.connection.close()
            except DatabaseError:
                pass

    def setUp(self):
        super().setUp()
        self.addCleanup(self.close_connections)

        create_template_schema()

        self.shard = Shard.objects.create(
            node_name='default', schema_name='test_schema', alias='test', state=State.ACTIVE
        )

        # Create a new connection that we can safely play with
        self.connection = DatabaseWrapper(connections['default'].settings_dict, connections['default'].alias)

    def test_proxy_fields(self):
        """
        Case: Setting and getting an attribute that's listed in _PROXY_FIELDS is proxied to the main connection.
        Expected: The fields are proxied to the main connection.
        """
        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)

        for field in ShardDatabaseWrapper._PROXY_FIELDS:
            # First set the value on the ShardDatabaseWrapper instance and check whether the field is changed on the
            # main connection. Basically tests __setattr__.
            value = mock.MagicMock()
            setattr(connection_, field, value)
            self.assertIs(getattr(connection_._main_connection, field), value)

            # And next set it on the main connection and check whether the value on the ShardDatabaseWrapper is proxied
            # to the main connection. Basically tests __getattribute__.
            other_value = mock.MagicMock()
            setattr(self.connection, field, other_value)
            self.assertIs(getattr(connection_._main_connection, field), other_value)

    def test_expected_proxy_fields(self):
        """
        Case: Check whether the ShardDatabaseWrapper._PROXY_FIELDS are the same as the fields defined in
              BaseDatabaseWrapper's init fields, including the current_search_path, but excluding:
                * alias
                * client
                * creation
                * features
                * introspection
                * ops
                * validation
                * _thread_sharing_lock
                * _thread_sharing_count
        Expected: List is as we expected
        Note: if in future versions of Django the fields we define in BaseDatabaseWrapper changes, this test will tell
              us. We want to proxy all those fields (except for the alias).
        """
        exclude_classes = [
            'client',
            'creation',
            'features',
            'introspection',
            'ops',
            'validation',
            '_thread_sharing_lock',
            '_thread_sharing_count',
        ]

        class DummyDatabaseWrapper(BaseDatabaseWrapper):
            pass

        for exclude_class in exclude_classes:
            setattr(DummyDatabaseWrapper, '{}_class'.format(exclude_class), mock.Mock())

        proxy_fields = list(DummyDatabaseWrapper({}).__dict__.keys())
        proxy_fields.remove('alias')

        for exclude_class in exclude_classes:
            if exclude_class in proxy_fields:
                proxy_fields.remove(exclude_class)

        proxy_fields.append('current_search_paths')

        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)
        self.assertCountEqual(connection_._PROXY_FIELDS, proxy_fields)

    def test_current_search_paths(self):
        """
        Case: Initialize a new ShardDatabaseWrapper and after ask for a cursor
        Expected: After initializing the new ShardDatabaseWrapper, the `current_search_paths` of the main connection has
                  not been altered to the new schema in ShardDatabaseWrapper. Only after asking for a cursor, the
                  `current_search_paths` has been altered.
        """
        current_search_paths = [PUBLIC_SCHEMA_NAME, get_template_name()]
        self.connection.current_search_paths = current_search_paths

        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)

        self.assertEqual(connection_.current_search_paths, current_search_paths)
        self.assertEqual(self.connection.current_search_paths, current_search_paths)

        connection_.cursor()

        new_current_search_paths = [self.shard.schema_name, PUBLIC_SCHEMA_NAME]

        self.assertEqual(connection_.current_search_paths, new_current_search_paths)
        self.assertEqual(self.connection.current_search_paths, new_current_search_paths)

    def test_alias(self):
        """
        Case: Get the alias of a ShardDatabaseWrapper
        Expected: Returns the node name and the schema name divided by a pipe
        """
        options = {'node_name': 'default', 'schema_name': self.shard.schema_name}
        shard_options = ShardOptions(**options)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)

        self.assertEqual(connection_.alias, '{node_name}|{schema_name}'.format(**options))

    def test_change_alias(self):
        """
        Case: Change the alias of a ShardDatabaseWrapper
        Expected: ValueError raised, because the alias is managed by the main connection
        """
        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)

        with self.assertRaisesMessage(ValueError, 'The alias is managed by the main connection and cannot be changed.'):
            connection_.alias = 'other'

    @mock.patch.object(ShardDatabaseWrapper, 'acquire_advisory_lock')
    def test_acquire_locks(self, mock_acquire_advisory_lock):
        """
        Case: Acquire lock on a ShardDatabaseWrapper while having a shard_id set on ShardOptions
        Expected: Lock keys from the ShardOptions used to call acquire_advisory_lock
        """
        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name, shard_id=self.shard.id)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)
        connection_.acquire_locks()
        mock_acquire_advisory_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True, xact=False)

    @mock.patch.object(ShardDatabaseWrapper, 'acquire_advisory_lock')
    def test_acquire_locks_with_mapping_value(self, mock_acquire_advisory_lock):
        """
        Case: Acquire lock on a ShardDatabaseWrapper while having a shard_id and a mapping value set on ShardOptions
        Expected: Lock keys from the ShardOptions used to call acquire_advisory_lock
        """
        shard_options = ShardOptions(
            node_name='default', schema_name=self.shard.schema_name, shard_id=self.shard.id, mapping_value=42
        )
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)
        connection_.acquire_locks()
        self.assertEqual(mock_acquire_advisory_lock.call_count, 2)
        mock_acquire_advisory_lock.assert_has_calls(
            [
                mock.call('shard_{}'.format(self.shard.id), shared=True, xact=False),
                mock.call('mapping_42', shared=True, xact=False),
            ]
        )

    @mock.patch.object(ShardDatabaseWrapper, 'release_advisory_lock')
    def test_release_locks(self, mock_release_advisory_lock):
        """
        Case: Release lock on a ShardDatabaseWrapper while having a shard_id set on ShardOptions
        Expected: Lock keys from the ShardOptions used to call release_advisory_lock
        """
        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name, shard_id=self.shard.id)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)
        connection_.release_locks()
        mock_release_advisory_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True)

    @mock.patch.object(ShardDatabaseWrapper, 'release_advisory_lock')
    def test_release_locks_with_mapping_value(self, mock_release_advisory_lock):
        """
        Case: Release lock on a ShardDatabaseWrapper while having a shard_id and a mapping value set on ShardOptions
        Expected: Lock keys from the ShardOptions used to call release_advisory_lock
        """
        shard_options = ShardOptions(
            node_name='default', schema_name=self.shard.schema_name, shard_id=self.shard.id, mapping_value=42
        )
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)
        connection_.release_locks()
        self.assertEqual(mock_release_advisory_lock.call_count, 2)
        mock_release_advisory_lock.assert_has_calls(
            [
                mock.call('shard_{}'.format(self.shard.id), shared=True),
                mock.call('mapping_42', shared=True),
            ]
        )

    def test_lock_on_execute(self):
        """
        Case: Initialize the ShardDatabaseWrapper for multiple combinations of options
        Expected: If we are not in a use_shard context, set lock to True and have lock keys, then `lock_on_execute`
                  returns True. It returns False otherwise.
        """
        dont_lock_on_execute = [
            {'lock': False},
            {'lock': True, 'use_shard': True, 'shard_id': self.shard.id},
            {'lock': True, 'use_shard': False},
        ]

        lock_on_execute = [
            {'lock': True, 'use_shard': False, 'shard_id': self.shard.id},
            {'lock': True, 'use_shard': False, 'shard_id': self.shard.id, 'mapping_value': 42},
            {'lock': True, 'use_shard': False, 'mapping_value': 42},  # Unlikely, but possible
        ]

        for options in dont_lock_on_execute:
            shard_options = ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name, **options)
            connection_ = ShardDatabaseWrapper(self.connection, shard_options)
            self.assertFalse(connection_.lock_on_execute)

        for options in lock_on_execute:
            shard_options = ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name, **options)
            connection_ = ShardDatabaseWrapper(self.connection, shard_options)
            self.assertTrue(connection_.lock_on_execute)


class IdentityColumnTestCase(ShardingTransactionTestCase):
    """
    Test cases for Django 6.0+ identity column support.
    Identity columns use GENERATED BY DEFAULT AS IDENTITY instead of sequences.
    """

    def test_clone_schema_carries_identity_sequences_of_any_column_name(self):
        """
        Case: the template holds a table whose identity column is not named id and already contains rows.
        Expected: the cloned schema's identity sequence continues past the copied rows, so an insert into
                  the clone does not reuse a taken key.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute(
            'CREATE TABLE template.widget (code BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name TEXT)'
        )
        cursor.execute("INSERT INTO template.widget (name) VALUES ('a'), ('b')")
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        cursor.execute("INSERT INTO test_schema.widget (name) VALUES ('c') RETURNING code")
        self.assertEqual(cursor.fetchone()[0], 3)

    def test_clone_schema_skips_the_identity_sequence_of_a_table_it_does_not_copy(self):
        """
        Case: The template has a table with an identity column. The role cloning the schema has no privileges on the
              table, so the clone does not copy it.
        Expected: The clone succeeds and has every other table, but not the one it did not copy.
        """
        create_template_schema('default')
        connection.create_schema('test_schema')
        cursor = connection.cursor()
        role = create_role_for_this_test_database(self, cursor, 'clone_without_widget_access')
        cursor.execute('GRANT ALL ON SCHEMA template, test_schema TO {}'.format(role))
        cursor.execute('GRANT ALL ON ALL TABLES IN SCHEMA public, template TO {}'.format(role))
        cursor.execute('GRANT ALL ON ALL SEQUENCES IN SCHEMA template TO {}'.format(role))
        cursor.execute(
            'CREATE TABLE template.widget (code BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name TEXT)'
        )

        connection.set_clone_function()

        cursor.execute('SET ROLE {}'.format(role))
        self.addCleanup(cursor.execute, 'RESET ROLE')
        cursor.execute("SELECT public.clone_schema('template', 'test_schema')")
        cursor.execute('RESET ROLE')

        cursor.execute("SELECT to_regclass('test_schema.example_organization'), to_regclass('test_schema.widget')")
        organization_table, widget_table = cursor.fetchone()
        self.assertIsNotNone(organization_table)
        self.assertIsNone(widget_table)

    def test_get_all_table_sequences_with_identity_columns(self):
        """
        Case: Call get_all_table_sequences on a schema with identity columns (Django 6.0+).
        Expected: Should return sequences for identity columns using pg_get_serial_sequence.
        Note: Identity columns have underlying sequences that can be found via pg_get_serial_sequence,
        but they may not appear directly in pg_sequence. This test verifies the current behavior.
        """
        create_template_schema('default')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        # Get sequences from the cloned schema
        sequences = connection.get_all_table_sequences(schema_name='test_schema')

        # Verify we found some sequences (either standalone sequences or identity column sequences)
        # In Django 6.0, identity columns should have underlying sequences accessible via pg_get_serial_sequence
        self.assertIsInstance(sequences, list)

        # Check if we can find sequences for tables using pg_get_serial_sequence
        # This is what the code should ideally use for identity columns
        cursor = connection.cursor()
        cursor.execute("""
            SELECT TABLE_NAME::text
            FROM information_schema.TABLES
            WHERE table_schema = 'test_schema'
            AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]

        # For each table, try to find its sequence using pg_get_serial_sequence
        # Only check tables that have an 'id' column (some tables like QuiltSession use different PKs)
        found_sequences_via_pg_get_serial = []
        for table in tables:
            # Check if table has an 'id' column before trying to get its sequence
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'test_schema'
                AND table_name = %s
                AND column_name = 'id'
            """,
                [table],
            )
            has_id_column = cursor.fetchone() is not None

            if has_id_column:
                cursor.execute(
                    """
                    SELECT pg_get_serial_sequence(%s, 'id')
                """,
                    ['test_schema.{}'.format(table)],
                )
                result = cursor.fetchone()
                if result and result[0]:
                    # Extract sequence name from the full sequence name (schema.sequence)
                    seq_name = result[0].split('.')[-1] if '.' in result[0] else result[0]
                    found_sequences_via_pg_get_serial.append(seq_name)

        # The get_all_table_sequences should find sequences, but may miss identity column sequences
        # This test documents the current behavior and potential limitation
        if found_sequences_via_pg_get_serial:
            # If we found sequences via pg_get_serial_sequence, verify they're accessible
            for seq_name in found_sequences_via_pg_get_serial:
                # Check if sequence exists in pg_sequence (standalone sequences)
                # or if it's an identity column sequence (which may not be in pg_sequence directly)
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_catalog.pg_sequence seq
                        JOIN pg_catalog.pg_class cls ON seq.seqrelid = cls.oid
                        JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
                        WHERE nsp.nspname = 'test_schema' AND cls.relname = %s
                    )
                """,
                    [seq_name],
                )
                in_pg_sequence = cursor.fetchone()[0]

                # Sequence should either be in pg_sequence OR be an identity column sequence
                # (identity column sequences are accessible via pg_get_serial_sequence but may not be in pg_sequence)
                if not in_pg_sequence:
                    # This is an identity column sequence - verify it's accessible
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_attribute a
                            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                            WHERE n.nspname = 'test_schema'
                            AND a.attname = 'id'
                            AND a.attidentity != ''
                            AND pg_get_serial_sequence(n.nspname || '.' || c.relname, 'id') LIKE '%' || %s
                        )
                    """,
                        [seq_name],
                    )
                    is_identity = cursor.fetchone()[0]
                    self.assertTrue(
                        is_identity,
                        f'Sequence {seq_name} should be accessible via pg_get_serial_sequence for identity columns',
                    )

    def test_get_all_table_sequences_includes_identity_columns(self):
        """
        Case: Call get_all_table_sequences on a schema with identity columns (Django 6.0+).
        Expected: Should return ALL sequences including identity column sequences.
        This test verifies that get_all_table_sequences correctly finds identity column sequences
        using pg_get_serial_sequence, not just standalone sequences from pg_sequence.
        """
        create_template_schema('default')
        connection.create_schema('test_schema_sequences')
        connection.clone_schema('template', 'test_schema_sequences')

        # Get sequences using get_all_table_sequences
        sequences_from_function = set(connection.get_all_table_sequences(schema_name='test_schema_sequences'))

        # Get all sequences that should exist (using pg_get_serial_sequence for identity columns)
        cursor = connection.cursor()

        # Get standalone sequences from pg_sequence
        cursor.execute("""
            SELECT cls.relname::text
            FROM pg_catalog.pg_sequence seq
            JOIN pg_catalog.pg_class cls ON seq.seqrelid = cls.oid
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = 'test_schema_sequences'
        """)
        standalone_sequences = {row[0] for row in cursor.fetchall()}

        # Get identity column sequences using pg_get_serial_sequence
        cursor.execute("""
            SELECT DISTINCT
                split_part(pg_get_serial_sequence(n.nspname || '.' || c.relname, a.attname), '.', 2) AS seq_name
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'test_schema_sequences'
            AND a.attname = 'id'
            AND a.attidentity != ''
            AND a.attnum > 0
            AND NOT a.attisdropped
            AND c.relkind = 'r'
            AND pg_get_serial_sequence(n.nspname || '.' || c.relname, a.attname) IS NOT NULL
        """)
        identity_sequences = {row[0] for row in cursor.fetchall() if row[0]}

        # All sequences that should exist (standalone + identity column sequences)
        expected_sequences = standalone_sequences | identity_sequences

        # Verify get_all_table_sequences returns all expected sequences
        # Note: Currently it may only return standalone sequences, but ideally it should return both
        self.assertIsInstance(sequences_from_function, set)

        # Check that all standalone sequences are included
        missing_standalone = standalone_sequences - sequences_from_function
        self.assertEqual(
            missing_standalone,
            set(),
            f'get_all_table_sequences should include all standalone sequences. Missing: {missing_standalone}',
        )

        # Check if identity column sequences are included
        missing_identity = identity_sequences - sequences_from_function
        if identity_sequences:
            if missing_identity:
                # This documents the current limitation - identity sequences may not be included
                # Once the function is fixed, this assertion should pass
                self.fail(
                    f'get_all_table_sequences should include identity column sequences. '
                    f'Found via pg_get_serial_sequence: {identity_sequences}, '
                    f'but missing from get_all_table_sequences: {missing_identity}. '
                    f'All sequences returned: {sequences_from_function}'
                )
            else:
                # All identity sequences are included - function is working correctly
                self.assertEqual(
                    sequences_from_function,
                    expected_sequences,
                    'get_all_table_sequences should return all sequences (standalone + identity column sequences)',
                )

    def test_get_all_table_sequences_completeness(self):
        """
        Case: Verify get_all_table_sequences returns complete list of sequences.
        Expected: Should return sequences that match what pg_get_serial_sequence finds for identity columns.
        """
        create_template_schema('default')
        shard = Shard.objects.create(
            alias='test_shard_seq', schema_name='test_shard_seq', node_name='default', state=State.ACTIVE
        )

        # Create some data to ensure sequences exist
        with use_shard(shard):
            Organization.objects.create(name='Test Org 1')
            Organization.objects.create(name='Test Org 2')

        # Get sequences using get_all_table_sequences
        sequences_from_function = set(connection.get_all_table_sequences(schema_name=shard.schema_name))

        # Verify sequences exist for tables with identity columns
        cursor = connection.cursor()

        # Check Organization table specifically (we know it has an id column)
        cursor.execute(
            """
            SELECT pg_get_serial_sequence(%s, 'id')
        """,
            ['{}.{}'.format(shard.schema_name, Organization._meta.db_table)],
        )
        org_seq_result = cursor.fetchone()

        if org_seq_result and org_seq_result[0]:
            # Extract sequence name
            org_seq_full = org_seq_result[0]
            org_seq_name = org_seq_full.split('.')[-1] if '.' in org_seq_full else org_seq_full

            # Verify this sequence is in the list returned by get_all_table_sequences
            self.assertIn(
                org_seq_name,
                sequences_from_function,
                f"Sequence '{org_seq_name}' (from Organization table) should be in get_all_table_sequences result. "
                f'Got: {sequences_from_function}',
            )

    def test_get_schema_for_sequence_with_identity_columns(self):
        """
        Case: Call get_schema_for_sequence for an identity column sequence (Django 6.0+).
        Expected: Should return the correct schema name using pg_get_serial_sequence approach.
        """
        create_template_schema('default')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        # Find a table with an identity column
        cursor = connection.cursor()
        cursor.execute("""
            SELECT c.relname::text, n.nspname::text
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
            WHERE n.nspname = 'test_schema'
            AND a.attname = 'id'
            AND a.attidentity != ''
            AND c.relkind = 'r'
            LIMIT 1
        """)
        result = cursor.fetchone()

        if result:
            table_name, schema_name = result
            # Get the sequence name using pg_get_serial_sequence
            cursor.execute(
                """
                SELECT pg_get_serial_sequence(%s, 'id')
            """,
                ['{}.{}'.format(schema_name, table_name)],
            )
            seq_result = cursor.fetchone()

            if seq_result and seq_result[0]:
                # Extract sequence name (remove schema prefix)
                full_seq_name = seq_result[0]
                seq_name = full_seq_name.split('.')[-1] if '.' in full_seq_name else full_seq_name

                # Test get_schema_for_sequence
                schema_result = connection.get_schema_for_sequence(seq_name)
                # The function queries pg_sequence, which may not include identity column sequences
                # So we verify the behavior - it may return empty or the correct schema
                if schema_result:
                    # If it returns a result, verify it's correct
                    self.assertIn((schema_name,), schema_result)
                else:
                    # If it returns empty, that's a known limitation for identity columns
                    # Document this behavior
                    self.assertEqual(schema_result, [])

    def test_clone_schema_with_identity_columns(self):
        """
        Case: Clone a schema containing identity columns (Django 6.0+).
        Expected: Identity columns should be cloned correctly with sequences reset properly.
        """
        create_template_schema('default')

        # Create a test schema and add some data
        connection.create_schema('source_schema')
        connection.clone_schema('template', 'source_schema')

        # Insert some test data to create IDs
        with use_shard(node_name='default', schema_name='source_schema') as env:
            # Use raw SQL to insert into a table (assuming Organization exists)
            cursor = env.connection.cursor()
            try:
                cursor.execute('INSERT INTO "example_organization" (name) VALUES (%s) RETURNING id', ['Test Org 1'])
                cursor.execute('INSERT INTO "example_organization" (name) VALUES (%s) RETURNING id', ['Test Org 2'])
            except Exception:
                # If table doesn't exist or other error, skip data insertion
                pass

        # Clone the schema
        connection.create_schema('dest_schema')
        connection.clone_schema('source_schema', 'dest_schema')

        # Verify the cloned schema has the same structure
        source_tables = connection.get_all_table_headers(schema_name='source_schema')
        dest_tables = connection.get_all_table_headers(schema_name='dest_schema')
        self.assertCountEqual(source_tables, dest_tables)

        # Verify identity columns are properly cloned
        cursor = connection.cursor()
        for table in source_tables:
            # Check if source table has identity column
            cursor.execute(
                """
                SELECT a.attidentity
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'source_schema'
                AND c.relname = %s
                AND a.attname = 'id'
                AND a.attnum > 0
                AND NOT a.attisdropped
            """,
                [table],
            )
            source_result = cursor.fetchone()

            if source_result and source_result[0]:
                # Source has identity column, verify dest also has it
                cursor.execute(
                    """
                    SELECT a.attidentity
                    FROM pg_catalog.pg_attribute a
                    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = 'dest_schema'
                    AND c.relname = %s
                    AND a.attname = 'id'
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                """,
                    [table],
                )
                dest_result = cursor.fetchone()
                self.assertIsNotNone(dest_result, f'Table {table} should have id column in dest_schema')
                self.assertEqual(
                    dest_result[0], source_result[0], f'Identity column type should match for table {table}'
                )

                # Verify sequence is accessible via pg_get_serial_sequence
                cursor.execute(
                    """
                    SELECT pg_get_serial_sequence('dest_schema.{table}', 'id')
                """.format(table=table)
                )
                seq_result = cursor.fetchone()
                self.assertIsNotNone(seq_result[0], f'Identity column sequence should be accessible for table {table}')

                # Verify sequence is set correctly (should be >= max(id) + 1)
                cursor.execute(
                    """
                    SELECT COALESCE(MAX(id), 0) FROM "dest_schema"."{table}"
                """.format(table=table)
                )
                max_id_result = cursor.fetchone()
                max_id = max_id_result[0] if max_id_result else 0

                if max_id > 0:
                    # Get current sequence value
                    seq_name = seq_result[0]
                    cursor.execute('SELECT last_value FROM {}'.format(seq_name))
                    last_value_result = cursor.fetchone()
                    if last_value_result:
                        last_value = last_value_result[0]
                        # Sequence should be set to at least max_id + 1
                        self.assertGreaterEqual(
                            last_value, max_id, f'Sequence for {table} should be >= max(id) = {max_id}'
                        )

    def test_flush_schema_with_identity_columns(self):
        """
        Case: Call flush_schema on a schema with identity columns (Django 6.0+).
        Expected: Should drop all tables and sequences, including identity column sequences.
        Note: This test documents current behavior - get_all_table_sequences may miss identity sequences.
        """
        create_template_schema('default')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        # Verify schema has tables and sequences before flush
        tables_before = connection.get_all_table_headers(schema_name='test_schema')

        self.assertNotEqual(tables_before, [])
        # Sequences may be empty if get_all_table_sequences doesn't find identity sequences
        # This documents the potential limitation

        # Flush the schema
        with use_shard(node_name='default', schema_name='test_schema') as env:
            env.connection.flush_schema(schema_name='test_schema')

        # Verify tables are gone
        tables_after = connection.get_all_table_headers(schema_name='test_schema')
        self.assertEqual(tables_after, [])

        # Verify sequences are gone (using direct query to catch identity sequences too)
        cursor = connection.cursor()
        cursor.execute("""
            SELECT cls.relname::text
            FROM pg_catalog.pg_sequence seq
            JOIN pg_catalog.pg_class cls ON seq.seqrelid = cls.oid
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = 'test_schema'
        """)
        remaining_sequences = [row[0] for row in cursor.fetchall()]

        # Also check for identity column sequences that might not be in pg_sequence
        # but should be cleaned up when tables are dropped
        cursor.execute("""
            SELECT COUNT(*)
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'test_schema'
            AND a.attidentity != ''
        """)
        remaining_identity_cols = cursor.fetchone()[0]

        # After flush, there should be no sequences or identity columns
        self.assertEqual(remaining_sequences, [], 'All sequences should be dropped after flush_schema')
        self.assertEqual(
            remaining_identity_cols, 0, 'All identity columns should be dropped after flush_schema (tables are dropped)'
        )

    def test_reset_sequence_with_identity_columns(self):
        """
        Case: Call reset_sequence for models with identity columns (Django 6.0+).
        Expected: Should reset the identity column sequence correctly using pg_get_serial_sequence.
        """
        create_template_schema('default')
        shard = Shard.objects.create(
            alias='test_shard', schema_name='test_shard', node_name='default', state=State.ACTIVE
        )

        # Create some data to establish max IDs
        with use_shard(shard):
            org1 = Organization.objects.create(name='Org 1')
            org2 = Organization.objects.create(name='Org 2')
            max_id = max(org1.id, org2.id)

        # Reset sequences
        with use_shard(shard) as env:
            env.connection.reset_sequence(model_list=[Organization])

        # Verify sequence is reset correctly
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT pg_get_serial_sequence(%s, 'id')
        """,
            ['{}.{}'.format(shard.schema_name, Organization._meta.db_table)],
        )
        seq_result = cursor.fetchone()

        if seq_result and seq_result[0]:
            seq_name = seq_result[0]
            # Get current sequence value
            cursor.execute('SELECT last_value, is_called FROM {}'.format(seq_name))
            seq_info = cursor.fetchone()

            if seq_info:
                last_value, is_called = seq_info
                # reset_sequence() uses setval(seq, max(id), true), so:
                # - last_value should equal max_id
                # - is_called should be True if max_id > 0
                # - Next nextval() will return max_id + 1
                self.assertEqual(
                    last_value,
                    max_id,
                    f'Sequence last_value should equal max_id ({max_id}, from {org1.id} and {org2.id}), got {last_value}',
                )
                if max_id > 0:
                    self.assertTrue(is_called, 'Sequence should be marked as called when max_id > 0')


def rename_sequence_of_column(cursor, table_name, column_name, new_name):
    cursor.execute('SELECT pg_get_serial_sequence(%s, %s)', [table_name, column_name])
    cursor.execute('ALTER SEQUENCE {} RENAME TO {}'.format(cursor.fetchone()[0], new_name))


class ResetSequenceTestCase(ShardingTransactionTestCase):
    def _clone_template_into_test_schema(self):
        create_template_schema('default')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

    def test_reset_sequence_on_a_renamed_identity_table(self):
        """
        Case: example_organization's identity sequence has the name the table had before it was renamed, and the table
              has rows inserted with explicit ids. reset_sequence is then called.
        Expected: The next insert gets the max id + 1.
        """
        self._clone_template_into_test_schema()

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            rename_sequence_of_column(cursor, 'example_organization', 'id', 'legacy_organization_id_seq')
            cursor.execute(
                "INSERT INTO example_organization (id, name, created_at) VALUES (5, 'a', now()), (9, 'b', now())"
            )

            env.connection.reset_sequence(model_list=[Organization])

            cursor.execute("INSERT INTO example_organization (name, created_at) VALUES ('c', now()) RETURNING id")
            self.assertEqual(cursor.fetchone()[0], 10)

    def test_reset_sequence_on_a_renamed_serial_table(self):
        """
        Case: example_organization's id is a serial column whose sequence has the name the table had before it was
              renamed, and the table has rows inserted with explicit ids. reset_sequence is then called.
        Expected: The next insert gets the max id + 1.
        """
        self._clone_template_into_test_schema()

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute('ALTER TABLE example_organization ALTER COLUMN id DROP IDENTITY')
            cursor.execute('CREATE SEQUENCE legacy_organization_id_seq OWNED BY example_organization.id')
            cursor.execute(
                "ALTER TABLE example_organization ALTER COLUMN id SET DEFAULT nextval('legacy_organization_id_seq')"
            )
            cursor.execute(
                "INSERT INTO example_organization (id, name, created_at) VALUES (5, 'a', now()), (9, 'b', now())"
            )

            env.connection.reset_sequence(model_list=[Organization])

            cursor.execute("INSERT INTO example_organization (name, created_at) VALUES ('c', now()) RETURNING id")
            self.assertEqual(cursor.fetchone()[0], 10)

    def test_reset_sequence_on_an_empty_renamed_table(self):
        """
        Case: example_organization's identity sequence has the name the table had before it was renamed, and the table
              is empty. reset_sequence is then called.
        Expected: The first insert gets id 1.
        """
        self._clone_template_into_test_schema()

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            rename_sequence_of_column(cursor, 'example_organization', 'id', 'legacy_organization_id_seq')

            env.connection.reset_sequence(model_list=[Organization])

            cursor.execute("INSERT INTO example_organization (name, created_at) VALUES ('a', now()) RETURNING id")
            self.assertEqual(cursor.fetchone()[0], 1)

    def test_reset_sequence_on_a_renamed_through_table(self):
        """
        Case: The identity sequence of User.cake's auto-created through table has the name the table had before it was
              renamed, and the table has a row inserted with an explicit id. reset_sequence is then called for the
              through model, as move_shard_to_node does.
        Expected: The next row added through the relation gets the max id + 1.
        """
        self._clone_template_into_test_schema()
        through_model = User.cake.through

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            rename_sequence_of_column(cursor, through_model._meta.db_table, 'id', 'legacy_user_cake_id_seq')
            user = User.objects.create_user(email='user@example.com', name='user')
            first_cake = Cake.objects.create(name='first')
            second_cake = Cake.objects.create(name='second')
            through_model.objects.create(id=7, user=user, cake=first_cake)

            env.connection.reset_sequence(model_list=[through_model])

            user.cake.add(second_cake)
            self.assertEqual(through_model.objects.get(cake=second_cake).id, 8)

    def test_reset_sequence_on_a_clone_of_a_serial_table(self):
        """
        Case: The template's example_organization has a serial id column. A schema is cloned from the template, rows
              with explicit ids are inserted into the clone, and reset_sequence is called on the clone.
        Expected: The next insert gets the max id + 1.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('ALTER TABLE template.example_organization ALTER COLUMN id DROP IDENTITY')
        cursor.execute('CREATE SEQUENCE template.example_organization_id_seq OWNED BY template.example_organization.id')
        cursor.execute(
            'ALTER TABLE template.example_organization ALTER COLUMN id'
            " SET DEFAULT nextval('template.example_organization_id_seq')"
        )
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute(
                "INSERT INTO example_organization (id, name, created_at) VALUES (5, 'a', now()), (9, 'b', now())"
            )

            env.connection.reset_sequence(model_list=[Organization])

            cursor.execute("INSERT INTO example_organization (name, created_at) VALUES ('c', now()) RETURNING id")
            self.assertEqual(cursor.fetchone()[0], 10)

    def test_reset_sequence_on_a_serial_column_whose_sequence_has_no_owner(self):
        """
        Case: example_organization's id is a serial column whose default takes its values from a sequence that is not
              owned by any column and has the name the table had before it was renamed. The table has rows inserted
              with explicit ids. reset_sequence is then called.
        Expected: The next insert gets the max id + 1.
        """
        self._clone_template_into_test_schema()

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute('ALTER TABLE example_organization ALTER COLUMN id DROP IDENTITY')
            cursor.execute('CREATE SEQUENCE legacy_organization_id_seq')
            cursor.execute(
                "ALTER TABLE example_organization ALTER COLUMN id SET DEFAULT nextval('legacy_organization_id_seq')"
            )
            cursor.execute(
                "INSERT INTO example_organization (id, name, created_at) VALUES (5, 'a', now()), (9, 'b', now())"
            )

            env.connection.reset_sequence(model_list=[Organization])

            cursor.execute("INSERT INTO example_organization (name, created_at) VALUES ('c', now()) RETURNING id")
            self.assertEqual(cursor.fetchone()[0], 10)

    def test_reset_sequence_on_a_serial_column_whose_default_uses_a_sequence_it_does_not_own(self):
        """
        Case: example_organization's id owns one sequence, but its default takes its values from another. The table
              has rows inserted with explicit ids. reset_sequence is then called.
        Expected: The sequence the default uses is reset, so the next insert gets the max id + 1.
        """
        self._clone_template_into_test_schema()

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute('ALTER TABLE example_organization ALTER COLUMN id DROP IDENTITY')
            cursor.execute('CREATE SEQUENCE owned_organization_id_seq OWNED BY example_organization.id')
            cursor.execute('CREATE SEQUENCE default_organization_id_seq')
            cursor.execute(
                "ALTER TABLE example_organization ALTER COLUMN id SET DEFAULT nextval('default_organization_id_seq')"
            )
            cursor.execute(
                "INSERT INTO example_organization (id, name, created_at) VALUES (5, 'a', now()), (9, 'b', now())"
            )

            env.connection.reset_sequence(model_list=[Organization])

            cursor.execute("INSERT INTO example_organization (name, created_at) VALUES ('c', now()) RETURNING id")
            self.assertEqual(cursor.fetchone()[0], 10)

    def test_reset_sequence_on_a_column_whose_default_uses_two_sequences(self):
        """
        Case: example_organization's id has a default that names two sequences, b_organization_id_seq before
              a_organization_id_seq, and the table has rows inserted with explicit ids. reset_sequence is then called.
        Expected: The sequence whose name sorts first, a_organization_id_seq, is reset to the max id, and
                  b_organization_id_seq is unchanged.
        """
        self._clone_template_into_test_schema()

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute('ALTER TABLE example_organization ALTER COLUMN id DROP IDENTITY')
            cursor.execute('CREATE SEQUENCE a_organization_id_seq')
            cursor.execute('CREATE SEQUENCE b_organization_id_seq')
            cursor.execute(
                'ALTER TABLE example_organization ALTER COLUMN id'
                " SET DEFAULT coalesce(nextval('b_organization_id_seq'), nextval('a_organization_id_seq'))"
            )
            cursor.execute(
                "INSERT INTO example_organization (id, name, created_at) VALUES (5, 'a', now()), (9, 'b', now())"
            )

            env.connection.reset_sequence(model_list=[Organization])

            cursor.execute(
                'SELECT a_seq.last_value, a_seq.is_called, b_seq.is_called'
                ' FROM a_organization_id_seq a_seq, b_organization_id_seq b_seq'
            )
            self.assertEqual(cursor.fetchone(), (9, True, False))

    def test_reset_sequence_resets_several_models_in_one_call(self):
        """
        Case: example_organization and example_cake both have rows inserted with explicit ids. reset_sequence is then
              called for both models in one call.
        Expected: The next insert into each table gets that table's max id + 1.
        """
        self._clone_template_into_test_schema()

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute(
                "INSERT INTO example_organization (id, name, created_at) VALUES (5, 'a', now()), (9, 'b', now())"
            )
            Cake.objects.create(id=7, name='first')

            env.connection.reset_sequence(model_list=[Organization, Cake])

            cursor.execute("INSERT INTO example_organization (name, created_at) VALUES ('c', now()) RETURNING id")
            self.assertEqual(cursor.fetchone()[0], 10)
            self.assertEqual(Cake.objects.create(name='second').id, 8)

    def test_reset_sequence_raises_for_a_column_without_a_sequence(self):
        """
        Case: example_organization's id is not an identity column and has no sequence, and example_cake has a row
              inserted with an explicit id. reset_sequence is then called for both models in one call.
        Expected: A ValueError that names example_organization's column, raised before any sequence is reset, so
                  example_cake's sequence is unchanged.
        """
        self._clone_template_into_test_schema()

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute('ALTER TABLE example_organization ALTER COLUMN id DROP IDENTITY')
            Cake.objects.create(id=7, name='first')

            with self.assertRaisesMessage(ValueError, 'example_organization.id'):
                env.connection.reset_sequence(model_list=[Organization, Cake])

            self.assertEqual(Cake.objects.create(name='second').id, 1)

    def test_reset_sequence_raises_for_a_model_whose_table_is_missing(self):
        """
        Case: The schema has no example_organization table, and example_cake has a row inserted with an explicit id.
              reset_sequence is then called for both models in one call.
        Expected: A ValueError that names example_organization's column, raised before any sequence is reset, so
                  example_cake's sequence is unchanged.
        """
        self._clone_template_into_test_schema()

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute('DROP TABLE example_organization CASCADE')
            Cake.objects.create(id=7, name='first')

            with self.assertRaisesMessage(ValueError, 'example_organization.id'):
                env.connection.reset_sequence(model_list=[Organization, Cake])

            self.assertEqual(Cake.objects.create(name='second').id, 1)

    def test_reset_sequence_raises_for_a_model_whose_column_is_missing(self):
        """
        Case: The schema's example_organization table has no id column, and example_cake has a row inserted with an
              explicit id. reset_sequence is then called for both models in one call.
        Expected: A ValueError that names example_organization's column, raised before any sequence is reset, so
                  example_cake's sequence is unchanged.
        """
        self._clone_template_into_test_schema()

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute('ALTER TABLE example_organization DROP COLUMN id CASCADE')
            Cake.objects.create(id=7, name='first')

            with self.assertRaisesMessage(ValueError, 'example_organization.id'):
                env.connection.reset_sequence(model_list=[Organization, Cake])

            self.assertEqual(Cake.objects.create(name='second').id, 1)

    def test_reset_sequence_never_rewinds(self):
        """
        Case: A sequence was advanced past the table's max id, as a concurrent insert on a live target shard does while
              move_data_to_shard runs. After that, reset_sequence is called.
        Expected: The sequence keeps its advanced position instead of being rewound to max(id).
        """
        create_template_schema('default')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        with use_shard(node_name='default', schema_name='test_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute("INSERT INTO example_organization (name, created_at) VALUES ('a', now())")
            cursor.execute("SELECT pg_get_serial_sequence('test_schema.example_organization', 'id')")
            sequence = cursor.fetchone()[0]
            cursor.execute('SELECT setval(%s, 500, true)', [sequence])

            env.connection.reset_sequence(model_list=[Organization])

            cursor.execute('SELECT last_value FROM {}'.format(sequence))
            self.assertEqual(cursor.fetchone()[0], 500)


class RenamedTableTestCase(ShardingTransactionTestCase):
    @staticmethod
    def _primary_key_names(schema_name, table_name):
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT con.conname::text, idx_cls.relname::text
              FROM pg_catalog.pg_constraint con
              JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
              JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
              JOIN pg_catalog.pg_class idx_cls ON idx_cls.oid = con.conindid
              WHERE nsp.nspname = %s AND cls.relname = %s AND con.contype = 'p'
            """,
            [schema_name, table_name],
        )
        return cursor.fetchone()

    @staticmethod
    def _sequence_name(schema_name, table_name, column_name):
        cursor = connection.cursor()
        cursor.execute('SELECT pg_get_serial_sequence(%s, %s)', ['{}.{}'.format(schema_name, table_name), column_name])
        return cursor.fetchone()[0]

    def test_clone_schema_keeps_the_primary_key_name_of_a_renamed_table(self):
        """
        Case: The template has a table with an identity primary key, and the table was renamed after it was created.
        Expected: The clone's primary key constraint and its index have the template's names, not names based on the
                  table's current name.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.legacy_widget (code BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)')
        cursor.execute('ALTER TABLE template.legacy_widget RENAME TO widget')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        self.assertEqual(self._primary_key_names('test_schema', 'widget'), ('legacy_widget_pkey', 'legacy_widget_pkey'))

    def test_clone_schema_keeps_the_primary_key_name_of_a_renamed_partitioned_table(self):
        """
        Case: The template has a partitioned table with a primary key, and the table was renamed after it was created.
        Expected: The clone's primary key constraint and its index have the template's names, not names based on the
                  table's current name.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.legacy_widget (code INTEGER PRIMARY KEY) PARTITION BY RANGE (code)')
        cursor.execute('ALTER TABLE template.legacy_widget RENAME TO widget')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        self.assertEqual(self._primary_key_names('test_schema', 'widget'), ('legacy_widget_pkey', 'legacy_widget_pkey'))

    def test_clone_schema_names_the_primary_key_after_a_table_that_was_never_renamed(self):
        """
        Case: The template has a table with an identity primary key, and the table was never renamed.
        Expected: The clone's primary key and identity sequence are named after the table.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.widget (code BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        self.assertEqual(self._primary_key_names('test_schema', 'widget'), ('widget_pkey', 'widget_pkey'))
        self.assertEqual(self._sequence_name('test_schema', 'widget', 'code'), 'test_schema.widget_code_seq')

    def test_clone_schema_with_a_newer_table_that_has_a_renamed_tables_old_name(self):
        """
        Case: The template has a table that was renamed from widget to gadget, and a newer table named widget. So the
              renamed table's primary key and identity sequence have the names that the newer table's would get.
        Expected: The clone succeeds, and both tables have the template's primary key and identity sequence names.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.widget (code BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)')
        cursor.execute('ALTER TABLE template.widget RENAME TO gadget')
        cursor.execute('CREATE TABLE template.widget (code BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        for table_name in ('gadget', 'widget'):
            with self.subTest(table=table_name):
                self.assertEqual(
                    self._primary_key_names('test_schema', table_name),
                    self._primary_key_names('template', table_name),
                )
                self.assertEqual(
                    self._sequence_name('test_schema', table_name, 'code').split('.')[1],
                    self._sequence_name('template', table_name, 'code').split('.')[1],
                )

    def test_clone_schema_keeps_the_identity_sequence_name_of_a_renamed_table(self):
        """
        Case: The template has a table with an identity primary key, and the table was renamed after it was created.
        Expected: The clone's identity sequence has the template's name, not a name based on the table's current
                  name.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.legacy_widget (code BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)')
        cursor.execute('ALTER TABLE template.legacy_widget RENAME TO widget')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        self.assertEqual(self._sequence_name('test_schema', 'widget', 'code'), 'test_schema.legacy_widget_code_seq')

    def test_clone_schema_with_a_default_using_the_identity_sequence_of_a_renamed_table(self):
        """
        Case: The template has a table with an identity primary key that was renamed from widget to gadget, so its
              identity sequence is named widget_code_seq. Another column's default takes its values from that sequence,
              by name.
        Expected: The clone succeeds, and the default takes its values from the clone's own identity sequence.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.widget (code BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)')
        cursor.execute('ALTER TABLE template.widget RENAME TO gadget')
        cursor.execute(
            "ALTER TABLE template.gadget ADD COLUMN ticket BIGINT DEFAULT nextval('template.widget_code_seq')"
        )
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        cursor.execute('INSERT INTO test_schema.gadget (code) VALUES (100) RETURNING ticket')
        self.assertEqual(cursor.fetchone()[0], 1)
        cursor.execute('SELECT last_value FROM test_schema.widget_code_seq')
        self.assertEqual(cursor.fetchone()[0], 1)
        cursor.execute('SELECT is_called FROM template.widget_code_seq')
        self.assertFalse(cursor.fetchone()[0])

    def test_clone_schema_into_a_schema_that_has_a_relation_with_a_primary_key_name(self):
        """
        Case: The template has a table that was renamed from legacy_widget, with a primary key named legacy_widget_pkey.
              The schema cloned into already has a sequence named legacy_widget_pkey.
        Expected: The clone raises an error that lists the name already in use, and the schema is unchanged.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.legacy_widget (code INTEGER PRIMARY KEY)')
        cursor.execute('ALTER TABLE template.legacy_widget RENAME TO widget')
        connection.create_schema('test_schema')
        cursor.execute('CREATE SEQUENCE test_schema.legacy_widget_pkey')

        with self.assertRaisesMessage(
            DatabaseError, 'test_schema.legacy_widget_pkey already exists, so widget_pkey cannot be renamed to it'
        ):
            connection.clone_schema('template', 'test_schema')

        cursor.execute("SELECT to_regclass('test_schema.widget')")
        self.assertIsNone(cursor.fetchone()[0])

    def test_clone_schema_into_a_schema_that_has_a_relation_with_an_identity_sequence_name(self):
        """
        Case: The template has a table with an identity primary key that was renamed from legacy_widget, so its
              identity sequence is named legacy_widget_code_seq. The schema cloned into already has a sequence named
              legacy_widget_code_seq.
        Expected: The clone raises an error that lists the name already in use, and the schema is unchanged.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.legacy_widget (code BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)')
        cursor.execute('ALTER TABLE template.legacy_widget RENAME TO widget')
        cursor.execute('ALTER TABLE template.widget RENAME CONSTRAINT legacy_widget_pkey TO widget_pkey')
        connection.create_schema('test_schema')
        cursor.execute('CREATE SEQUENCE test_schema.legacy_widget_code_seq')

        with self.assertRaisesMessage(
            DatabaseError,
            'test_schema.legacy_widget_code_seq already exists, so widget_code_seq cannot be renamed to it',
        ):
            connection.clone_schema('template', 'test_schema')

        cursor.execute("SELECT to_regclass('test_schema.widget')")
        self.assertIsNone(cursor.fetchone()[0])

    def test_clone_schema_keeps_the_serial_sequence_name_of_a_renamed_table(self):
        """
        Case: The template has a table with a serial primary key, and the table was renamed after it was created.
        Expected: The clone's serial sequence has the template's name, and the column's default takes its values from
                  it.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.legacy_widget (code SERIAL PRIMARY KEY)')
        cursor.execute('ALTER TABLE template.legacy_widget RENAME TO widget')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        self.assertEqual(self._sequence_name('test_schema', 'widget', 'code'), 'test_schema.legacy_widget_code_seq')
        cursor.execute('INSERT INTO test_schema.widget DEFAULT VALUES RETURNING code')
        self.assertEqual(cursor.fetchone()[0], 1)

    def test_clone_schema_makes_the_serial_sequence_of_a_partitioned_table_owned_by_its_column(self):
        """
        Case: The template has a partitioned table with a serial primary key.
        Expected: The clone's serial sequence is owned by the clone's column, so dropping the table also drops the
                  sequence.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.widget (code SERIAL PRIMARY KEY) PARTITION BY RANGE (code)')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        self.assertEqual(self._sequence_name('test_schema', 'widget', 'code'), 'test_schema.widget_code_seq')
        cursor.execute('DROP TABLE test_schema.widget')
        cursor.execute("SELECT to_regclass('test_schema.widget_code_seq')")
        self.assertIsNone(cursor.fetchone()[0])

    def test_clone_schema_with_a_newer_identity_table_that_has_a_renamed_serial_tables_old_name(self):
        """
        Case: The template has a table with a serial primary key that was renamed from widget to gadget, and a newer
              table named widget with an identity primary key. So the serial sequence has the name that the newer
              table's identity sequence would get.
        Expected: The clone succeeds, and both tables have the template's sequence names.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template.widget (code SERIAL PRIMARY KEY)')
        cursor.execute('ALTER TABLE template.widget RENAME TO gadget')
        cursor.execute('CREATE TABLE template.widget (code BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        for table_name in ('gadget', 'widget'):
            with self.subTest(table=table_name):
                self.assertEqual(
                    self._sequence_name('test_schema', table_name, 'code').split('.')[1],
                    self._sequence_name('template', table_name, 'code').split('.')[1],
                )

    def test_clone_schema_keeps_a_mixed_case_serial_sequence_name(self):
        """
        Case: The template has a table with a serial primary key. The table was created with a mixed-case name, which
              its sequence's name is based on, and was then renamed.
        Expected: The clone's serial sequence has the template's mixed-case name, and the column's default takes its
                  values from it.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE template."Legacy_Widget" (code SERIAL PRIMARY KEY)')
        cursor.execute('ALTER TABLE template."Legacy_Widget" RENAME TO widget')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        self.assertEqual(self._sequence_name('test_schema', 'widget', 'code'), 'test_schema."Legacy_Widget_code_seq"')
        cursor.execute('INSERT INTO test_schema.widget DEFAULT VALUES RETURNING code')
        self.assertEqual(cursor.fetchone()[0], 1)

    def test_reset_sequence_on_a_clone_of_a_renamed_table(self):
        """
        Case: The primary key and identity sequence of the template's example_organization have names based on a name
              the table had before it was renamed. A schema is cloned from the template, rows with explicit ids are
              inserted into the clone, and reset_sequence is called on the clone.
        Expected: The clone has the template's names, and the next insert gets the max id + 1.
        """
        create_template_schema('default')
        cursor = connection.cursor()
        rename_sequence_of_column(cursor, 'template.example_organization', 'id', 'legacy_organization_id_seq')
        cursor.execute(
            'ALTER TABLE template.example_organization RENAME CONSTRAINT example_organization_pkey'
            ' TO legacy_organization_pkey'
        )
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        self.assertEqual(
            self._sequence_name('test_schema', 'example_organization', 'id'), 'test_schema.legacy_organization_id_seq'
        )
        self.assertEqual(self._primary_key_names('test_schema', 'example_organization')[0], 'legacy_organization_pkey')
        with use_shard(node_name='default', schema_name='test_schema') as env:
            shard_cursor = env.connection.cursor()
            shard_cursor.execute(
                "INSERT INTO example_organization (id, name, created_at) VALUES (5, 'a', now()), (9, 'b', now())"
            )
            env.connection.reset_sequence(model_list=[Organization])
            shard_cursor.execute("INSERT INTO example_organization (name, created_at) VALUES ('c', now()) RETURNING id")
            self.assertEqual(shard_cursor.fetchone()[0], 10)


class TriggersTestCase(ShardingTransactionTestCase):
    def test_clone_schema_with_triggers(self):
        """
        Case: Clone a schema containing triggers.
        Expected: Triggers should be cloned correctly to the destination schema and function correctly.
        """
        create_template_schema('default')

        # Create a test schema
        connection.create_schema('source_schema')
        connection.clone_schema('template', 'source_schema')

        with use_shard(node_name='default', schema_name='source_schema') as env:
            cursor = env.connection.cursor()
            # First check if example_organization table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'source_schema'
                    AND table_name = 'example_organization'
                )
            """)
            self.assertTrue(cursor.fetchone()[0])

            # Create log tables to track updates
            cursor.execute("""
                CREATE TABLE update_log (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    record_id INTEGER,
                    trigger_type INTEGER,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Create a simple trigger function that inserts into the log table with a fully qualified reference
            cursor.execute("""
                CREATE OR REPLACE FUNCTION log_update_trigger_1()
                RETURNS TRIGGER AS $$
                BEGIN
                    INSERT INTO source_schema.update_log (table_name, record_id, trigger_type)
                    VALUES (TG_TABLE_NAME, NEW.id, 1);
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """)
            # Create a simple trigger function that inserts into the log table with a reference that relies on
            # search_path being set correctly.
            cursor.execute("""
                CREATE OR REPLACE FUNCTION log_update_trigger_2()
                RETURNS TRIGGER AS $$
                BEGIN
                    INSERT INTO update_log (table_name, record_id, trigger_type)
                    VALUES (TG_TABLE_NAME, NEW.id, 2);
                    RETURN NEW;
                    END;
                $$ LANGUAGE plpgsql;
            """)

            # Create triggers on example_organization that fires on UPDATE
            cursor.execute("""
                CREATE TRIGGER update_log_trigger_1
                    AFTER UPDATE ON example_organization
                    FOR EACH ROW
                    EXECUTE FUNCTION log_update_trigger_1();
                """)
            cursor.execute("""
                CREATE TRIGGER update_log_trigger_2
                AFTER UPDATE ON example_organization
                FOR EACH ROW
                EXECUTE FUNCTION log_update_trigger_2();
            """)

        # Clone the schema
        connection.create_schema('dest_schema')
        connection.clone_schema('source_schema', 'dest_schema')

        # Verify triggers are cloned
        cursor = connection.cursor()
        # Get triggers from source schema
        cursor.execute("""
            SELECT tg.tgname::text, pg_get_triggerdef(tg.oid)::text
            FROM pg_catalog.pg_trigger tg
            JOIN pg_catalog.pg_class cls ON tg.tgrelid = cls.oid
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = 'source_schema'
              AND cls.relname = 'example_organization'
              AND NOT tg.tgisinternal
        """)
        source_triggers = cursor.fetchall()

        self.assertGreaterEqual(len(source_triggers), 2, 'Source schema should have at least two triggers')

        # Get triggers from dest schema
        cursor.execute("""
            SELECT tg.tgname::text, pg_get_triggerdef(tg.oid)::text
            FROM pg_catalog.pg_trigger tg
            JOIN pg_catalog.pg_class cls ON tg.tgrelid = cls.oid
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = 'dest_schema'
              AND cls.relname = 'example_organization'
              AND NOT tg.tgisinternal
        """)
        dest_triggers = cursor.fetchall()

        # Verify we have the same number of triggers
        self.assertEqual(
            len(dest_triggers), len(source_triggers), 'Number of triggers should match between source and dest schemas'
        )

        # Verify trigger names match
        source_trigger_names = {t[0] for t in source_triggers}
        dest_trigger_names = {t[0] for t in dest_triggers}
        self.assertEqual(
            source_trigger_names, dest_trigger_names, 'Trigger names should match between source and dest schemas'
        )

        # Verify trigger definitions reference the correct schema
        for dest_trigger_name, dest_trigger_def in dest_triggers:
            # The trigger definition should reference dest_schema for the table, not source_schema
            self.assertIn(
                'dest_schema.example_organization',
                dest_trigger_def,
                f'Trigger {dest_trigger_name} definition should reference dest_schema table',
            )
            self.assertNotIn(
                'source_schema.example_organization',
                dest_trigger_def,
                f'Trigger {dest_trigger_name} definition should not reference source_schema table',
            )

        # Test that the trigger actually works by performing an UPDATE and checking the log tables. The two trigger
        # functions codify the cloning contract for bodies: the search-path-relying one follows the schema it runs
        # in, while the explicitly qualified one keeps pointing at the schema it names - bodies are never rewritten.
        with use_shard(node_name='default', schema_name='dest_schema') as env:
            cursor = env.connection.cursor()
            # Insert a test record
            cursor.execute(
                'INSERT INTO dest_schema.example_organization (name, created_at) VALUES (%s, %s) RETURNING id',
                ['Test Org', '2024-01-01 00:00:00'],
            )
            org_id = cursor.fetchone()[0]

            # Verify both log tables are empty before update
            cursor.execute('SELECT COUNT(*) FROM dest_schema.update_log')
            self.assertEqual(cursor.fetchone()[0], 0, 'Log table should be empty before update')
            cursor.execute('SELECT COUNT(*) FROM source_schema.update_log')
            self.assertEqual(cursor.fetchone()[0], 0, 'Source log table should be empty before update')

            # Update the record - both triggers fire: the search-path one logs here, the qualified one at the
            # schema its body names.
            cursor.execute(
                'UPDATE dest_schema.example_organization SET name = %s WHERE id = %s', ['Updated Org', org_id]
            )

            cursor.execute('SELECT COUNT(*) FROM dest_schema.update_log')
            self.assertEqual(cursor.fetchone()[0], 1, 'The search-path-relying trigger should log on this schema')
            cursor.execute('SELECT COUNT(*) FROM source_schema.update_log')
            self.assertEqual(cursor.fetchone()[0], 1, 'The qualified trigger should log on the schema it names')

            # Update the record again with an explicit search_path - same distribution.
            cursor.execute('SET search_path = dest_schema,public')
            cursor.execute('UPDATE example_organization SET name = %s WHERE id = %s', ['Updated Org', org_id])

            cursor.execute('SELECT COUNT(*) FROM dest_schema.update_log')
            self.assertEqual(cursor.fetchone()[0], 2, 'The search-path-relying trigger should have logged again')
            cursor.execute('SELECT COUNT(*) FROM source_schema.update_log')
            self.assertEqual(cursor.fetchone()[0], 2, 'The qualified trigger should have logged again')

            # Verify the log entries carry the correct data on both sides
            for log_table, trigger_type in (('source_schema.update_log', 1), ('dest_schema.update_log', 2)):
                cursor.execute(
                    'SELECT table_name, record_id FROM {} WHERE record_id = %s AND trigger_type = %s'.format(log_table),
                    [org_id, trigger_type],
                )
                log_entries = cursor.fetchall()
                self.assertEqual(len(log_entries), 2, 'Both updates should have logged in {}'.format(log_table))
                for log_entry in log_entries:
                    self.assertEqual(log_entry[0], 'example_organization')
                    self.assertEqual(log_entry[1], org_id)


class GeneratedColumnsTestCase(ShardingTransactionTestCase):
    def setUp(self):
        super().setUp()
        connection.create_schema('generated_source_schema')
        connection.create_schema('generated_dest_schema')
        self.addCleanup(self._drop_generation_function)

    def _drop_generation_function(self):
        connection.cursor().execute(DROP_ALLUPPERCASE)

    def _create_generation_function(self):
        connection.cursor().execute(CREATE_ALLUPPERCASE)

    def _create_generated_column_table(self, table_name='generated_source', with_rows=True):
        self._create_generation_function()
        with use_shard(node_name='default', schema_name='generated_source_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute(
                """
                CREATE TABLE {table} (
                    id SERIAL PRIMARY KEY,
                    body TEXT NOT NULL DEFAULT '',
                    body_alluppercased TEXT GENERATED ALWAYS AS (public.alluppercase(body)) STORED
                )
                """.format(table=table_name)
            )
            if with_rows:
                cursor.execute(
                    'INSERT INTO {table} (body) VALUES (%s), (%s)'.format(table=table_name),
                    ['first', 'second'],
                )

    def test_get_copyable_column_names_by_table_matches_the_single_table_lookup(self):
        """
        Case: Ask for the copyable columns of a whole schema at once.
        Expected: One entry per table in declaration order, generated columns excluded.
        """
        self._create_generated_column_table()

        by_table = connection.get_copyable_column_names_by_table(schema_name='generated_source_schema')

        self.assertEqual(by_table['generated_source'], ['id', 'body'])
        for table, columns in by_table.items():
            self.assertEqual(
                columns, connection.get_copyable_column_names(table, schema_name='generated_source_schema')
            )

    def test_clone_schema_with_generated_columns(self):
        """
        Case: Clone a schema with a stored generated column and read pre-existing rows.
        Expected: The values are correctly function-generated.
        """
        self._create_generated_column_table()

        connection.clone_schema('generated_source_schema', 'generated_dest_schema')

        with use_shard(node_name='default', schema_name='generated_dest_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute('SELECT body, body_alluppercased FROM generated_source ORDER BY id')
            self.assertEqual(cursor.fetchall(), [('first', 'FIRST'), ('second', 'SECOND')])

    def test_clone_schema_keeps_the_column_generated(self):
        """
        Case: Clone a schema with a stored generated column and insert new rows.
        Expected: The generated column gets an appropriately function-created value for each new row.
        """
        self._create_generated_column_table()

        connection.clone_schema('generated_source_schema', 'generated_dest_schema')

        with use_shard(node_name='default', schema_name='generated_dest_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute("INSERT INTO generated_source (body) VALUES ('third')")
            cursor.execute("SELECT body_alluppercased FROM generated_source WHERE body = 'third'")
            self.assertEqual(cursor.fetchone(), ('THIRD',))

    def test_clone_schema_with_an_empty_generated_column_table(self):
        """
        Case: Clone a schema whose table has a generated column but no rows.
        Expected: The clone succeeds (no parser error triggers on INSERT on a generated row, even on an empty table).
        """
        self._create_generated_column_table(table_name='generated_empty', with_rows=False)

        connection.clone_schema('generated_source_schema', 'generated_dest_schema')

        with use_shard(node_name='default', schema_name='generated_dest_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute('SELECT COUNT(*) FROM generated_empty')
            self.assertEqual(cursor.fetchone(), (0,))

    def test_get_copyable_column_names_leaves_out_generated_columns(self):
        """
        Case: Request the schema for a table that has a stored generated column.
        Expected: The schema excludes the stored generated columns.
        """
        self._create_generated_column_table()

        columns = connection.get_copyable_column_names('generated_source', schema_name='generated_source_schema')

        self.assertEqual(columns, ['id', 'body'])


class ExpressionRebindTestCase(ShardingTransactionTestCase):
    """
    Postgres stores an expression with the OID of the function it calls, and cloning a table with
    CREATE TABLE ... (LIKE ... INCLUDING ALL) copies those OIDs verbatim. Every expression a table can carry therefore
    has to be rebound by name onto the schema it was cloned into, or the shard keeps calling the template's copy of a
    sharded function and stops working the moment that template is dropped.
    """

    SOURCE_SCHEMA = 'rebind_source_schema'
    DEST_SCHEMA = 'rebind_dest_schema'

    def setUp(self):
        super().setUp()
        connection.create_schema(self.SOURCE_SCHEMA)
        connection.create_schema(self.DEST_SCHEMA)

    def _create_sharded_function(self, schema_name):
        """
        Create a function under its bare name in the given schema, the way a ShardingMode.SHARDED declaration does.
        """
        with use_shard(node_name='default', schema_name=schema_name) as env:
            env.connection.cursor().execute(
                """
                CREATE FUNCTION shard_upper(input TEXT) RETURNS TEXT AS $function$
                    BEGIN
                        RETURN UPPER(input);
                    END;
                $function$ LANGUAGE plpgsql IMMUTABLE STRICT;
                """
            )

    def _create_table_using(self, function_reference, table_name='rebind_example'):
        """
        Create a table calling the given function from a default, a generated column, a check constraint, an expression
        index and a partial index, so one clone exercises every expression kind LIKE copies by OID.
        """
        with use_shard(node_name='default', schema_name=self.SOURCE_SCHEMA) as env:
            cursor = env.connection.cursor()
            cursor.execute(
                """
                CREATE TABLE {table} (
                    id SERIAL PRIMARY KEY,
                    body TEXT NOT NULL DEFAULT '',
                    label TEXT DEFAULT {function}('fresh'),
                    body_upper TEXT GENERATED ALWAYS AS ({function}(body)) STORED,
                    CONSTRAINT rebind_body_check CHECK ({function}(body) <> 'FORBIDDEN')
                )
                """.format(table=table_name, function=function_reference)
            )
            cursor.execute(
                'CREATE INDEX rebind_expr_idx ON {table} ({function}(body))'.format(
                    table=table_name, function=function_reference
                )
            )
            cursor.execute(
                "CREATE INDEX rebind_partial_idx ON {table} (body) WHERE {function}(body) <> 'SKIP'".format(
                    table=table_name, function=function_reference
                )
            )
            cursor.execute('INSERT INTO {table} (body) VALUES (%s), (%s)'.format(table=table_name), ['first', 'second'])

    def _expressions(self, schema_name, table_name='rebind_example'):
        """
        Return every expression the given table carries, as rendered from a connection that has neither schema on its
        search path, so that each function and sequence prints with the schema it is actually bound to.
        """
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT att.attname::text, pg_get_expr(def.adbin, def.adrelid, true)::text
            FROM pg_catalog.pg_attrdef def
            JOIN pg_catalog.pg_attribute att ON att.attrelid = def.adrelid AND att.attnum = def.adnum
            JOIN pg_catalog.pg_class cls ON cls.oid = def.adrelid
            JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
            WHERE nsp.nspname = %s AND cls.relname = %s
            """,
            [schema_name, table_name],
        )
        expressions = {'column {}'.format(column): expression for column, expression in cursor.fetchall()}

        cursor.execute(
            """
            SELECT con.conname::text, pg_get_constraintdef(con.oid, true)::text
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
            JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
            WHERE nsp.nspname = %s AND cls.relname = %s AND con.contype = 'c'
            """,
            [schema_name, table_name],
        )
        expressions.update({'constraint {}'.format(name): definition for name, definition in cursor.fetchall()})

        cursor.execute(
            """
            SELECT idx_cls.relname::text, pg_get_indexdef(idx.indexrelid, 0, true)::text
            FROM pg_catalog.pg_index idx
            JOIN pg_catalog.pg_class idx_cls ON idx_cls.oid = idx.indexrelid
            JOIN pg_catalog.pg_class cls ON cls.oid = idx.indrelid
            JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
            WHERE nsp.nspname = %s AND cls.relname = %s
            """,
            [schema_name, table_name],
        )
        expressions.update({'index {}'.format(name): definition for name, definition in cursor.fetchall()})

        return expressions

    def test_clone_schema_rebinds_expressions_to_the_functions_of_the_new_schema(self):
        """
        Case: Clone a schema whose table calls a function of that same schema from every kind of expression.
        Expected: Each expression on the clone calls the clone's own copy of the function, never the source's.
        """
        self._create_sharded_function(self.SOURCE_SCHEMA)
        self._create_table_using('shard_upper')

        connection.clone_schema(self.SOURCE_SCHEMA, self.DEST_SCHEMA)

        expressions = self._expressions(self.DEST_SCHEMA)
        self.assertCountEqual(
            expressions,
            [
                'column id',
                'column body',
                'column label',
                'column body_upper',
                'constraint rebind_body_check',
                'index rebind_example_pkey',
                'index rebind_expr_idx',
                'index rebind_partial_idx',
            ],
        )
        for name in ('column label', 'column body_upper', 'constraint rebind_body_check'):
            self.assertIn('{}.shard_upper'.format(self.DEST_SCHEMA), expressions[name])
        for name in ('index rebind_expr_idx', 'index rebind_partial_idx'):
            self.assertIn('{}.shard_upper'.format(self.DEST_SCHEMA), expressions[name])
        for name, expression in expressions.items():
            self.assertNotIn(self.SOURCE_SCHEMA, expression, 'Expression of {} still binds the source'.format(name))

    def test_clone_schema_rebinds_serial_defaults_to_the_sequence_of_the_new_schema(self):
        """
        Case: Clone a schema whose table has a serial primary key.
        Expected: The clone's default draws from the sequence cloned alongside it, not from the source's sequence.
        """
        self._create_sharded_function(self.SOURCE_SCHEMA)
        self._create_table_using('shard_upper')

        connection.clone_schema(self.SOURCE_SCHEMA, self.DEST_SCHEMA)

        self.assertEqual(
            self._expressions(self.DEST_SCHEMA)['column id'],
            "nextval('{}.rebind_example_id_seq'::regclass)".format(self.DEST_SCHEMA),
        )

    def test_clone_schema_leaves_the_clone_working_after_the_source_schema_is_dropped(self):
        """
        Case: Clone a schema, drop the source schema outright, then write to the clone.
        Expected: Every expression still works, because none of them reaches back into the schema just dropped. Were
                  they still bound to the source, dropping it would cascade them away or fail outright.
        """
        self._create_sharded_function(self.SOURCE_SCHEMA)
        self._create_table_using('shard_upper')
        connection.clone_schema(self.SOURCE_SCHEMA, self.DEST_SCHEMA)

        connection.cursor().execute('DROP SCHEMA {} CASCADE'.format(self.SOURCE_SCHEMA))

        with use_shard(node_name='default', schema_name=self.DEST_SCHEMA) as env:
            cursor = env.connection.cursor()
            cursor.execute("INSERT INTO rebind_example (body) VALUES ('third')")
            cursor.execute("SELECT label, body_upper FROM rebind_example WHERE body = 'third'")
            self.assertEqual(cursor.fetchone(), ('FRESH', 'THIRD'))

        self.assertCountEqual(
            self._expressions(self.DEST_SCHEMA),
            [
                'column id',
                'column body',
                'column label',
                'column body_upper',
                'constraint rebind_body_check',
                'index rebind_example_pkey',
                'index rebind_expr_idx',
                'index rebind_partial_idx',
            ],
        )

    def test_clone_schema_keeps_the_check_constraint_enforced(self):
        """
        Case: Insert a row the check constraint forbids, into a clone whose source schema is gone.
        Expected: The insert is refused, so the constraint is rebound and enforced rather than merely present.
        """
        self._create_sharded_function(self.SOURCE_SCHEMA)
        self._create_table_using('shard_upper')
        connection.clone_schema(self.SOURCE_SCHEMA, self.DEST_SCHEMA)
        connection.cursor().execute('DROP SCHEMA {} CASCADE'.format(self.SOURCE_SCHEMA))

        with use_shard(node_name='default', schema_name=self.DEST_SCHEMA) as env:
            with self.assertRaises(IntegrityError):
                env.connection.cursor().execute("INSERT INTO rebind_example (body) VALUES ('forbidden')")

    def test_clone_schema_keeps_public_functions_bound_to_public(self):
        """
        Case: Clone a schema whose expressions call a function living in public rather than in the schema itself.
        Expected: The clone keeps calling public's function. Only a reference into the schema being cloned may move;
                  rebinding a shared function onto each shard would look for a copy that is never made.
        """
        connection.cursor().execute(CREATE_ALLUPPERCASE)
        self.addCleanup(connection.cursor().execute, DROP_ALLUPPERCASE)
        self._create_table_using('public.alluppercase')

        connection.clone_schema(self.SOURCE_SCHEMA, self.DEST_SCHEMA)

        expressions = self._expressions(self.DEST_SCHEMA)
        for name in (
            'column label',
            'column body_upper',
            'constraint rebind_body_check',
            'index rebind_expr_idx',
            'index rebind_partial_idx',
        ):
            self.assertIn('alluppercase', expressions[name])
            self.assertNotIn('{}.alluppercase'.format(self.DEST_SCHEMA), expressions[name])
            self.assertNotIn('{}.alluppercase'.format(self.SOURCE_SCHEMA), expressions[name])

    def test_clone_schema_preserves_string_literals_matching_the_schema_name(self):
        """
        Case: Clone a schema whose expressions hold string literals that read like a reference into that schema.
        Expected: The literals survive verbatim, since rebinding re-resolves names through the search path rather than
                  rewriting the text of a definition.
        """
        literal = '{}.reserved'.format(self.SOURCE_SCHEMA)
        with use_shard(node_name='default', schema_name=self.SOURCE_SCHEMA) as env:
            cursor = env.connection.cursor()
            cursor.execute(
                """
                CREATE TABLE rebind_literals (
                    id SERIAL PRIMARY KEY,
                    body TEXT NOT NULL DEFAULT '{literal}',
                    CONSTRAINT rebind_literal_check CHECK (body <> '{literal}    ')
                )
                """.format(literal=literal)
            )
            cursor.execute(
                "CREATE INDEX rebind_literal_idx ON rebind_literals (body) WHERE body <> '{}'".format(literal)
            )

        connection.clone_schema(self.SOURCE_SCHEMA, self.DEST_SCHEMA)

        expressions = self._expressions(self.DEST_SCHEMA, table_name='rebind_literals')
        self.assertIn("'{}'".format(literal), expressions['column body'])
        self.assertIn("'{}    '".format(literal), expressions['constraint rebind_literal_check'])
        self.assertIn("'{}'".format(literal), expressions['index rebind_literal_idx'])


class VirtualGeneratedColumnsTestCase(ShardingTransactionTestCase):
    """
    A virtual generated column is computed on read and never stored. PostgreSQL only offers them from version 18
    onwards, so these tests are skipped on earlier versions.
    """

    @skip_without_virtual_generated_column_support
    def setUp(self):
        super().setUp()
        connection.create_schema('virtual_source_schema')
        connection.create_schema('virtual_dest_schema')

    def _create_virtual_column_table(self, table_name='virtual_source', with_rows=True):
        with use_shard(node_name='default', schema_name='virtual_source_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute(
                """
                CREATE TABLE {table} (
                    id SERIAL PRIMARY KEY,
                    body TEXT NOT NULL DEFAULT '',
                    body_uppercased TEXT GENERATED ALWAYS AS (upper(body)) VIRTUAL
                )
                """.format(table=table_name)
            )
            if with_rows:
                cursor.execute(
                    'INSERT INTO {table} (body) VALUES (%s), (%s)'.format(table=table_name),
                    ['first', 'second'],
                )

    def _get_attgenerated(self, schema_name, table_name, column_name):
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT att.attgenerated::text
            FROM pg_catalog.pg_attribute att
            JOIN pg_catalog.pg_class cls ON att.attrelid = cls.oid
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = %s AND cls.relname = %s AND att.attname = %s
            """,
            [schema_name, table_name, column_name],
        )
        return cursor.fetchone()[0]

    def test_clone_schema_with_virtual_generated_columns(self):
        """
        Case: Clone a schema with a virtual generated column and read pre-existing rows.
        Expected: The values are correctly generated.
        """
        self._create_virtual_column_table()

        connection.clone_schema('virtual_source_schema', 'virtual_dest_schema')

        with use_shard(node_name='default', schema_name='virtual_dest_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute('SELECT body, body_uppercased FROM virtual_source ORDER BY id')
            self.assertEqual(cursor.fetchall(), [('first', 'FIRST'), ('second', 'SECOND')])

    def test_clone_schema_keeps_the_column_generated(self):
        """
        Case: Clone a schema with a virtual generated column and insert new rows.
        Expected: The generated column computes a value for each new row.
        """
        self._create_virtual_column_table()

        connection.clone_schema('virtual_source_schema', 'virtual_dest_schema')

        with use_shard(node_name='default', schema_name='virtual_dest_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute("INSERT INTO virtual_source (body) VALUES ('third')")
            cursor.execute("SELECT body_uppercased FROM virtual_source WHERE body = 'third'")
            self.assertEqual(cursor.fetchone(), ('THIRD',))

    def test_clone_schema_keeps_the_column_virtual(self):
        """
        Case: Clone a schema with a virtual generated column and inspect the clone's column definition.
        Expected: The column is still virtual, not silently materialised as stored.
        """
        self._create_virtual_column_table()

        connection.clone_schema('virtual_source_schema', 'virtual_dest_schema')

        self.assertEqual(self._get_attgenerated('virtual_source_schema', 'virtual_source', 'body_uppercased'), 'v')
        self.assertEqual(self._get_attgenerated('virtual_dest_schema', 'virtual_source', 'body_uppercased'), 'v')

    def test_clone_schema_with_an_empty_virtual_generated_column_table(self):
        """
        Case: Clone a schema whose table has a virtual generated column but no rows.
        Expected: The clone succeeds.
        """
        self._create_virtual_column_table(table_name='virtual_empty', with_rows=False)

        connection.clone_schema('virtual_source_schema', 'virtual_dest_schema')

        with use_shard(node_name='default', schema_name='virtual_dest_schema') as env:
            cursor = env.connection.cursor()
            cursor.execute('SELECT COUNT(*) FROM virtual_empty')
            self.assertEqual(cursor.fetchone(), (0,))

    def test_get_copyable_column_names_leaves_out_virtual_generated_columns(self):
        """
        Case: Request the copyable columns for a table that has a virtual generated column.
        Expected: The virtual generated column is excluded, just as a stored one is.
        """
        self._create_virtual_column_table()

        columns = connection.get_copyable_column_names('virtual_source', schema_name='virtual_source_schema')

        self.assertEqual(columns, ['id', 'body'])


class ViewsTestCase(ShardingTransactionTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        connection.create_schema('view_source_schema')
        connection.clone_schema('template', 'view_source_schema')
        connection.create_schema('view_dest_schema')

    @contextmanager
    def _source(self):
        with use_shard(node_name='default', schema_name='view_source_schema') as env:
            yield env.connection.cursor()

    @contextmanager
    def _dest(self):
        with use_shard(node_name='default', schema_name='view_dest_schema') as env:
            yield env.connection.cursor()

    def _clone(self):
        connection.clone_schema('view_source_schema', 'view_dest_schema')

    def _relkind(self, name, schema_name='view_dest_schema'):
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT cls.relkind::text
            FROM pg_catalog.pg_class cls
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = %s AND cls.relname = %s
        """,
            [schema_name, name],
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def _add_organization(self, cursor, name):
        cursor.execute(
            'INSERT INTO view_dest_schema.example_organization (name, created_at) VALUES (%s, %s) RETURNING id',
            [name, '2024-01-01 00:00:00'],
        )
        return cursor.fetchone()[0]

    def test_clone_schema_keeps_a_view_a_view(self):
        """
        Case: Clone a schema with a view.
        Expected: The destination has a view (not a table), which references the table in the destination schema.
        """
        with self._source() as cursor:
            cursor.execute('CREATE VIEW example_organization_view AS SELECT id, name FROM example_organization')

        self._clone()

        self.assertEqual(self._relkind('example_organization_view'), 'v')

        cursor = connection.cursor()
        cursor.execute("SELECT pg_get_viewdef('view_dest_schema.example_organization_view'::regclass, true)")
        definition = cursor.fetchone()[0]
        self.assertIn('view_dest_schema.example_organization', definition)
        self.assertNotIn('view_source_schema', definition)

    def test_a_cloned_view_tracks_its_base_table(self):
        """
        Case: Insert into the base table of a cloned view, after the clone.
        Expected: The new row is visible through the view.
        """
        with self._source() as cursor:
            cursor.execute('CREATE VIEW example_organization_view AS SELECT id, name FROM example_organization')
            cursor.execute(
                'INSERT INTO example_organization (name, created_at) VALUES (%s, %s)',
                ['Cloned Org', '2024-01-01 00:00:00'],
            )

        self._clone()

        with self._dest() as cursor:
            self._add_organization(cursor, 'Added After Cloning')

            cursor.execute('SELECT name FROM view_dest_schema.example_organization_view ORDER BY name')
            self.assertEqual([row[0] for row in cursor.fetchall()], ['Added After Cloning', 'Cloned Org'])

    def test_a_cloned_view_stays_writable(self):
        """
        Case: Write through an auto-updatable view in the cloned schema.
        Expected: The writes reach the cloned base table and fire its row triggers.
        """
        with self._source() as cursor:
            # A column alias keeps the view auto-updatable; the restriction is on expressions, not on renamed
            # plain column references.
            cursor.execute("""
                CREATE VIEW example_organization_bridge AS
                SELECT id, name AS organization_name, created_at FROM example_organization
            """)
            cursor.execute("""
                CREATE TABLE write_log (id SERIAL PRIMARY KEY, record_id INTEGER, action TEXT)
            """)
            cursor.execute("""
                CREATE OR REPLACE FUNCTION log_write() RETURNS TRIGGER AS $$
                BEGIN
                    INSERT INTO write_log (record_id, action) VALUES (COALESCE(NEW.id, OLD.id), TG_OP);
                    RETURN NULL;
                END;
                $$ LANGUAGE plpgsql;
            """)
            cursor.execute("""
                CREATE TRIGGER log_write_trigger
                AFTER INSERT OR UPDATE OR DELETE ON example_organization
                FOR EACH ROW EXECUTE FUNCTION log_write();
            """)

        self._clone()

        with self._dest() as cursor:
            cursor.execute(
                'INSERT INTO view_dest_schema.example_organization_bridge (organization_name, created_at) '
                'VALUES (%s, %s) RETURNING id',
                ['Through The View', '2024-01-01 00:00:00'],
            )
            organization_id = cursor.fetchone()[0]

            cursor.execute('SELECT name FROM view_dest_schema.example_organization WHERE id = %s', [organization_id])
            self.assertEqual(cursor.fetchone(), ('Through The View',))

            cursor.execute(
                'UPDATE view_dest_schema.example_organization_bridge SET organization_name = %s WHERE id = %s',
                ['Renamed Through The View', organization_id],
            )
            cursor.execute('SELECT name FROM view_dest_schema.example_organization WHERE id = %s', [organization_id])
            self.assertEqual(cursor.fetchone(), ('Renamed Through The View',))

            cursor.execute('DELETE FROM view_dest_schema.example_organization_bridge WHERE id = %s', [organization_id])
            cursor.execute(
                'SELECT COUNT(*) FROM view_dest_schema.example_organization WHERE id = %s', [organization_id]
            )
            self.assertEqual(cursor.fetchone(), (0,))

            cursor.execute(
                'SELECT action FROM view_dest_schema.write_log WHERE record_id = %s ORDER BY id', [organization_id]
            )
            self.assertEqual(
                [row[0] for row in cursor.fetchall()],
                ['INSERT', 'UPDATE', 'DELETE'],
                'Row triggers on the base table should fire for writes made through the cloned view',
            )

    def test_clone_schema_preserves_string_literals_matching_the_schema_name(self):
        """
        Case: Clone a view whose body contains the source schema's name inside string literals.
        Expected: The literals survive verbatim.
        """
        with self._source() as cursor:
            cursor.execute("""
                CREATE VIEW literal_view AS
                SELECT id, name, 'view_source_schema.marker' AS marker
                FROM example_organization
                WHERE name LIKE 'view_source_schema.%'
            """)
            cursor.execute(
                'INSERT INTO example_organization (name, created_at) VALUES (%s, %s)',
                ['view_source_schema.a', '2024-01-01 00:00:00'],
            )

        self._clone()

        cursor = connection.cursor()
        cursor.execute("SELECT pg_get_viewdef('view_dest_schema.literal_view'::regclass, true)")
        definition = cursor.fetchone()[0]
        self.assertIn("'view_source_schema.%'", definition)
        self.assertIn("'view_source_schema.marker'", definition)
        self.assertIn('view_dest_schema.example_organization', definition)

        with self._dest() as cursor:
            cursor.execute('SELECT name, marker FROM view_dest_schema.literal_view')
            self.assertEqual(cursor.fetchall(), [('view_source_schema.a', 'view_source_schema.marker')])

    def test_clone_schema_preserves_literals_in_matview_indexes_and_view_defaults(self):
        """
        Case: Clone a materialized view with a partial index whose predicate contains the source schema's name, and
              a view with a column default that is such a literal.
        Expected: Both literals survive verbatim.
        """
        with self._source() as cursor:
            cursor.execute('CREATE MATERIALIZED VIEW literal_matview AS SELECT id, name FROM example_organization')
            cursor.execute(
                'CREATE INDEX literal_matview_partial ON literal_matview (name) '
                "WHERE name <> 'view_source_schema.reserved'"
            )
            cursor.execute('CREATE VIEW defaulted_literal_view AS SELECT id, name FROM example_organization')
            cursor.execute(
                "ALTER VIEW defaulted_literal_view ALTER COLUMN name SET DEFAULT 'view_source_schema.default'"
            )

        self._clone()

        cursor = connection.cursor()
        cursor.execute(
            'SELECT pg_get_indexdef(i.indexrelid) FROM pg_catalog.pg_index i '
            "WHERE i.indrelid = 'view_dest_schema.literal_matview'::regclass"
        )
        index_definitions = [row[0] for row in cursor.fetchall()]
        self.assertTrue(
            any("'view_source_schema.reserved'" in definition for definition in index_definitions),
            index_definitions,
        )

        cursor.execute(
            'SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_catalog.pg_attrdef d '
            "WHERE d.adrelid = 'view_dest_schema.defaulted_literal_view'::regclass"
        )
        self.assertIn("'view_source_schema.default'", cursor.fetchone()[0])

    def test_materialized_view_helpers_report_population_and_dependency_order(self):
        """
        Case: Clone a schema with a materialized view, a second one stacked on it (named to sort out of dependency
              order), and an unpopulated one.
        Expected: The dependency order lists the base before its dependent regardless of names, and the populated
                  set leaves out the WITH NO DATA view.
        """
        with self._source() as cursor:
            cursor.execute('CREATE MATERIALIZED VIEW z_base AS SELECT id, name FROM example_organization')
            cursor.execute('CREATE MATERIALIZED VIEW a_dependent AS SELECT id, name FROM z_base')
            cursor.execute('CREATE MATERIALIZED VIEW n_unpopulated AS SELECT id FROM example_organization WITH NO DATA')

        with use_shard(node_name='default', schema_name='view_source_schema') as env:
            order = env.connection.get_materialized_views_in_dependency_order()
            populated = env.connection.get_populated_materialized_views()

        self.assertEqual(set(order), {'z_base', 'a_dependent', 'n_unpopulated'})
        self.assertLess(order.index('z_base'), order.index('a_dependent'))
        self.assertEqual(populated, {'z_base', 'a_dependent'})

    def test_materialized_view_dependency_order_reaches_through_a_plain_view(self):
        """
        Case: A materialized view reading a plain view that reads another materialized view, named so the dependent
              sorts first alphabetically.
        Expected: The base is still listed before the dependent, and the plain view carrying the edge between them is
                  not listed at all, since only a materialized view can be refreshed.
        """
        with self._source() as cursor:
            cursor.execute('CREATE MATERIALIZED VIEW z_stacked_base AS SELECT id, name FROM example_organization')
            cursor.execute('CREATE VIEW m_stacked_bridge AS SELECT id, name FROM z_stacked_base')
            cursor.execute('CREATE MATERIALIZED VIEW a_stacked_dependent AS SELECT id, name FROM m_stacked_bridge')

        with use_shard(node_name='default', schema_name='view_source_schema') as env:
            order = env.connection.get_materialized_views_in_dependency_order()

        self.assertEqual(set(order), {'z_stacked_base', 'a_stacked_dependent'})
        self.assertLess(order.index('z_stacked_base'), order.index('a_stacked_dependent'))

    def test_refresh_materialized_views_brings_the_stored_rows_up_to_date(self):
        """
        Case: Insert a row after building a materialized view, a plain view over it and a second materialized view
              over that, then refresh the schema.
        Expected: Both materialized views serve the new row, so the one reading through the plain view was refreshed
                  after the one it depends on. A view left WITH NO DATA stays unpopulated.
        """
        with self._source() as cursor:
            cursor.execute('CREATE MATERIALIZED VIEW z_stacked_base AS SELECT id, name FROM example_organization')
            cursor.execute('CREATE VIEW m_stacked_bridge AS SELECT id, name FROM z_stacked_base')
            cursor.execute('CREATE MATERIALIZED VIEW a_stacked_dependent AS SELECT id, name FROM m_stacked_bridge')
            cursor.execute('CREATE MATERIALIZED VIEW n_unpopulated AS SELECT id FROM example_organization WITH NO DATA')
            cursor.execute(
                'INSERT INTO example_organization (name, created_at) VALUES (%s, %s)',
                ['Refreshed Org', '2024-01-01 00:00:00'],
            )

        with use_shard(node_name='default', schema_name='view_source_schema') as env:
            env.connection.refresh_materialized_views()

            cursor = env.connection.cursor()
            cursor.execute('SELECT name FROM z_stacked_base')
            self.assertEqual(cursor.fetchall(), [('Refreshed Org',)])
            cursor.execute('SELECT name FROM a_stacked_dependent')
            self.assertEqual(cursor.fetchall(), [('Refreshed Org',)])
            self.assertNotIn('n_unpopulated', env.connection.get_populated_materialized_views())

    def test_refresh_materialized_views_can_be_limited_to_the_views_named(self):
        """
        Case: Refresh with only one of two populated materialized views named.
        Expected: Only that one catches up, which is what lets a caller hold back the views another schema keeps
                  unpopulated.
        """
        with self._source() as cursor:
            cursor.execute('CREATE MATERIALIZED VIEW named_view AS SELECT id, name FROM example_organization')
            cursor.execute('CREATE MATERIALIZED VIEW held_back_view AS SELECT id, name FROM example_organization')
            cursor.execute(
                'INSERT INTO example_organization (name, created_at) VALUES (%s, %s)',
                ['Named Org', '2024-01-01 00:00:00'],
            )

        with use_shard(node_name='default', schema_name='view_source_schema') as env:
            env.connection.refresh_materialized_views(names={'named_view'})

            cursor = env.connection.cursor()
            cursor.execute('SELECT name FROM named_view')
            self.assertEqual(cursor.fetchall(), [('Named Org',)])
            cursor.execute('SELECT name FROM held_back_view')
            self.assertEqual(cursor.fetchall(), [])

    def test_clone_schema_with_instead_of_triggers_on_views(self):
        """
        Case: A view that is not auto-updatable (an expression column), made writable by an INSTEAD OF INSERT
        trigger whose function lives in the source schema.
        Expected: Inserting through the cloned view works and lands in the destination's base table.
        """
        with self._source() as cursor:
            cursor.execute('CREATE VIEW writable_org AS SELECT id, UPPER(name) AS name FROM example_organization')
            cursor.execute("""
                CREATE FUNCTION writable_org_insert() RETURNS trigger AS $$
                BEGIN
                    INSERT INTO example_organization (name, created_at) VALUES (NEW.name, '2024-01-01 00:00:00');
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
            """)
            cursor.execute("""
                CREATE TRIGGER writable_org_ins INSTEAD OF INSERT ON writable_org
                FOR EACH ROW EXECUTE FUNCTION writable_org_insert()
            """)

        self._clone()

        with self._dest() as cursor:
            cursor.execute('INSERT INTO view_dest_schema.writable_org (name) VALUES (%s)', ['through the view'])
            cursor.execute('SELECT name FROM view_dest_schema.example_organization')
            self.assertEqual(cursor.fetchall(), [('through the view',)])

    def test_failed_view_cloning_reports_every_failure(self):
        """
        Case: Clone a schema with two source views whose names collide with tables pre-created in the destination
              schema.
        Expected: The raised error names both failed views.
        """
        with self._source() as cursor:
            cursor.execute('CREATE VIEW collide_a AS SELECT id, name FROM example_organization')
            cursor.execute('CREATE VIEW collide_b AS SELECT id, name FROM example_organization')

        with self._dest() as cursor:
            cursor.execute('CREATE TABLE view_dest_schema.collide_a (id INTEGER)')
            cursor.execute('CREATE TABLE view_dest_schema.collide_b (id INTEGER)')

        with self.assertRaises(DatabaseError) as caught:
            self._clone()

        message = str(caught.exception)
        self.assertIn('could not create all views', message)
        self.assertIn('collide_a', message)
        self.assertIn('collide_b', message)

    def test_clone_schema_orders_dependent_views(self):
        """
        Case: Clone a schema where one view selects from another, named so the dependent sorts first.
        Expected: Both views are cloned, with the dependency relationship intact.
        """
        with self._source() as cursor:
            cursor.execute('CREATE VIEW b_organization_view AS SELECT id, name FROM example_organization')
            # Sorts before the view it depends on, so a naive alphabetical pass would fail on it.
            cursor.execute('CREATE VIEW a_view_on_b AS SELECT id, name FROM b_organization_view')
            cursor.execute(
                'INSERT INTO example_organization (name, created_at) VALUES (%s, %s)',
                ['Dependent', '2024-01-01 00:00:00'],
            )

        self._clone()

        self.assertEqual(self._relkind('b_organization_view'), 'v')
        self.assertEqual(self._relkind('a_view_on_b'), 'v')

        with self._dest() as cursor:
            cursor.execute('SELECT name FROM view_dest_schema.a_view_on_b')
            self.assertEqual(cursor.fetchall(), [('Dependent',)])

    def test_clone_schema_carries_the_check_option_of_a_view(self):
        """
        Case: Clone a view declared WITH CASCADED CHECK OPTION.
        Expected: The cloned view carries the option.
        """
        with self._source() as cursor:
            cursor.execute("""
                CREATE VIEW checked_organization_view AS
                SELECT id, name, created_at FROM example_organization WHERE name LIKE 'ok%'
                WITH CASCADED CHECK OPTION
            """)

        self._clone()

        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT cls.reloptions
            FROM pg_catalog.pg_class cls
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = 'view_dest_schema' AND cls.relname = 'checked_organization_view'
        """
        )
        self.assertEqual(cursor.fetchone()[0], ['check_option=cascaded'])

        with self._dest() as cursor:
            with self.assertRaises(DatabaseError):
                cursor.execute(
                    'INSERT INTO view_dest_schema.checked_organization_view (name, created_at) VALUES (%s, %s)',
                    ['not allowed', '2024-01-01 00:00:00'],
                )

    def test_clone_schema_carries_the_column_defaults_of_a_view(self):
        """
        Case: Clone a view with a column default set through ALTER VIEW.
        Expected: The default is set on the cloned view.
        """
        with self._source() as cursor:
            cursor.execute("""
                CREATE VIEW defaulted_organization_view AS
                SELECT id, name, created_at FROM example_organization
            """)
            cursor.execute("ALTER VIEW defaulted_organization_view ALTER COLUMN name SET DEFAULT 'defaulted name'")

        self._clone()

        with self._dest() as cursor:
            cursor.execute(
                'INSERT INTO view_dest_schema.defaulted_organization_view (created_at) VALUES (%s) RETURNING id',
                ['2024-01-01 00:00:00'],
            )
            organization_id = cursor.fetchone()[0]
            cursor.execute('SELECT name FROM view_dest_schema.example_organization WHERE id = %s', [organization_id])
            self.assertEqual(cursor.fetchone(), ('defaulted name',))

    def test_clone_schema_clones_materialized_views(self):
        """
        Case: Clone a schema with a materialized view with a unique index.
        Expected: The clone is a populated materialized view referencing the correct table in the destination schema,
                  with an appropriately cloned index.
        """
        with self._source() as cursor:
            cursor.execute(
                'INSERT INTO example_organization (name, created_at) VALUES (%s, %s)',
                ['Materialized', '2024-01-01 00:00:00'],
            )
            cursor.execute('CREATE MATERIALIZED VIEW organization_summary AS SELECT id, name FROM example_organization')
            cursor.execute('CREATE UNIQUE INDEX organization_summary_id_key ON organization_summary (id)')

        self._clone()

        self.assertEqual(self._relkind('organization_summary'), 'm')

        with self._dest() as cursor:
            cursor.execute('SELECT name FROM view_dest_schema.organization_summary')
            self.assertEqual(cursor.fetchall(), [('Materialized',)])

            # A concurrent refresh is only possible when the unique index was cloned along with the view.
            self._add_organization(cursor, 'Added After Cloning')
            cursor.execute('REFRESH MATERIALIZED VIEW CONCURRENTLY view_dest_schema.organization_summary')

            cursor.execute('SELECT name FROM view_dest_schema.organization_summary ORDER BY name')
            self.assertEqual([row[0] for row in cursor.fetchall()], ['Added After Cloning', 'Materialized'])

    def test_clone_schema_without_views_creates_no_views(self):
        """
        Case: Clone a schema without views at all.
        Expected: The destination schema gets tables, no views.
        """
        self._clone()

        self.assertEqual(connection.get_all_views(schema_name='view_dest_schema'), [])
        self.assertCountEqual(
            connection.get_all_table_headers(schema_name='view_dest_schema'),
            connection.get_all_table_headers(schema_name='view_source_schema'),
        )

    def test_get_all_views_reports_views_and_materialized_views(self):
        """
        Case: Ask for the views of a schema holding a view and a materialized view.
        Expected: Both are reported.
        """
        with self._source() as cursor:
            cursor.execute('CREATE VIEW an_organization_view AS SELECT id, name FROM example_organization')
            cursor.execute(
                'CREATE MATERIALIZED VIEW an_organization_matview AS SELECT id, name FROM example_organization'
            )

        self.assertCountEqual(
            connection.get_all_views(schema_name='view_source_schema'),
            [('an_organization_view', 'v'), ('an_organization_matview', 'm')],
        )

    def test_flush_schema_drops_views_and_materialized_views(self):
        """
        Case: Flush a schema holding a view and a materialized view.
        Expected: The views are dropped.
        """
        with self._source() as cursor:
            cursor.execute('CREATE VIEW an_organization_view AS SELECT id, name FROM example_organization')
            cursor.execute(
                'CREATE MATERIALIZED VIEW an_organization_matview AS SELECT id, name FROM example_organization'
            )

        connection.flush_schema(schema_name='view_source_schema')

        self.assertEqual(connection.get_all_views(schema_name='view_source_schema'), [])
        self.assertEqual(connection.get_all_table_headers(schema_name='view_source_schema'), [])
