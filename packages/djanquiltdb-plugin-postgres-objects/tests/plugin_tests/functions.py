from django.db import DatabaseError
from django.db.models import F
from djanquiltdb import ShardingMode
from djanquiltdb.db import connection
from djanquiltdb.decorators import mirrored_function
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import use_shard
from postgres_objects import Function
from postgres_objects.operations import AddFunction, RemoveFunction

from example.functions import APP_LABEL, AllUppercase, ShardOnly, Unannotated

SHARD_SCHEMA = 'shard_schema'


class FunctionShardingTestCase(ShardingTransactionTestCase):
    """
    A function is not table data, so it survives a rolled back transaction. Tests subclassing this class run as
    transaction test cases and thus drop what they created.
    """

    DECLARATIONS = (AllUppercase, ShardOnly, Unannotated)

    def setUp(self):
        super().setUp()
        connection.create_schema(SHARD_SCHEMA)
        self.addCleanup(self._drop_declared_functions)

    def _drop_declared_functions(self):
        cursor = connection.cursor()
        for declaration in self.DECLARATIONS:
            cursor.execute('DROP FUNCTION IF EXISTS public.{} CASCADE;'.format(declaration.definition.drop_signature))

    def apply(self, operation, schema_name, node_name='default'):
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            with env.connection.schema_editor() as schema_editor:
                operation.database_forwards(APP_LABEL, schema_editor, None, None)

    def apply_everywhere(self, operation):
        for schema_name in (PUBLIC_SCHEMA_NAME, SHARD_SCHEMA):
            self.apply(operation, schema_name)

    def function_exists(self, declaration, schema_name, node_name='default'):
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            cursor = env.connection.cursor()
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM pg_catalog.pg_proc proc
                JOIN pg_catalog.pg_namespace nsp ON proc.pronamespace = nsp.oid
                WHERE proc.proname = %s AND nsp.nspname = %s
                """,
                [declaration.resolved_db_name, schema_name],
            )
            return cursor.fetchone()[0] == 1


class AnnotationTestCase(FunctionShardingTestCase):
    def test_the_decorator_sets_the_hints_the_operations_carry(self):
        """
        Case: Build an operation for an annotated declaration.
        Expected: It is hinted with the declaration's sharding mode.
        """
        operation = AddFunction(AllUppercase.definition, hints=AllUppercase.router_hints)

        self.assertEqual(operation.hints, {'sharding_mode': ShardingMode.PUBLIC})
        self.assertEqual(ShardOnly.router_hints, {'sharding_mode': ShardingMode.SHARDED})

    def test_every_sharding_mode_has_a_decorator(self):
        """
        Case: Annotate declarations with each of the three decorators.
        Expected: Each records the matching sharding mode.
        """

        @mirrored_function()
        class Mirrored(Function):
            app_label = APP_LABEL
            returns = 'TEXT'
            body = 'BEGIN RETURN 1; END;'

        self.assertEqual(Mirrored.router_hints, {'sharding_mode': ShardingMode.MIRRORED})

    def test_an_unannotated_declaration_defaults_to_public(self):
        """
        Case: A declaration that was never annotated, in a project that has this plugin installed.
        Expected: It is treated as PUBLIC.
        """
        self.assertEqual(Unannotated.router_hints, {'sharding_mode': ShardingMode.PUBLIC})

        self.apply_everywhere(AddFunction(Unannotated.definition, hints=Unannotated.router_hints))

        self.assertTrue(self.function_exists(Unannotated, PUBLIC_SCHEMA_NAME))
        self.assertFalse(self.function_exists(Unannotated, SHARD_SCHEMA))


class PlacementTestCase(FunctionShardingTestCase):
    def test_a_public_function_lands_on_the_public_schema_only(self):
        """
        Case: Apply a PUBLIC-annotated declaration against the public schema and a shard.
        Expected: It exists in public and not in the shard.
        """
        self.apply_everywhere(AddFunction(AllUppercase.definition, hints=AllUppercase.router_hints))

        self.assertTrue(self.function_exists(AllUppercase, PUBLIC_SCHEMA_NAME))
        self.assertFalse(self.function_exists(AllUppercase, SHARD_SCHEMA))

    def test_a_public_function_lands_on_the_public_schema_of_another_node(self):
        """
        Case: Apply a PUBLIC-annotated declaration against the public schema of the second node.
        Expected: That node gets it too.
        """
        self.addCleanup(self._drop_on_other_node, AllUppercase)

        self.apply(
            AddFunction(AllUppercase.definition, hints=AllUppercase.router_hints),
            PUBLIC_SCHEMA_NAME,
            node_name='other',
        )

        self.assertTrue(self.function_exists(AllUppercase, PUBLIC_SCHEMA_NAME, node_name='other'))

    def _drop_on_other_node(self, declaration):
        """
        The shared cleanup knows only about the default node, so a case that reaches the second one clears up itself.
        """
        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME) as env:
            env.connection.cursor().execute(
                'DROP FUNCTION IF EXISTS public.{} CASCADE;'.format(declaration.definition.drop_signature)
            )

    def test_a_sharded_function_lands_on_the_shard_only(self):
        """
        Case: Apply a SHARDED-annotated declaration against the public schema and a shard.
        Expected: It exists in the shard and not in public.
        """
        self.apply_everywhere(AddFunction(ShardOnly.definition, hints=ShardOnly.router_hints))

        self.assertFalse(self.function_exists(ShardOnly, PUBLIC_SCHEMA_NAME))
        self.assertTrue(self.function_exists(ShardOnly, SHARD_SCHEMA))

    def test_a_public_function_is_reachable_from_a_shard(self):
        """
        Case: Call a PUBLIC function unqualified while pointed at a shard.
        Expected: It resolves, because the shard's search path covers the public schema.
        """
        self.apply(AddFunction(AllUppercase.definition, hints=AllUppercase.router_hints), PUBLIC_SCHEMA_NAME)

        with use_shard(node_name='default', schema_name=SHARD_SCHEMA) as env:
            cursor = env.connection.cursor()
            cursor.execute('SELECT {}(%s)'.format(AllUppercase.resolved_db_name), ['cake'])
            self.assertEqual(cursor.fetchone()[0], 'CAKE')

    def test_removing_a_function_from_a_shard_spares_the_public_one(self):
        """
        Case: Create a function in public, then remove it from a shard with an explicit SHARDED hint.
        Expected: The public copy survives.
        """
        self.apply(AddFunction(AllUppercase.definition, hints=AllUppercase.router_hints), PUBLIC_SCHEMA_NAME)

        self.apply(
            RemoveFunction(AllUppercase.definition, hints={'sharding_mode': ShardingMode.SHARDED}),
            SHARD_SCHEMA,
        )

        self.assertTrue(self.function_exists(AllUppercase, PUBLIC_SCHEMA_NAME))


class GeneratedColumnTestCase(FunctionShardingTestCase):
    """
    The reason functions are managed at all: a stored generated column calls one, and binds to it.
    """

    def setUp(self):
        super().setUp()
        self.addCleanup(self._drop_table)

    def _drop_table(self):
        connection.cursor().execute('DROP TABLE IF EXISTS public.gen_dep_table CASCADE')

    def test_a_declaration_builds_a_generated_column_expression(self):
        """
        Case: Use an annotated declaration as a GeneratedField expression.
        Expected: It names the function unqualified, so a shard's search path resolves it to the public copy.
        """
        expression = AllUppercase(F('name'))

        self.assertEqual(expression.extra['function'], AllUppercase.resolved_db_name)

    def test_a_dependent_generated_column_blocks_the_drop(self):
        """
        Case: Remove a function that a stored generated column still calls.
        Expected: Postgres refuses. This is what the ordering of function migrations around the model migrations
                  exists to avoid.
        """
        self.apply(AddFunction(AllUppercase.definition, hints=AllUppercase.router_hints), PUBLIC_SCHEMA_NAME)
        connection.cursor().execute(
            """
            CREATE TABLE public.gen_dep_table (
                id serial PRIMARY KEY,
                name text,
                uppercased text GENERATED ALWAYS AS ({}(name)) STORED
            )
            """.format(AllUppercase.resolved_db_name)
        )

        with self.assertRaises(DatabaseError):
            self.apply(
                RemoveFunction(AllUppercase.definition, hints=AllUppercase.router_hints),
                PUBLIC_SCHEMA_NAME,
            )

    def test_a_sharded_function_reaches_a_newly_created_shard(self):
        """
        Case: Apply a SHARDED declaration to a schema, then clone that schema the way creating a shard does.
        Expected: The clone has the function too. A shard is created by cloning the template rather than by migrating
                  an empty schema, so this is how a sharded function reaches shards made after the migration ran.
        """
        self.apply(AddFunction(ShardOnly.definition, hints=ShardOnly.router_hints), SHARD_SCHEMA)
        connection.create_schema('cloned_schema')

        connection.clone_schema(SHARD_SCHEMA, 'cloned_schema')

        self.assertTrue(self.function_exists(ShardOnly, 'cloned_schema'))
