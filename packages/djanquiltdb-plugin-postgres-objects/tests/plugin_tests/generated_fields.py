"""
Stored generated columns on sharded models, for both of the fields that can declare one.
"""

from django.db import models
from django.db.migrations.state import ModelState, ProjectState
from django.db.models import F
from djanquiltdb.db import connection
from djanquiltdb.decorators import public_function, sharded_function
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.testing import ShardingTestCase, ShardingTransactionTestCase
from djanquiltdb.utils import create_schema_on_node, create_template_schema, get_template_name, use_shard
from postgres_objects import Function, GeneratedField
from postgres_objects.autodetector.recalculation import get_recalculations
from postgres_objects.operations import AddFunction, AlterFunction, RecalculateGeneratedField
from postgres_objects.registry import get_declared_objects

from example.functions import APP_LABEL, AllUppercase, ShardOnly
from plugin_tests.functions import SHARD_SCHEMA, FunctionShardingTestCase

MODEL_APP_LABEL = 'example'
MODEL_NAME = 'Cake'
FIELD_NAME = 'name_uppercased'

#: Where the suite's declarations live, relative to each app: the module
#: POSTGRES_OBJECTS['FUNCTIONS_MODULE_PATH'] names, since the example app declares them the documented way.
FUNCTIONS_MODULE_PATH = 'functions'

#: The table the operation is aimed at. It is named after the case rather than after example.Cake so that a test only
#: ever touches a table it created itself, while the model state still carries example.Cake's name. That name is what
#: the router resolves the sharding mode from, and example.Cake is the suite's sharded model.
TABLE_NAME = 'generated_column_cake'

OTHER_SHARD_SCHEMA = 'other_shard_schema'

#: The node the multi-node case reaches for, next to the 'default' one every other case uses.
OTHER_NODE = 'other'


def excited_public_declaration():
    """
    A second declaration of AllUppercase, PUBLIC like it, whose body computes something else.

    Built on call rather than declared at module level on purpose: it shares AllUppercase's name, and therefore its
    database name, so two module-level declarations of it would clash in the registry.
    """

    @public_function()
    class AllUppercaseExcited(Function):
        app_label = APP_LABEL
        name = AllUppercase.name
        arguments = 'input TEXT'
        returns = 'TEXT'
        volatility = 'IMMUTABLE'
        strict = True
        parallel = 'SAFE'
        body = """
            BEGIN
                RETURN UPPER(input) || '!';
            END;
        """

    return AllUppercaseExcited


class GeneratedFieldStateMixin:
    """
    Builds the migration state a RecalculateGeneratedField reads the column out of.
    """

    def state_with(self, field):
        state = ProjectState()
        state.add_model(
            ModelState(
                MODEL_APP_LABEL,
                MODEL_NAME,
                [
                    ('id', models.AutoField(primary_key=True)),
                    ('name', models.TextField()),
                    (FIELD_NAME, field),
                ],
                {'db_table': TABLE_NAME},
            )
        )
        return state

    def generated_field(self, field_class, declaration=None):
        return field_class(
            expression=(declaration or AllUppercase)(F('name')),
            output_field=models.TextField(),
            db_persist=True,
        )


class GeneratedFieldExpressionTestCase(GeneratedFieldStateMixin, ShardingTestCase):
    def test_django_declaration_can_be_used_as_a_generated_field_expression(self):
        """
        Case: Build a django.db.models.GeneratedField from a declared function.
        Expected: The expression names the function as the database has it.
        """
        field = self.generated_field(models.GeneratedField)

        self.assertEqual(field.expression.extra['function'], AllUppercase.resolved_db_name)
        self.assertEqual(AllUppercase.resolved_db_name, 'example_alluppercase')

    def test_recalculating_declaration_can_be_used_as_a_generated_field_expression(self):
        """
        Case: Build a postgres_objects.GeneratedField from a declared function.
        Expected: The expression names the function as the database has it.
        """
        field = self.generated_field(GeneratedField)

        self.assertEqual(field.expression.extra['function'], AllUppercase.resolved_db_name)
        self.assertEqual(AllUppercase.resolved_db_name, 'example_alluppercase')

    def test_recalculating_field_sees_through_a_placement_decorator(self):
        """
        Case: Ask a postgres_objects.GeneratedField which functions it depends on, for a decorated declaration.
        Expected: The declaration is still there (stock behavior unaffected).
        """
        field = self.generated_field(GeneratedField)

        self.assertIn(AllUppercase.resolved_db_name, field.referenced_function_names())

    def test_decorated_declaration_is_still_discovered_as_a_dependency(self):
        """
        Case: Resolve a postgres_objects.GeneratedField against the declarations collected from a module of them.
        Expected: The declaration the column calls is among them (stock behavior unaffected).
        """
        field = self.generated_field(GeneratedField)
        declared = {
            declaration.resolved_db_name.lower()
            for declaration in get_declared_objects(FUNCTIONS_MODULE_PATH, kind=Function).values()
        }

        self.assertIn(AllUppercase.resolved_db_name, declared)
        self.assertTrue(field.referenced_function_names() & declared)

    def test_django_generated_field_is_never_recalculated(self):
        """
        Case: A column declared with django.db.models.GeneratedField, whose function changed what it computes.
        Expected: No recalculation is written (stock behavior unaffected).
        """
        state = self.state_with(self.generated_field(models.GeneratedField))

        self.assertEqual(get_recalculations({AllUppercase.resolved_db_name}, state, state), {})

    def test_recalculating_generated_field_asks_for_a_rewrite(self):
        """
        Case: A column declared with postgres_objects.GeneratedField, whose function changed what it computes.
        Expected: A RecalculateGeneratedField is written for it, in the model's own app.
        """
        state = self.state_with(self.generated_field(GeneratedField))

        operations = get_recalculations({AllUppercase.resolved_db_name}, state, state)

        self.assertEqual([type(operation) for operation in operations[MODEL_APP_LABEL]], [RecalculateGeneratedField])


class RecalculationPlacementTestCase(ShardingTransactionTestCase):
    """
    RecalculateGeneratedField is the one operation of the library bound to a model rather than to a declared object,
    so it carries no sharding hints and DjanQuiltDB places it the way it places any model migration.
    """

    def setUp(self):
        super().setUp()
        # Empty schemas are enough: nothing is applied here, only asked where it would be allowed to run.
        create_template_schema(migrate=False)
        connection.create_schema(SHARD_SCHEMA)

    def allowed_in(self, model_name, schema_name):
        operation = RecalculateGeneratedField(model_name, FIELD_NAME)

        with use_shard(node_name='default', schema_name=schema_name) as env:
            with env.connection.schema_editor() as schema_editor:
                return operation.allowed(MODEL_APP_LABEL, schema_editor)

    def test_sharded_model_is_recalculated_on_the_template_and_the_shards(self):
        """
        Case: Ask where the recalculation of a column on example.Cake, a @sharded_model, is allowed to run.
        Expected: On the template and on a shard, and never in the public schema. Every shard is rewritten, since each
                  computes the column with whatever its own search path resolves the function to.
        """
        # Where the template is concerned this is as far as the suite goes, deliberately. A recalculation is only
        # observable through the rows it rewrites, and a template holds structure and no rows. Cloning it proves
        # nothing either: clone_schema copies rows without the generated columns, so a clone recomputes them on insert
        # and would read the new values whether or not the template had been rewritten.
        self.assertTrue(self.allowed_in(MODEL_NAME, get_template_name()))
        self.assertTrue(self.allowed_in(MODEL_NAME, SHARD_SCHEMA))
        self.assertFalse(self.allowed_in(MODEL_NAME, PUBLIC_SCHEMA_NAME))

    def test_public_model_is_recalculated_in_public_only(self):
        """
        Case: Ask where the recalculation of a column on example.CakeType, a @public_model, is allowed to run.
        Expected: In the public schema and never on a shard (i.e. a generated column is rewritten wherever its table
                  is).
        """
        self.assertTrue(self.allowed_in('CakeType', PUBLIC_SCHEMA_NAME))
        self.assertFalse(self.allowed_in('CakeType', SHARD_SCHEMA))


class RecalculationTestCase(GeneratedFieldStateMixin, FunctionShardingTestCase):
    def create_table(self, schema_name, function_name, node_name='default'):
        # Unqualified, so the table lands in the schema the connection creates in rather than in public.
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            env.connection.cursor().execute(
                """
                CREATE TABLE {table} (
                    id serial PRIMARY KEY,
                    name text,
                    {column} text GENERATED ALWAYS AS ({function}(name)) STORED
                )
                """.format(table=TABLE_NAME, column=FIELD_NAME, function=function_name)
            )

    def insert(self, schema_name, name, node_name='default'):
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            env.connection.cursor().execute('INSERT INTO {} (name) VALUES (%s)'.format(TABLE_NAME), [name])

    def stored_values(self, schema_name, node_name='default'):
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            cursor = env.connection.cursor()
            cursor.execute('SELECT {} FROM {} ORDER BY id'.format(FIELD_NAME, TABLE_NAME))
            return [value for (value,) in cursor.fetchall()]

    def recalculate(self, schema_name, declaration=None, node_name='default'):
        state = self.state_with(self.generated_field(GeneratedField, declaration))
        operation = RecalculateGeneratedField(MODEL_NAME, FIELD_NAME)

        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            with env.connection.schema_editor() as schema_editor:
                operation.database_forwards(MODEL_APP_LABEL, schema_editor, state, state)

    def test_shard_is_recomputed_with_the_changed_public_function(self):
        """
        Case: Change the body of a PUBLIC function a shard's stored generated column calls, then apply the
              recalculation against that shard.
        Expected: The stored values are what the new body computes.
        """
        excited = excited_public_declaration()

        self.apply(AddFunction(AllUppercase.definition, hints=AllUppercase.router_hints), PUBLIC_SCHEMA_NAME)
        self.create_table(SHARD_SCHEMA, AllUppercase.resolved_db_name)
        self.insert(SHARD_SCHEMA, 'cake')

        self.apply(
            AlterFunction(excited.definition, AllUppercase.definition, hints=AllUppercase.router_hints),
            PUBLIC_SCHEMA_NAME,
        )

        self.assertEqual(self.stored_values(SHARD_SCHEMA), ['CAKE'])

        self.recalculate(SHARD_SCHEMA)

        self.assertEqual(self.stored_values(SHARD_SCHEMA), ['CAKE!'])

    def test_each_shard_is_recomputed_with_the_changed_public_function(self):
        """
        Case: Two shards holding the same column, computed by the one PUBLIC function both of them reach, whose body
              changed once. Recalculated in both.
        Expected: Both shards store what the new body computes, and neither of them until it is recalculated itself.
                  One body change reaches every shard, but the rewrite that brings its rows up to date happens per
                  shard.
        """
        excited = excited_public_declaration()

        self.apply(AddFunction(AllUppercase.definition, hints=AllUppercase.router_hints), PUBLIC_SCHEMA_NAME)
        connection.create_schema(OTHER_SHARD_SCHEMA)

        for schema_name in (SHARD_SCHEMA, OTHER_SHARD_SCHEMA):
            self.create_table(schema_name, AllUppercase.resolved_db_name)
            self.insert(schema_name, 'cake')

        # Once, in public: there is only one copy of the function for both shards to compute with.
        self.apply(
            AlterFunction(excited.definition, AllUppercase.definition, hints=AllUppercase.router_hints),
            PUBLIC_SCHEMA_NAME,
        )

        self.assertEqual(self.stored_values(SHARD_SCHEMA), ['CAKE'])
        self.assertEqual(self.stored_values(OTHER_SHARD_SCHEMA), ['CAKE'])

        # One shard at a time, so that a rewrite reaching further than the schema it was applied to would show up as
        # the second shard catching up before it was asked to.
        self.recalculate(SHARD_SCHEMA)

        self.assertEqual(self.stored_values(SHARD_SCHEMA), ['CAKE!'])
        self.assertEqual(self.stored_values(OTHER_SHARD_SCHEMA), ['CAKE'])

        self.recalculate(OTHER_SHARD_SCHEMA)

        self.assertEqual(self.stored_values(OTHER_SHARD_SCHEMA), ['CAKE!'])

    def test_a_shard_is_recomputed_with_its_own_nodes_copy_of_a_public_function(self):
        """
        Case: A shard on each node, both holding the same column, with only the second node's copy of the PUBLIC
              function changed. Recalculated on both nodes.
        Expected: Each shard stores what the copy on its own node computes. The expression is never schema-qualified,
                  so a shard resolves the function through its own node's public schema.
        """
        excited = excited_public_declaration()
        self.addCleanup(self._drop_on_other_node, AllUppercase)
        create_schema_on_node(schema_name=SHARD_SCHEMA, node_name=OTHER_NODE, migrate=False)

        for node_name in ('default', OTHER_NODE):
            self.apply(
                AddFunction(AllUppercase.definition, hints=AllUppercase.router_hints),
                PUBLIC_SCHEMA_NAME,
                node_name=node_name,
            )
            self.create_table(SHARD_SCHEMA, AllUppercase.resolved_db_name, node_name=node_name)
            self.insert(SHARD_SCHEMA, 'cake', node_name=node_name)

        # Only the second node's copy starts computing something else.
        self.apply(
            AlterFunction(excited.definition, AllUppercase.definition, hints=AllUppercase.router_hints),
            PUBLIC_SCHEMA_NAME,
            node_name=OTHER_NODE,
        )

        self.assertEqual(self.stored_values(SHARD_SCHEMA, node_name=OTHER_NODE), ['CAKE'])

        for node_name in ('default', OTHER_NODE):
            self.recalculate(SHARD_SCHEMA, node_name=node_name)

        self.assertEqual(self.stored_values(SHARD_SCHEMA), ['CAKE'])
        self.assertEqual(self.stored_values(SHARD_SCHEMA, node_name=OTHER_NODE), ['CAKE!'])

    def test_each_shard_is_recomputed_with_its_own_copy_of_a_sharded_function(self):
        """
        Case: Two shards holding the same column, with a SHARDED function whose copy differs per shard, recalculated
              in both.
        Expected: Each shard stores what its own copy computes.
        """

        @sharded_function()
        class ShardOnlyExcited(Function):
            app_label = APP_LABEL
            name = ShardOnly.name
            arguments = 'input TEXT'
            returns = 'TEXT'
            volatility = 'IMMUTABLE'
            strict = True
            parallel = 'SAFE'
            body = """
                BEGIN
                    RETURN input || '!';
                END;
            """

        connection.create_schema(OTHER_SHARD_SCHEMA)

        for schema_name in (SHARD_SCHEMA, OTHER_SHARD_SCHEMA):
            self.apply(AddFunction(ShardOnly.definition, hints=ShardOnly.router_hints), schema_name)
            self.create_table(schema_name, ShardOnly.resolved_db_name)
            self.insert(schema_name, 'cake')

        # Only the second shard's copy starts computing something else.
        self.apply(
            AlterFunction(ShardOnlyExcited.definition, ShardOnly.definition, hints=ShardOnly.router_hints),
            OTHER_SHARD_SCHEMA,
        )

        self.assertEqual(self.stored_values(OTHER_SHARD_SCHEMA), ['cake'])

        for schema_name in (SHARD_SCHEMA, OTHER_SHARD_SCHEMA):
            self.recalculate(schema_name, declaration=ShardOnly)

        self.assertEqual(self.stored_values(SHARD_SCHEMA), ['cake'])
        self.assertEqual(self.stored_values(OTHER_SHARD_SCHEMA), ['cake!'])
