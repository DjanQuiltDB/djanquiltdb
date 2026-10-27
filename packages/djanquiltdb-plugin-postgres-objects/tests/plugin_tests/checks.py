from unittest import mock

from django.apps import apps
from django.core.checks.registry import registry
from django.db.migrations.writer import MigrationWriter
from django.test import SimpleTestCase, override_settings
from djanquiltdb import PRIMARY_NODE, ShardingMode, single_node_unsharded
from djanquiltdb.decorators import (
    override_sharding_setting,
    public_view,
    single_node_unsharded_function,
    single_node_unsharded_view,
)
from postgres_objects import Function, MaterializedView, View

from djanquiltdb_plugin_postgres_objects.checks import check_declarations_have_a_placement, has_placement_decorator
from example import db_views, functions


def ids_and_objects(messages):
    return [(message.id, message.obj) for message in messages]


class DeclarationsHaveAPlacementCheckTestCase(SimpleTestCase):
    def test_check_is_registered(self):
        """
        Case: The app registry has been readied, so install() has run, as it has for every test run.
        Expected: The check is part of Django's check registry.
        """
        self.assertIn(check_declarations_have_a_placement, registry.get_checks())

    def test_the_declarations_of_the_example_app(self):
        """
        Case: The check runs for the example app, which declares the function Unannotated and the view Unannotated
              without a placement decorator, next to declarations with one.
        Expected: djanquiltdb_plugin_postgres_objects.E001 for both Unannotated declarations, and nothing else.
        """
        messages = check_declarations_have_a_placement([apps.get_app_config('example')])

        self.assertCountEqual(
            ids_and_objects(messages),
            [
                ('djanquiltdb_plugin_postgres_objects.E001', functions.Unannotated),
                ('djanquiltdb_plugin_postgres_objects.E001', db_views.Unannotated),
            ],
        )
        self.assertIn('example.Unannotated has no placement decorator', messages[0].msg)

    @override_settings(POSTGRES_OBJECTS={'FUNCTIONS_MODULE_PATH': 'functions'})
    def test_only_the_kinds_of_declarations_that_the_project_manages_are_checked(self):
        """
        Case: The check runs for the example app while POSTGRES_OBJECTS manages functions only.
        Expected: djanquiltdb_plugin_postgres_objects.E001 for the function Unannotated only, and not for the view.
        """
        messages = check_declarations_have_a_placement([apps.get_app_config('example')])

        self.assertEqual(
            ids_and_objects(messages), [('djanquiltdb_plugin_postgres_objects.E001', functions.Unannotated)]
        )

    def test_only_the_declarations_of_the_given_apps_are_checked(self):
        """
        Case: The check runs for the declared app only, while the example app has declarations without a placement
              decorator.
        Expected: No message for the declarations of the example app.
        """
        messages = check_declarations_have_a_placement([apps.get_app_config('declared')])

        self.assertNotIn(functions.Unannotated, [message.obj for message in messages])

    def test_a_declaration_on_a_single_node(self):
        """
        Case: The check runs for a function on the node 'other' and a view on PRIMARY_NODE, while PRIMARY_DB_ALIAS is
              'default'.
        Expected: djanquiltdb_plugin_postgres_objects.W001 for each of them, naming the resolved node.
        """

        @single_node_unsharded_function(node='other')
        class OnOther(Function):
            app_label = 'example'
            returns = 'INTEGER'
            body = 'BEGIN RETURN 1; END;'

        @single_node_unsharded_view(node=PRIMARY_NODE)
        class OnThePrimaryNode(View):
            app_label = 'example'
            sql = 'SELECT 1 AS id'

        with mock.patch(
            'djanquiltdb_plugin_postgres_objects.checks._declarations', return_value=[OnOther, OnThePrimaryNode]
        ):
            messages = check_declarations_have_a_placement(None)

        self.assertEqual(
            ids_and_objects(messages),
            [
                ('djanquiltdb_plugin_postgres_objects.W001', OnOther),
                ('djanquiltdb_plugin_postgres_objects.W001', OnThePrimaryNode),
            ],
        )
        self.assertIn("on the single node 'other'", messages[0].msg)
        self.assertIn("on the single node 'default'", messages[1].msg)

    def test_a_declaration_on_a_node_that_is_not_in_databases(self):
        """
        Case: The check runs for a view on the node 'defualt', which is not in DATABASES.
        Expected: djanquiltdb_plugin_postgres_objects.W001, and djanquiltdb_plugin_postgres_objects.E002 naming the
                  node, as core reports a model on such a node with djanquiltdb.E006.
        """

        @single_node_unsharded_view(node='defualt')
        class OnAMisspelledNode(View):
            app_label = 'example'
            sql = 'SELECT 1 AS id'

        with mock.patch('djanquiltdb_plugin_postgres_objects.checks._declarations', return_value=[OnAMisspelledNode]):
            messages = check_declarations_have_a_placement(None)

        self.assertEqual(
            ids_and_objects(messages),
            [
                ('djanquiltdb_plugin_postgres_objects.W001', OnAMisspelledNode),
                ('djanquiltdb_plugin_postgres_objects.E002', OnAMisspelledNode),
            ],
        )
        self.assertIn("is on the node 'defualt', which is not in DATABASES", messages[1].msg)

    def test_router_hints_on_a_single_node_without_a_node(self):
        """
        Case: The check runs for a view whose router_hints are written by hand with SINGLE_NODE_UNSHARDED and without a
              node, and for a view whose router_hints name the node with single_node_unsharded(node='other').
        Expected: djanquiltdb_plugin_postgres_objects.E003 for the first view, and W001 naming 'other' for the second.
        """

        class WithoutANode(View):
            app_label = 'example'
            sql = 'SELECT 1 AS id'
            router_hints = {'sharding_mode': ShardingMode.SINGLE_NODE_UNSHARDED}

        class WithTheNodeInTheMode(View):
            app_label = 'example'
            sql = 'SELECT 1 AS id'
            router_hints = {'sharding_mode': single_node_unsharded(node='other')}

        with mock.patch(
            'djanquiltdb_plugin_postgres_objects.checks._declarations',
            return_value=[WithoutANode, WithTheNodeInTheMode],
        ):
            messages = check_declarations_have_a_placement(None)

        self.assertEqual(
            ids_and_objects(messages),
            [
                ('djanquiltdb_plugin_postgres_objects.E003', WithoutANode),
                ('djanquiltdb_plugin_postgres_objects.W001', WithTheNodeInTheMode),
            ],
        )
        self.assertIn("on the single node 'other'", messages[1].msg)


class SingleNodeDecoratorTestCase(SimpleTestCase):
    def test_the_hints_name_the_mode_and_the_node(self):
        """
        Case: Decorate a function with @single_node_unsharded_function(node='other').
        Expected: Its router hints have the mode SINGLE_NODE_UNSHARDED and the node 'other'.
        """

        @single_node_unsharded_function(node='other')
        class OnOther(Function):
            app_label = 'example'
            returns = 'INTEGER'
            body = 'BEGIN RETURN 1; END;'

        self.assertEqual(OnOther.router_hints, {'sharding_mode': ShardingMode.SINGLE_NODE_UNSHARDED, 'node': 'other'})

    def test_a_migration_can_write_the_hints(self):
        """
        Case: Serialize the router hints of a view on PRIMARY_NODE, as makemigrations writes them into a migration.
        Expected: Both enum members are written by name, with an import of djanquiltdb.
        """

        @single_node_unsharded_view(node=PRIMARY_NODE)
        class OnThePrimaryNode(View):
            app_label = 'example'
            sql = 'SELECT 1 AS id'

        string, imports = MigrationWriter.serialize(OnThePrimaryNode.router_hints)

        self.assertEqual(
            string,
            "{'node': djanquiltdb.PrimaryNode['PRIMARY_NODE'], "
            "'sharding_mode': djanquiltdb.ShardingMode['SINGLE_NODE_UNSHARDED']}",
        )
        self.assertEqual(imports, {'import djanquiltdb'})

    def test_a_materialized_view_is_refreshed_on_its_node(self):
        """
        Case: Ask a materialized view on PRIMARY_NODE for the connection that it is refreshed on, while PRIMARY_DB_ALIAS
              is 'other'.
        Expected: 'other'.
        """

        @single_node_unsharded_view(node=PRIMARY_NODE)
        class StoredOnThePrimaryNode(MaterializedView):
            app_label = 'example'
            sql = 'SELECT 1 AS id'

        with override_sharding_setting('PRIMARY_DB_ALIAS', 'other'):
            self.assertEqual(StoredOnThePrimaryNode.db_for_refresh(), 'other')

    def test_a_node_that_is_not_an_alias(self):
        """
        Case: Call @single_node_unsharded_view with a node that is neither a string nor PRIMARY_NODE.
        Expected: TypeError.
        """
        with self.assertRaisesMessage(TypeError, 'node must be the alias of a node or PRIMARY_NODE, not 1.'):
            single_node_unsharded_view(node=1)


class HasPlacementDecoratorTestCase(SimpleTestCase):
    def test_a_subclass_of_a_decorated_declaration_has_one(self):
        """
        Case: Ask whether a view that subclasses a decorated view, and a view without a decorator, have a placement
              decorator.
        Expected: The subclass has one, through its parent, and the view without a decorator has none.
        """

        @public_view()
        class Parent(View):
            app_label = 'example'
            sql = 'SELECT 1 AS id'

        class Child(Parent):
            app_label = 'example'

        class Bare(View):
            app_label = 'example'
            sql = 'SELECT 1 AS id'

        self.assertTrue(has_placement_decorator(Child))
        self.assertFalse(has_placement_decorator(Bare))
