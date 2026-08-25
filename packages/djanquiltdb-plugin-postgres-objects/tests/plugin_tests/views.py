from django.conf import settings
from django.db import DEFAULT_DB_ALIAS
from djanquiltdb import ShardingMode
from djanquiltdb.db import connection
from djanquiltdb.decorators import mirrored_view, sharded_view
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import use_shard
from postgres_objects import View
from postgres_objects.operations import AddView, RefreshMaterializedView, RemoveView

from example.db_views import (
    APP_LABEL,
    SOURCE_TABLE,
    EmptyStored,
    MirroredStored,
    PublicOnly,
    PublicStored,
    ShardOnly,
    ShardStored,
    Unannotated,
)
from example.functions import AllUppercase

SHARD_SCHEMA = 'shard_schema'

#: A schema cloned from SHARD_SCHEMA the way creating a shard clones the template. The test case drops every schema the
#: test made, so it needs no cleanup of its own.
CLONE_SCHEMA = 'cloned_schema'


class ViewShardingTestCase(ShardingTransactionTestCase):
    """
    A view is not table data, so it survives a rolled back transaction. These run as transaction test cases and drop
    what they created.

    Every schema under test gets its own copy of the table the views read, since a view's body binds to the schema it
    was created in rather than resolving per query.
    """

    DECLARATIONS = (PublicOnly, ShardOnly, Unannotated)

    def setUp(self):
        super().setUp()
        connection.create_schema(SHARD_SCHEMA)
        self.addCleanup(self._drop_declared_views)

        for schema_name in (PUBLIC_SCHEMA_NAME, SHARD_SCHEMA):
            with use_shard(node_name='default', schema_name=schema_name) as env:
                env.connection.cursor().execute(
                    'CREATE TABLE {} (id serial PRIMARY KEY, name text)'.format(SOURCE_TABLE)
                )

    @staticmethod
    def _drop_statement(declaration, schema_name):
        """
        DROP VIEW refuses a materialized view and the other way round, so which one the declaration is picks the
        statement. CASCADE, unlike the library's own drop, because a case may have stacked something on top.
        """
        return 'DROP {materialized}VIEW IF EXISTS "{schema}".{name} CASCADE;'.format(
            materialized='MATERIALIZED ' if declaration.definition.materialized else '',
            schema=schema_name,
            name=declaration.resolved_db_name,
        )

    def _drop_declared_views(self):
        for schema_name in (PUBLIC_SCHEMA_NAME, SHARD_SCHEMA):
            cursor = connection.cursor()
            for declaration in self.DECLARATIONS:
                cursor.execute(self._drop_statement(declaration, schema_name))
            cursor.execute('DROP TABLE IF EXISTS "{}".{} CASCADE;'.format(schema_name, SOURCE_TABLE))

    def apply(self, operation, schema_name, node_name='default'):
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            with env.connection.schema_editor() as schema_editor:
                operation.database_forwards(APP_LABEL, schema_editor, None, None)

    def apply_everywhere(self, operation):
        for schema_name in (PUBLIC_SCHEMA_NAME, SHARD_SCHEMA):
            self.apply(operation, schema_name)

    def view_exists(self, declaration, schema_name):
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM pg_catalog.pg_class cls
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE cls.relname = %s AND nsp.nspname = %s AND cls.relkind IN ('v', 'm')
            """,
            [declaration.resolved_db_name, schema_name],
        )
        return cursor.fetchone()[0] == 1

    def relkind(self, declaration, schema_name, node_name='default'):
        """
        'v' for a view, 'm' for a materialized one, None when the schema has nothing by that name.
        """
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            cursor = env.connection.cursor()
            cursor.execute(
                """
                SELECT cls.relkind::text
                FROM pg_catalog.pg_class cls
                JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
                WHERE cls.relname = %s AND nsp.nspname = %s
                """,
                [declaration.resolved_db_name, schema_name],
            )
            result = cursor.fetchone()
            return result[0] if result else None

    def is_populated(self, declaration, schema_name):
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT cls.relispopulated
            FROM pg_catalog.pg_class cls
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE cls.relname = %s AND nsp.nspname = %s
            """,
            [declaration.resolved_db_name, schema_name],
        )
        return cursor.fetchone()[0]

    def unique_index_names(self, declaration, schema_name):
        """
        The names of the indexes on the declaration's relation, split by whether they are unique.
        """
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT cls.relname::text, idx.indisunique
            FROM pg_catalog.pg_index idx
            JOIN pg_catalog.pg_class cls ON idx.indexrelid = cls.oid
            JOIN pg_catalog.pg_class rel ON idx.indrelid = rel.oid
            JOIN pg_catalog.pg_namespace nsp ON rel.relnamespace = nsp.oid
            WHERE rel.relname = %s AND nsp.nspname = %s
            """,
            [declaration.resolved_db_name, schema_name],
        )
        rows = cursor.fetchall()
        return {name for name, unique in rows if unique}, {name for name, unique in rows if not unique}


class ViewAnnotationTestCase(ViewShardingTestCase):
    def test_the_decorator_sets_the_hints_the_operations_carry(self):
        """
        Case: Build an operation for an annotated view.
        Expected: It is hinted with the view's sharding mode, which is the whole of the seam between the two libraries.
        """
        operation = AddView(PublicOnly.definition, hints=PublicOnly.router_hints)

        self.assertEqual(operation.hints, {'sharding_mode': ShardingMode.PUBLIC})
        self.assertEqual(ShardOnly.router_hints, {'sharding_mode': ShardingMode.SHARDED})

    def test_every_sharding_mode_has_a_decorator(self):
        """
        Case: Annotate a view with the mirrored decorator.
        Expected: It records the matching sharding mode, mirroring the function and model decorators.
        """

        @mirrored_view()
        class Mirrored(View):
            app_label = APP_LABEL
            sql = 'SELECT 1'

        self.assertEqual(Mirrored.router_hints, {'sharding_mode': ShardingMode.MIRRORED})

    def test_an_unannotated_view_defaults_to_public(self):
        """
        Case: A view that was never annotated, in a project that has this plugin installed.
        Expected: It is treated as PUBLIC, the same default an unannotated function gets, so declarations written for a
                  single-database project keep working once that project is sharded.
        """
        self.assertEqual(Unannotated.router_hints, {'sharding_mode': ShardingMode.PUBLIC})


class ViewPlacementTestCase(ViewShardingTestCase):
    def test_a_public_view_lands_on_the_public_schema_only(self):
        """
        Case: Apply the operation for a PUBLIC view against both the public schema and a shard.
        Expected: Only the public schema gets it, because the router refuses the operation on the shard.
        """
        self.apply_everywhere(AddView(PublicOnly.definition, hints=PublicOnly.router_hints))

        self.assertTrue(self.view_exists(PublicOnly, PUBLIC_SCHEMA_NAME))
        self.assertFalse(self.view_exists(PublicOnly, SHARD_SCHEMA))

    def test_a_sharded_view_lands_on_the_shard_only(self):
        """
        Case: Apply the operation for a SHARDED view against both schemas.
        Expected: Only the shard gets it, which is what lets each shard have its own copy over its own tables.
        """
        self.apply_everywhere(AddView(ShardOnly.definition, hints=ShardOnly.router_hints))

        self.assertFalse(self.view_exists(ShardOnly, PUBLIC_SCHEMA_NAME))
        self.assertTrue(self.view_exists(ShardOnly, SHARD_SCHEMA))

    def test_a_sharded_view_reads_the_tables_of_its_own_shard(self):
        """
        Case: Query a sharded view from inside the shard, after inserting a row there.
        Expected: The shard's own row comes back, so each copy is bound to the tables of the schema it was created in.
        """
        self.apply_everywhere(AddView(ShardOnly.definition, hints=ShardOnly.router_hints))

        with use_shard(node_name='default', schema_name=SHARD_SCHEMA) as env:
            cursor = env.connection.cursor()
            cursor.execute("INSERT INTO {} (name) VALUES ('sharded')".format(SOURCE_TABLE))
            cursor.execute('SELECT name FROM {}'.format(ShardOnly.resolved_db_name))

            self.assertEqual(cursor.fetchone()[0], 'sharded')

    def test_removing_a_sharded_view_spares_a_public_one(self):
        """
        Case: Remove a SHARDED view while a PUBLIC one of another name exists on the public schema.
        Expected: The public view is untouched, since the removal is refused on the schema it does not belong to.
        """
        self.apply_everywhere(AddView(PublicOnly.definition, hints=PublicOnly.router_hints))
        self.apply_everywhere(AddView(ShardOnly.definition, hints=ShardOnly.router_hints))

        self.apply_everywhere(RemoveView(ShardOnly.definition, hints=ShardOnly.router_hints))

        self.assertFalse(self.view_exists(ShardOnly, SHARD_SCHEMA))
        self.assertTrue(self.view_exists(PublicOnly, PUBLIC_SCHEMA_NAME))


class MaterializedViewShardingTestCase(ViewShardingTestCase):
    """
    The same seam as the plain views above, for the stored flavour. The decorators are the library's only input on
    placement, so a materialized view has to be placed by them unchanged.
    """

    DECLARATIONS = (PublicStored, ShardStored, MirroredStored, EmptyStored)


class MaterializedViewAnnotationTestCase(MaterializedViewShardingTestCase):
    def test_the_decorators_annotate_a_materialized_view_the_same_way(self):
        """
        Case: Annotate materialized views with each of the three decorators.
        Expected: They record the same hints they record for a plain view, and an operation built for one carries
                  them, since neither the decorators nor the operations care which flavour the declaration is.
        """
        self.assertEqual(PublicStored.router_hints, {'sharding_mode': ShardingMode.PUBLIC})
        self.assertEqual(ShardStored.router_hints, {'sharding_mode': ShardingMode.SHARDED})
        self.assertEqual(MirroredStored.router_hints, {'sharding_mode': ShardingMode.MIRRORED})

        operation = AddView(PublicStored.definition, hints=PublicStored.router_hints)
        self.assertEqual(operation.hints, {'sharding_mode': ShardingMode.PUBLIC})

    def test_only_a_stored_view_is_annotated_with_where_a_refresh_goes(self):
        """
        Case: The refresh hook on a materialized view, a plain view and a function.
        Expected: Only the materialized view has one.
        """
        self.assertTrue(hasattr(ShardStored, 'db_for_refresh'))
        self.assertFalse(hasattr(ShardOnly, 'db_for_refresh'))
        self.assertFalse(hasattr(AllUppercase, 'db_for_refresh'))

    def test_reannotating_a_mirrored_subclass_drops_the_fan_out(self):
        """
        Case: Subclass a MIRRORED materialized view and re-annotate the subclass as SHARDED.
        Expected: The refresh the subclass inherited stops fanning out to every node.
        """

        @sharded_view()
        class ShardedAgain(MirroredStored):
            app_label = APP_LABEL

        self.assertEqual(ShardedAgain.router_hints, {'sharding_mode': ShardingMode.SHARDED})
        self.assertIsNone(getattr(ShardedAgain.refresh.__func__, '__wrapped_refresh__', None))
        self.assertIsNotNone(getattr(MirroredStored.refresh.__func__, '__wrapped_refresh__', None))

    def test_a_refresh_operation_carries_the_declaration_hints(self):
        """
        Case: Build a refresh operation for an annotated materialized view.
        Expected: It is hinted with the view's sharding mode, so a refresh is routed the same way the add and the
                  remove are.
        """
        operation = RefreshMaterializedView(ShardStored.definition, hints=ShardStored.router_hints)

        self.assertEqual(operation.hints, {'sharding_mode': ShardingMode.SHARDED})


class MaterializedViewPlacementTestCase(MaterializedViewShardingTestCase):
    def test_a_public_materialized_view_lands_on_the_public_schema_only(self):
        """
        Case: Apply the operation for a PUBLIC materialized view against both the public schema and a shard.
        Expected: Only the public schema gets it, and it arrives materialized rather than as a plain view.
        """
        self.apply_everywhere(AddView(PublicStored.definition, hints=PublicStored.router_hints))

        self.assertEqual(self.relkind(PublicStored, PUBLIC_SCHEMA_NAME), 'm')
        self.assertIsNone(self.relkind(PublicStored, SHARD_SCHEMA))

    def test_a_sharded_materialized_view_lands_on_the_shard_only(self):
        """
        Case: Apply the operation for a SHARDED materialized view against both schemas.
        Expected: Only the shard gets it, materialized, which is what lets each shard store its own rows.
        """
        self.apply_everywhere(AddView(ShardStored.definition, hints=ShardStored.router_hints))

        self.assertIsNone(self.relkind(ShardStored, PUBLIC_SCHEMA_NAME))
        self.assertEqual(self.relkind(ShardStored, SHARD_SCHEMA), 'm')

    def test_a_mirrored_materialized_view_lands_on_the_public_schema_of_another_node(self):
        """
        Case: Apply the operation for a MIRRORED materialized view against the public schema of the second node.
        Expected: That node gets it too, since a mirrored object belongs on the public schema of every node.
        """
        self._add_on_other_node(MirroredStored)

        self.assertEqual(self.relkind(MirroredStored, PUBLIC_SCHEMA_NAME, node_name='other'), 'm')

    def test_a_public_materialized_view_lands_on_the_public_schema_of_another_node(self):
        """
        Case: Apply the operation for a PUBLIC materialized view against the public schema of the second node.
        Expected: That node gets it as well. The router treats PUBLIC and MIRRORED alike, both being allowed on any
                  node's public schema and refused on a shard, so a public object is not confined to the default
                  node. What separates the two modes is the refresh, not the placement.
        """
        self._add_on_other_node(PublicStored)

        self.assertEqual(self.relkind(PublicStored, PUBLIC_SCHEMA_NAME, node_name='other'), 'm')

    def _add_on_other_node(self, declaration):
        """
        Build a materialized view on the public schema of the second node, source table and all.

        That node is not part of the shared setUp, which knows only about the default one, so the table the view
        reads has to be created there first.
        """
        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME) as env:
            env.connection.cursor().execute('CREATE TABLE {} (id serial PRIMARY KEY, name text)'.format(SOURCE_TABLE))
        self.addCleanup(self._drop_on_other_node, declaration)

        self.apply(
            AddView(declaration.definition, hints=declaration.router_hints),
            PUBLIC_SCHEMA_NAME,
            node_name='other',
        )

    def _drop_on_other_node(self, declaration):
        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME) as env:
            cursor = env.connection.cursor()
            cursor.execute(self._drop_statement(declaration, PUBLIC_SCHEMA_NAME))
            cursor.execute('DROP TABLE IF EXISTS "{}".{} CASCADE;'.format(PUBLIC_SCHEMA_NAME, SOURCE_TABLE))

    def test_the_declared_indexes_survive_the_placement(self):
        """
        Case: Apply a SHARDED materialized view declaring a unique index and a plain one.
        Expected: Both land on the shard and neither on the public schema. The unique one really is unique, which is
                  what a concurrent refresh needs, and is why unique_index is declared apart from indexes.
        """
        self.apply_everywhere(AddView(ShardStored.definition, hints=ShardStored.router_hints))

        unique, plain = self.unique_index_names(ShardStored, SHARD_SCHEMA)
        self.assertEqual(unique, {ShardStored.definition.index_name(('id',), unique=True)})
        self.assertEqual(plain, {ShardStored.definition.index_name(('name',), unique=False)})

        self.assertEqual(self.unique_index_names(ShardStored, PUBLIC_SCHEMA_NAME), (set(), set()))

    def test_a_view_declared_without_data_arrives_unpopulated(self):
        """
        Case: Apply a materialized view declared with with_data off.
        Expected: It is created on the shard but holds no rows yet, so the declaration's choice to defer the first
                  population survives being placed.
        """
        self.apply_everywhere(AddView(EmptyStored.definition, hints=EmptyStored.router_hints))

        self.assertEqual(self.relkind(EmptyStored, SHARD_SCHEMA), 'm')
        self.assertFalse(self.is_populated(EmptyStored, SHARD_SCHEMA))

    def test_the_fill_from_a_migration_is_what_a_newly_created_shard_inherits(self):
        """
        Case: Fill a view declared with_data off with the refresh operation, then clone its schema the way creating a
              shard does.
        Expected: The clone is populated too. A shard is created by cloning the template rather than by migrating an
                  empty schema, so the operation running on the template is what leaves every shard made afterwards
                  with a view PostgreSQL will read.
        """
        self.apply_everywhere(AddView(EmptyStored.definition, hints=EmptyStored.router_hints))
        self.apply_everywhere(RefreshMaterializedView(EmptyStored.definition, hints=EmptyStored.router_hints))
        connection.create_schema(CLONE_SCHEMA)

        connection.clone_schema(SHARD_SCHEMA, CLONE_SCHEMA)

        self.assertTrue(self.is_populated(EmptyStored, CLONE_SCHEMA))

    def test_a_view_left_unpopulated_is_cloned_unpopulated(self):
        """
        Case: Clone a schema holding a with_data off view that no refresh has run against.
        Expected: The clone's copy is unpopulated as well, since the clone carries the population state across rather
                  than deciding one. Leaving the fill out of the migrations therefore leaves every shard made
                  afterwards with a view that cannot be read, and no later migrate to repair it.
        """
        self.apply_everywhere(AddView(EmptyStored.definition, hints=EmptyStored.router_hints))
        connection.create_schema(CLONE_SCHEMA)

        connection.clone_schema(SHARD_SCHEMA, CLONE_SCHEMA)

        self.assertFalse(self.is_populated(EmptyStored, CLONE_SCHEMA))

    def test_removing_a_sharded_materialized_view_spares_a_public_one(self):
        """
        Case: Remove a SHARDED materialized view while a PUBLIC one of another name exists on the public schema.
        Expected: The public view is untouched, and the removal really drops the sharded one, which it could not do
                  were it issuing the plain DROP VIEW.
        """
        self.apply_everywhere(AddView(PublicStored.definition, hints=PublicStored.router_hints))
        self.apply_everywhere(AddView(ShardStored.definition, hints=ShardStored.router_hints))

        self.apply_everywhere(RemoveView(ShardStored.definition, hints=ShardStored.router_hints))

        self.assertIsNone(self.relkind(ShardStored, SHARD_SCHEMA))
        self.assertEqual(self.relkind(PublicStored, PUBLIC_SCHEMA_NAME), 'm')


class MaterializedViewRefreshTestCase(MaterializedViewShardingTestCase):
    def _insert(self, name, schema_name=SHARD_SCHEMA, node_name='default'):
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            env.connection.cursor().execute('INSERT INTO {} (name) VALUES (%s)'.format(SOURCE_TABLE), [name])

    def _stored_names(self, declaration, schema_name=SHARD_SCHEMA, node_name='default'):
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            cursor = env.connection.cursor()
            cursor.execute('SELECT name FROM {}'.format(declaration.resolved_db_name))
            return [name for (name,) in cursor.fetchall()]

    def _create_mirrored_on_both_nodes(self):
        """
        A mirrored view has a copy on the public schema of every node, so the case that refreshes one has to build
        both, source table and all. The second node's table is not part of the shared setUp, which knows only about
        the default node.
        """
        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME) as env:
            env.connection.cursor().execute('CREATE TABLE {} (id serial PRIMARY KEY, name text)'.format(SOURCE_TABLE))
        self.addCleanup(self._drop_on_other_node)

        operation = AddView(MirroredStored.definition, hints=MirroredStored.router_hints)
        self.apply(operation, PUBLIC_SCHEMA_NAME)
        self.apply(operation, PUBLIC_SCHEMA_NAME, node_name='other')

    def _drop_on_other_node(self):
        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME) as env:
            cursor = env.connection.cursor()
            cursor.execute(self._drop_statement(MirroredStored, PUBLIC_SCHEMA_NAME))
            cursor.execute('DROP TABLE IF EXISTS "{}".{} CASCADE;'.format(PUBLIC_SCHEMA_NAME, SOURCE_TABLE))

    def test_a_refresh_is_refused_on_the_schema_the_view_does_not_belong_to(self):
        """
        Case: Apply the refresh for a SHARDED materialized view against the public schema, where it was never created.
        Expected: Nothing happens and nothing raises. Were the routing not honoured the REFRESH would run and fail on
                  a relation that does not exist there.
        """
        self.apply_everywhere(AddView(ShardStored.definition, hints=ShardStored.router_hints))

        self.apply(RefreshMaterializedView(ShardStored.definition, hints=ShardStored.router_hints), PUBLIC_SCHEMA_NAME)

        self.assertIsNone(self.relkind(ShardStored, PUBLIC_SCHEMA_NAME))

    def test_a_refresh_repopulates_the_copy_on_its_own_shard(self):
        """
        Case: Insert a row into the shard's source table after the materialized view was created, then apply the
              refresh on that shard.
        Expected: The view is stale until the refresh and holds the row afterwards.
        """
        self.apply_everywhere(AddView(ShardStored.definition, hints=ShardStored.router_hints))
        self._insert('after the view')

        self.assertEqual(self._stored_names(ShardStored), [])

        self.apply(RefreshMaterializedView(ShardStored.definition, hints=ShardStored.router_hints), SHARD_SCHEMA)

        self.assertEqual(self._stored_names(ShardStored), ['after the view'])

    def test_a_declared_unique_index_allows_a_concurrent_refresh(self):
        """
        Case: Refresh a populated materialized view concurrently, on the strength of its declared unique index.
        Expected: It catches up. This is what the unique index is declared for, and Postgres refuses a concurrent
                  refresh without one.
        """
        self._insert('before the view')
        self.apply_everywhere(AddView(ShardStored.definition, hints=ShardStored.router_hints))
        self._insert('after the view')

        self.apply(
            RefreshMaterializedView(ShardStored.definition, concurrently=True, hints=ShardStored.router_hints),
            SHARD_SCHEMA,
        )

        self.assertEqual(sorted(self._stored_names(ShardStored)), ['after the view', 'before the view'])

    def test_a_refresh_from_code_follows_the_shard_in_context(self):
        """
        Case: The connection a declaration would refresh on, inside a shard and outside one.
        Expected: The shard's own connection while it is active, and the primary node otherwise.
        """
        with use_shard(node_name='default', schema_name=SHARD_SCHEMA) as env:
            self.assertEqual(ShardStored.db_for_refresh(), env.options)

        self.assertEqual(ShardStored.db_for_refresh(), settings.QUILT_DB.get('PRIMARY_DB_ALIAS', DEFAULT_DB_ALIAS))

    def test_a_refresh_from_code_repopulates_the_copy_of_the_active_shard(self):
        """
        Case: Insert a row into a shard's source table, then call refresh() on the declaration from inside that shard.
        Expected: The shard's copy is refreshed.
        """
        self.apply_everywhere(AddView(ShardStored.definition, hints=ShardStored.router_hints))
        self._insert('after the view')
        self.assertEqual(self._stored_names(ShardStored), [])

        with use_shard(node_name='default', schema_name=SHARD_SCHEMA):
            ShardStored.refresh()

        self.assertEqual(self._stored_names(ShardStored), ['after the view'])

    def test_a_concurrent_refresh_from_code_uses_the_declared_unique_index(self):
        """
        Case: refresh(concurrently=True) from inside the shard, on a view carrying its declared unique index.
        Expected: It refreshes without locking readers out.
        """
        self._insert('before the view')
        self.apply_everywhere(AddView(ShardStored.definition, hints=ShardStored.router_hints))
        self._insert('after the view')

        with use_shard(node_name='default', schema_name=SHARD_SCHEMA):
            ShardStored.refresh(concurrently=True)

        self.assertEqual(sorted(self._stored_names(ShardStored)), ['after the view', 'before the view'])

    def test_a_refresh_from_code_fills_a_view_created_without_data(self):
        """
        Case: A sharded view declared with_data off, refreshed from code inside its shard.
        Expected: Unpopulated until then, and holding the shard's rows afterwards.
        """
        self._insert('a row')
        self.apply_everywhere(AddView(EmptyStored.definition, hints=EmptyStored.router_hints))
        self.assertFalse(self.is_populated(EmptyStored, SHARD_SCHEMA))

        with use_shard(node_name='default', schema_name=SHARD_SCHEMA):
            EmptyStored.refresh()

        self.assertTrue(self.is_populated(EmptyStored, SHARD_SCHEMA))
        self.assertEqual(self._stored_names(EmptyStored), ['a row'])

    def test_a_mirrored_refresh_reaches_every_node(self):
        """
        Case: refresh() on a mirrored view, with a row added to the source table of each node after the copies were
              created.
        Expected: Both copies are refreshed from the one call.
        """
        self._create_mirrored_on_both_nodes()
        self._insert('on default', schema_name=PUBLIC_SCHEMA_NAME)
        self._insert('on other', schema_name=PUBLIC_SCHEMA_NAME, node_name='other')

        MirroredStored.refresh()

        self.assertEqual(self._stored_names(MirroredStored, PUBLIC_SCHEMA_NAME), ['on default'])
        self.assertEqual(self._stored_names(MirroredStored, PUBLIC_SCHEMA_NAME, node_name='other'), ['on other'])

    def test_a_named_connection_pins_a_mirrored_refresh_to_one_node(self):
        """
        Case: refresh(using=...) on a mirrored view, naming one node.
        Expected: Only that node's copy moves.
        """
        self._create_mirrored_on_both_nodes()
        self._insert('on default', schema_name=PUBLIC_SCHEMA_NAME)
        self._insert('on other', schema_name=PUBLIC_SCHEMA_NAME, node_name='other')

        MirroredStored.refresh(using=DEFAULT_DB_ALIAS)

        self.assertEqual(self._stored_names(MirroredStored, PUBLIC_SCHEMA_NAME), ['on default'])
        self.assertEqual(self._stored_names(MirroredStored, PUBLIC_SCHEMA_NAME, node_name='other'), [])
