import ast
import copy
import hashlib
import inspect
import textwrap
from unittest import mock

from django.db import router
from django.db.migrations import AddField, Migration, RunPython, SeparateDatabaseAndState
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.operations.base import Operation
from django.db.migrations.recorder import MigrationRecorder
from django.db.migrations.state import ProjectState, StateApps
from django.db.models import IntegerField, Manager
from django.db.transaction import atomic
from django.test import SimpleTestCase, override_settings
from django.utils.functional import cached_property

from djanquiltdb.db import connection
from djanquiltdb.management.executor import (
    HistoricalModelsChanged,
    OperationBoundToStates,
    SharedFilesMigrationExecutor,
    SharedOperationStates,
    SharedStartingStates,
    SharedStatesMigrationExecutor,
    compute_states_around_migration_operations,
    enable_shared_migration_states,
    shared_migration_states_enabled,
    snapshot_historical_model_attributes,
)
from djanquiltdb.utils import get_template_name, use_shard
from migration_tests.tests.migration_base import MigrationTestCase

INITIAL_MIGRATION = ('migration_tests', '0001_initial')
SECOND_MIGRATION = ('migration_tests', '0002_second')
THIRD_MIGRATION = ('migration_tests', '0003_third')


class EnableSharedMigrationStatesTestCase(SimpleTestCase):
    def test_shares_within_the_block_only(self):
        """
        Case: Check whether migrate shares project states before, inside and after an enable_shared_migration_states
              block.
        Expected: It shares them only inside the block.
        """
        self.assertFalse(shared_migration_states_enabled())
        with enable_shared_migration_states():
            self.assertTrue(shared_migration_states_enabled())
        self.assertFalse(shared_migration_states_enabled())

    def test_stops_sharing_when_the_block_raises(self):
        """
        Case: Raise an error inside an enable_shared_migration_states block.
        Expected: migrate does not share project states after the block.
        """
        with self.assertRaises(ValueError):
            with enable_shared_migration_states():
                raise ValueError

        self.assertFalse(shared_migration_states_enabled())


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ComputeStatesAroundMigrationOperationsTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        self.executor = SharedStatesMigrationExecutor(connection)
        self.migration = self.executor.loader.graph.nodes[INITIAL_MIGRATION]
        self.state = self.executor._create_project_state(with_applied_migrations=True)

    def test_one_state_around_each_operation(self):
        """
        Case: Compute the states of a migration with four operations.
        Expected: Five states. The first is the state passed in, and each next one adds an operation's change.
        """
        states = compute_states_around_migration_operations(self.migration, self.state)

        self.assertEqual(len(states), len(self.migration.operations) + 1)
        self.assertIs(states[0], self.state)
        self.assertNotIn(('migration_tests', 'author'), states[0].models)
        self.assertIn(('migration_tests', 'author'), states[1].models)
        self.assertNotIn(('migration_tests', 'tribble'), states[1].models)
        self.assertIn(('migration_tests', 'tribble'), states[2].models)

    def test_states_are_not_mutated_afterwards(self):
        """
        Case: Compute the states of a migration, then compute them again from the state it ended in.
        Expected: The first list is unchanged. (Every schema at that point shares these states, so a change would give
                  the schemas that follow the wrong from_state.)
        """
        states = compute_states_around_migration_operations(self.migration, self.state)
        before = [set(state.models) for state in states]

        compute_states_around_migration_operations(self.executor.loader.graph.nodes[SECOND_MIGRATION], states[-1])

        self.assertEqual([set(state.models) for state in states], before)

    def compute_states_after_initial_migration(self, *operations):
        """
        Return the states of a migration made of operations. The migration starts from the rendered end state of
        0001_initial.
        """
        self.state.apps
        migration = Migration('0099_test', 'migration_tests')
        migration.operations = list(operations)

        return compute_states_around_migration_operations(
            migration, compute_states_around_migration_operations(self.migration, self.state)[-1]
        )

    def test_run_python_from_state_is_rendered_once(self):
        """
        Case: Compute the states of a migration that adds a non-relational field and then runs a RunPython. (The
              AddField delays re-rendering the related models.)
        Expected: The RunPython starts from a fully rendered state that is not delayed. So the render survives the cache
                  clear that RunPython does for each schema.
        """
        states = self.compute_states_after_initial_migration(
            AddField('Author', 'pages', IntegerField(default=0)), RunPython(RunPython.noop)
        )
        run_python_from_state = states[1]

        self.assertFalse(run_python_from_state.is_delayed)
        run_python_from_state.clear_delayed_apps_cache()
        self.assertIn('apps', run_python_from_state.__dict__)
        author = run_python_from_state.apps.get_model('migration_tests', 'Author')
        self.assertEqual(author._meta.get_field('pages').get_internal_type(), 'IntegerField')

    def test_run_python_within_separate_database_and_state_from_state_is_rendered_once(self):
        """
        Case: Compute the states of a migration that adds a non-relational field and then runs a
              SeparateDatabaseAndState with a RunPython as database operation. (The AddField delays re-rendering the
              related models.)
        Expected: The SeparateDatabaseAndState starts from a fully rendered state that is not delayed, the same as a
                  plain RunPython. So the render survives the cache clear that the RunPython does for each schema.
        """
        states = self.compute_states_after_initial_migration(
            AddField('Author', 'pages', IntegerField(default=0)),
            SeparateDatabaseAndState(database_operations=[RunPython(RunPython.noop)]),
        )
        run_python_from_state = states[1]

        self.assertFalse(run_python_from_state.is_delayed)
        run_python_from_state.clear_delayed_apps_cache()
        self.assertIn('apps', run_python_from_state.__dict__)

    def test_separate_database_and_state_gets_the_states_around_its_database_operations(self):
        """
        Case: Compute the states of a migration with a SeparateDatabaseAndState. Its database operations add a
              non-relational field and then run a RunPython.
        Expected: The states around each database operation are computed too. They go from the migration's starting
                  state to a state with the field. The RunPython starts from a rendered state that is not delayed. The
                  migration's own states do not have the field.
        """
        states = self.compute_states_after_initial_migration(
            SeparateDatabaseAndState(
                database_operations=[AddField('Author', 'pages', IntegerField(default=0)), RunPython(RunPython.noop)]
            )
        )
        database_operation_states = states.database_operation_states[0]

        self.assertEqual(len(database_operation_states), 3)
        self.assertIs(database_operation_states[0], states[0])
        self.assertIn('pages', database_operation_states[1].models['migration_tests', 'author'].fields)
        self.assertFalse(database_operation_states[1].is_delayed)
        self.assertIn('apps', database_operation_states[1].__dict__)
        self.assertNotIn('pages', states[1].models['migration_tests', 'author'].fields)

    def test_delayed_field_without_run_python_stays_delayed(self):
        """
        Case: Compute the states of a migration that only adds a non-relational field.
        Expected: Its state stays delayed, the same as in Django. Only a RunPython pays for the full render.
        """
        states = self.compute_states_after_initial_migration(AddField('Author', 'pages', IntegerField(default=0)))

        self.assertTrue(states[1].is_delayed)

    def test_unrendered_starting_state_is_rendered_once_through_the_callback(self):
        """
        Case: Compute the states of a migration from a state whose models are not rendered, with a progress callback.
        Expected: The starting state is rendered before the first operation, and the callback is called for that render
                  once. (Django also renders it first, so each operation re-renders only what it touches.)
        """
        callback = mock.Mock()

        states = compute_states_around_migration_operations(self.migration, self.state, callback)

        self.assertIn('apps', states[0].__dict__)
        self.assertEqual(callback.call_args_list, [mock.call('render_start'), mock.call('render_success')])

    def test_rendered_starting_state_is_not_rendered_again(self):
        """
        Case: Compute the states of a migration from a state whose models are rendered, with a progress callback.
        Expected: The callback is not called.
        """
        callback = mock.Mock()
        self.state.apps

        compute_states_around_migration_operations(self.migration, self.state, callback)

        self.assertFalse(callback.called)

    def test_delayed_starting_state_before_run_python_is_rendered_once(self):
        """
        Case: Compute the states of a migration that starts with a RunPython, with a progress callback. The starting
              state is one that a non-relational AddField left delayed.
        Expected: The starting state is rendered in full once, and the callback is called for it. The state is not
                  delayed afterwards, so the render survives the cache clear that RunPython does for each schema.
        """
        callback = mock.Mock()
        delayed = self.compute_states_after_initial_migration(AddField('Author', 'pages', IntegerField(default=0)))[-1]
        migration = Migration('0100_test', 'migration_tests')
        migration.operations = [RunPython(RunPython.noop)]

        states = compute_states_around_migration_operations(migration, delayed, callback)

        self.assertIs(states[0], delayed)
        self.assertFalse(states[0].is_delayed)
        self.assertIn('apps', states[0].__dict__)
        self.assertEqual(callback.call_args_list, [mock.call('render_start'), mock.call('render_success')])
        self.assertEqual(
            states[0].apps.get_model('migration_tests', 'Author')._meta.get_field('pages').get_internal_type(),
            'IntegerField',
        )


class SharedStatesMigrationExecutorInitTestCase(SimpleTestCase):
    def test_sets_what_djangos_init_sets(self):
        """
        Case: Build Django's MigrationExecutor, a SharedFilesMigrationExecutor and a SharedStatesMigrationExecutor, all
              without a connection.
        Expected: The other two and their loaders have every attribute of Django's, plus only their own. If Django's
                  constructor sets a new attribute, this fails and SharedFilesMigrationExecutor.__init__ needs an
                  update.
        """
        django_executor = MigrationExecutor(None)
        expected = {
            SharedFilesMigrationExecutor: {'starting_states'},
            SharedStatesMigrationExecutor: {'starting_states', 'squashed_migrations_checked'},
        }

        for executor_class, own in expected.items():
            with self.subTest(executor_class=executor_class.__name__):
                executor = executor_class(None)

                self.assertEqual(set(vars(executor)) - set(vars(django_executor)), own)
                self.assertLessEqual(set(vars(django_executor)), set(vars(executor)))
                self.assertEqual(
                    set(vars(executor.loader)) - set(vars(django_executor.loader)), {'loaded_migration_files'}
                )
                self.assertLessEqual(set(vars(django_executor.loader)), set(vars(executor.loader)))


def fingerprint_function_code(function):
    """
    Return a hash of the function's syntax tree without its docstring. Comments, formatting and docstrings do not change
    the hash.
    """
    definition = ast.parse(textwrap.dedent(inspect.getsource(function))).body[0]
    if ast.get_docstring(definition) is not None:
        definition.body = definition.body[1:]

    return hashlib.sha256(ast.dump(definition).encode()).hexdigest()


# Django code that the executors hook into or re-enact, with the fingerprints of the checked versions.
DJANGO_FINGERPRINTS = {
    MigrationExecutor.migrate: {'d00c8d8f68b8395b550c2707182565c92b8fa15f725f20a26a2926a0174d4fce'},
    MigrationExecutor._migrate_all_forwards: {'abbf82312f4f5ce63f0bab8d37c4ba2267f02615989c6820f0fa5437f2e10415'},
    MigrationExecutor._create_project_state: {'6352aec7416a2a761179e3bf2514ce7d40acff9f1db949870173e246f2132e03'},
    MigrationExecutor.apply_migration: {'7d504fee872cbaa8c64ef3aa96c93193593885a95bbe4d10e872a9fc7dee3d24'},
    MigrationExecutor.record_migration: {'576ccff70b39f52736756c28d7cd2f652420b55a279e5fafac06b7167c8187ac'},
    MigrationExecutor.check_replacements: {'e626ecfca98224e02dceb3a1314c4c611fb41657eba500c6691e70cf14886454'},
    MigrationExecutor.detect_soft_applied: {'7203fe8a920fa127867cf0f6cbd9199b1024a7df35ef63b1aead70d9ba7a2432'},
    Migration.mutate_state: {'c39f3252fbb3319cebf79465bfb32411f082f5f23af8825d96c6858fb35ba559'},
    Migration.apply: {'1ab7e4554e2862e7bd701037fe59ac6143cbc6ee08065d3347c58daa4ca92279'},
    SeparateDatabaseAndState.state_forwards: {'fedfcae08c84f2a66f3852fc7e735f66647697b28fb8d3523c32cf398410cb1c'},
    SeparateDatabaseAndState.database_forwards: {'69fb072f089b2592c603ad7656686675de272fdc7fe1d65de724a00455c65e4a'},
    RunPython.database_forwards: {'e40d99827d2be40a32e427ab8ab3e278b0d2d9c3444f0687dbb5d6212e81d145'},
}


class DjangoCouplingTestCase(SimpleTestCase):
    """
    Check the parts of Django's migration internals that the executors rely on. If a Django release changes one of them,
    a test here fails and the executors need an update.
    """

    # Show the changed fingerprints in full in the failure message.
    maxDiff = None

    def test_django_code_the_executors_rely_on_is_unchanged(self):
        """
        Case: Fingerprint each piece of Django code the executors hook into or re-enact.
        Expected: Each matches a version the shared states path was checked against. That path hands Django a migration
                  with replaced apply and mutate_state methods, and with operations bound to their states. Nothing else
                  notices when Django changes how it builds states or checks operation types, so a changed fingerprint
                  is the cue to check.
        """
        changed = {
            function.__qualname__: fingerprint_function_code(function)
            for function, fingerprints in DJANGO_FINGERPRINTS.items()
            if fingerprint_function_code(function) not in fingerprints
        }

        self.assertEqual(
            changed,
            {},
            'Django changed code the executors rely on. Check the shared states path in djanquiltdb.management.executor '
            'against each change, then add its fingerprint to DJANGO_FINGERPRINTS.',
        )

    def test_migration_apply_and_mutate_state_signatures(self):
        """
        Case: Read the signatures of Migration.apply and Migration.mutate_state.
        Expected: They take the same arguments as the replacements that apply_migration_between_states installs.
        """
        self.assertEqual(
            list(inspect.signature(Migration.apply).parameters),
            ['self', 'project_state', 'schema_editor', 'collect_sql'],
        )
        self.assertEqual(
            list(inspect.signature(Migration.mutate_state).parameters), ['self', 'project_state', 'preserve']
        )

    def test_create_project_state_signature(self):
        """
        Case: Read the signature of MigrationExecutor._create_project_state.
        Expected: It takes with_applied_migrations. SharedFilesMigrationExecutor's override relies on that argument.
        """
        self.assertEqual(
            list(inspect.signature(MigrationExecutor._create_project_state).parameters),
            ['self', 'with_applied_migrations'],
        )

    def test_apply_migration_returns_what_migration_apply_returns(self):
        """
        Case: Run Django's apply_migration on a migration whose apply returns a state.
        Expected: apply_migration returns that state, so apply_migration_between_states can return it too.
        """
        executor = MigrationExecutor(None)
        executor.connection = mock.MagicMock()
        migration = mock.Mock(app_label='migration_tests', name='0099_test')
        end_state = ProjectState()
        migration.apply.return_value = end_state

        with mock.patch.object(executor, 'record_migration'):
            self.assertIs(executor.apply_migration(ProjectState(), migration), end_state)

    def test_run_python_runs_its_code_only_where_the_router_allows(self):
        """
        Case: Run a RunPython with hints forwards, with the router allowing it, and with the router not allowing it.
        Expected: Its code runs only when router.allow_migrate allows it. The router gets the schema editor's alias, the
                  app label and the hints. OperationBoundToStates uses the same condition to skip the historical models
                  check.
        """
        code = mock.Mock()
        operation = RunPython(code, hints={'model_name': 'author'})
        schema_editor = mock.Mock()
        schema_editor.connection.alias = 'default|template'

        for allowed in (True, False):
            with self.subTest(allowed=allowed):
                code.reset_mock()
                with mock.patch.object(router, 'allow_migrate', return_value=allowed) as mock_allow_migrate:
                    operation.database_forwards('migration_tests', schema_editor, mock.Mock(), mock.Mock())

                mock_allow_migrate.assert_called_once_with('default|template', 'migration_tests', model_name='author')
                self.assertIs(code.called, allowed)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
@mock.patch.object(MigrationExecutor, 'apply_migration', autospec=True, side_effect=MigrationExecutor.apply_migration)
@mock.patch.object(SharedStatesMigrationExecutor, 'apply_operations_between_states')
class SharedStatesMigrationExecutorTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        self.executor = SharedStatesMigrationExecutor(connection)
        self.migration = self.executor.loader.graph.nodes[INITIAL_MIGRATION]
        self.initial_migration_states = compute_states_around_migration_operations(
            self.migration, self.executor._create_project_state(with_applied_migrations=True)
        )

    def test_applies_the_operations_through_djangos_apply_migration(
        self, mock_apply_operations_between_states, mock_apply_migration
    ):
        """
        Case: Apply a migration from its precomputed states.
        Expected: Django's own apply_migration runs it from the first state. The operations' SQL runs between the
                  precomputed states, and then the migration is recorded.
        """
        self.executor.apply_migration_between_states(self.initial_migration_states, self.migration)

        mock_apply_migration.assert_called_once()
        self.assertIs(mock_apply_migration.call_args.args[1], self.initial_migration_states[0])
        mock_apply_operations_between_states.assert_called_once_with(
            self.initial_migration_states, self.migration, mock.ANY
        )
        self.assertIn(INITIAL_MIGRATION, self.executor.recorder.applied_migrations())

    def test_returns_the_end_state(self, mock_apply_operations_between_states, mock_apply_migration):
        """
        Case: Apply a migration from its precomputed states.
        Expected: It returns the last state. (Django's apply_migration also returns the end state of Migration.apply.)
        """
        self.assertIs(
            self.executor.apply_migration_between_states(self.initial_migration_states, self.migration),
            self.initial_migration_states[-1],
        )

    def test_fake_records_without_running_operations(self, mock_apply_operations_between_states, mock_apply_migration):
        """
        Case: Apply a migration from its precomputed states with fake=True.
        Expected: Django's own apply_migration records it without running any of its operations.
        """
        self.executor.apply_migration_between_states(self.initial_migration_states, self.migration, fake=True)

        mock_apply_migration.assert_called_once()
        self.assertFalse(mock_apply_operations_between_states.called)
        self.assertIn(INITIAL_MIGRATION, self.executor.recorder.applied_migrations())

    def test_record_migration_keeps_applied_set_current(
        self, mock_apply_operations_between_states, mock_apply_migration
    ):
        """
        Case: Record a migration as applied, then as unapplied.
        Expected: The loader's applied set follows each change and matches the database. So the executor can be kept for
                  a whole run.
        """
        self.executor.record_migration(*INITIAL_MIGRATION)

        self.assertIn(INITIAL_MIGRATION, self.executor.recorder.applied_migrations())
        self.assertIn(INITIAL_MIGRATION, self.executor.loader.applied_migrations)

        self.executor.record_migration(*INITIAL_MIGRATION, forward=False)

        self.assertNotIn(INITIAL_MIGRATION, self.executor.recorder.applied_migrations())
        self.assertNotIn(INITIAL_MIGRATION, self.executor.loader.applied_migrations)

    def test_record_migration_only_inserts(self, mock_apply_operations_between_states, mock_apply_migration):
        """
        Case: Record a migration as applied, once the migrations table exists.
        Expected: One query inserts the row, and there are no other queries. (The command records every migration on
                  every schema.)
        """
        self.executor.recorder.ensure_schema()

        with self.assertNumQueries(1):
            self.executor.record_migration(*INITIAL_MIGRATION)

        self.assertIn(INITIAL_MIGRATION, self.executor.loader.applied_migrations)

    def test_record_migration_with_the_migration_recorded_already(
        self, mock_apply_operations_between_states, mock_apply_migration
    ):
        """
        Case: Record a migration as applied when the migrations table already has two rows for it. (The table allows
              that.)
        Expected: The migration gets a third row and is in the loader's applied set.
        """
        self.executor.recorder.record_applied(*INITIAL_MIGRATION)
        self.executor.recorder.record_applied(*INITIAL_MIGRATION)

        self.executor.record_migration(*INITIAL_MIGRATION)

        self.assertEqual(
            self.executor.recorder.migration_qs.filter(app=INITIAL_MIGRATION[0], name=INITIAL_MIGRATION[1]).count(), 3
        )
        self.assertIn(INITIAL_MIGRATION, self.executor.loader.applied_migrations)

    @mock.patch.object(Migration, 'mutate_state', autospec=True, side_effect=Migration.mutate_state)
    def test_fake_initial_detects_from_the_computed_end_state(
        self, mock_mutate_state, mock_apply_operations_between_states, mock_apply_migration
    ):
        """
        Case: Apply an initial migration from its precomputed states with fake_initial=True, to a schema that already
              has its tables.
        Expected: Django's check for an already-applied initial migration gets the last state. So it does not build and
                  render the end state again for this schema. The migration is faked: recorded without running its
                  operations.
        """
        MigrationExecutor(connection).migrate([INITIAL_MIGRATION])
        self.executor.record_migration(*INITIAL_MIGRATION, forward=False)
        mock_mutate_state.reset_mock()
        detected = []

        def detect_soft_applied(project_state, migration):
            detected.append(MigrationExecutor.detect_soft_applied(self.executor, project_state, migration))
            return detected[-1]

        with mock.patch.object(self.executor, 'detect_soft_applied', side_effect=detect_soft_applied):
            self.executor.apply_migration_between_states(
                self.initial_migration_states, self.migration, fake_initial=True
            )

        self.assertEqual(len(detected), 1)
        self.assertIs(detected[0][1], self.initial_migration_states[-1])
        self.assertFalse(mock_mutate_state.called)
        self.assertFalse(mock_apply_operations_between_states.called)
        self.assertIn(INITIAL_MIGRATION, self.executor.recorder.applied_migrations())

    def test_check_replacements_without_squashes_does_not_query(
        self, mock_apply_operations_between_states, mock_apply_migration
    ):
        """
        Case: Check the replacements of an app that has no squashed migrations.
        Expected: The applied migrations are not read from the database, since there is no squash to record. (The
                  command checks after every migration on every schema, so each read would be a wasted query.)
        """
        with mock.patch.object(
            self.executor.recorder, 'applied_migrations', wraps=self.executor.recorder.applied_migrations
        ) as mock_applied_migrations:
            self.executor.check_replacements()

        self.assertFalse(mock_applied_migrations.called)


class StateRecordingOperation(Operation):
    """
    An operation that records the states it runs between.
    """

    def __init__(self, received_states):
        self.received_states = received_states

    def state_forwards(self, app_label, state):
        pass

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        self.received_states.append((from_state, to_state))


class StateRecordingSeparateDatabaseAndState(SeparateDatabaseAndState):
    """
    A SeparateDatabaseAndState that records the states it runs between. It does not run its database operations.
    """

    def __init__(self, received_states, **kwargs):
        super().__init__(**kwargs)
        self.received_states = received_states

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        self.received_states.append((from_state, to_state))


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ApplyOperationsBetweenStatesTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        self.executor = SharedStatesMigrationExecutor(connection)
        self.migration = self.executor.loader.graph.nodes[INITIAL_MIGRATION]
        self.initial_migration_states = compute_states_around_migration_operations(
            self.migration, self.executor._create_project_state(with_applied_migrations=True)
        )

    def apply_operations_in_schema_editor(self, states, migration, atomic=True):
        with connection.schema_editor(atomic=atomic) as schema_editor:
            SharedStatesMigrationExecutor.apply_operations_between_states(states, migration, schema_editor)

    @mock.patch.object(Migration, 'apply', autospec=True, side_effect=Migration.apply)
    def test_runs_djangos_apply_over_the_operations_bound_to_their_states(self, mock_apply):
        """
        Case: Run the operations of a migration between its precomputed states.
        Expected: Django's own Migration.apply runs them on a copy of the migration. The copy's operations are bound to
                  their states, in order. apply starts from an empty project state, since the bound operations carry
                  their own states. The migration's SQL has run.
        """
        self.apply_operations_in_schema_editor(self.initial_migration_states, self.migration)

        mock_apply.assert_called_once()
        bound, project_state = mock_apply.call_args.args[:2]
        self.assertIsNot(bound, self.migration)
        self.assertEqual([operation.operation for operation in bound.operations], self.migration.operations)
        self.assertEqual([operation.from_state for operation in bound.operations], self.initial_migration_states[:-1])
        self.assertEqual([operation.to_state for operation in bound.operations], self.initial_migration_states[1:])
        self.assertIsInstance(project_state, ProjectState)
        self.assertEqual(project_state.models, {})
        for operation in self.migration.operations:
            self.assertNotIsInstance(operation, OperationBoundToStates)
        self.assertTableExists('migration_tests_author')

    def test_each_operation_runs_between_its_states(self):
        """
        Case: Run two operations that record the states they get.
        Expected: Each gets the precomputed states around it, not the states Django's loop derives.
        """
        received_states = []
        migration = Migration('0099_test', 'migration_tests')
        migration.operations = [StateRecordingOperation(received_states), StateRecordingOperation(received_states)]
        states = compute_states_around_migration_operations(migration, self.initial_migration_states[-1])

        self.apply_operations_in_schema_editor(states, migration)

        self.assertEqual(len(received_states), 2)
        self.assertIs(received_states[0][0], states[0])
        self.assertIs(received_states[0][1], states[1])
        self.assertIs(received_states[1][0], states[1])
        self.assertIs(received_states[1][1], states[2])

    def apply_in_a_non_atomic_migration(self, operation):
        """
        Run operation as the only operation of a migration with atomic = False. Use a schema editor that opens no
        transaction. Return how many transactions Django's Migration.apply opened for it.
        """
        migration = Migration('0099_test', 'migration_tests')
        migration.atomic = False
        migration.operations = [operation]

        with mock.patch('django.db.migrations.migration.atomic', wraps=atomic) as mock_atomic:
            self.apply_operations_in_schema_editor(
                compute_states_around_migration_operations(migration, self.initial_migration_states[-1]),
                migration,
                atomic=False,
            )

        return mock_atomic.call_count

    def test_atomic_operation_in_a_non_atomic_migration_runs_in_a_transaction(self):
        """
        Case: Run a RunPython with atomic=True as the only operation of a migration with atomic = False.
        Expected: Django's apply runs it in its own transaction.
        """
        self.assertEqual(self.apply_in_a_non_atomic_migration(RunPython(RunPython.noop, atomic=True)), 1)

    def test_non_atomic_operation_in_a_non_atomic_migration_runs_without_a_transaction(self):
        """
        Case: Run a RunPython with atomic=False as the only operation of a migration with atomic = False.
        Expected: Django's apply opens no transaction for it.
        """
        self.assertEqual(self.apply_in_a_non_atomic_migration(RunPython(RunPython.noop, atomic=False)), 0)

    def test_default_operation_in_a_non_atomic_migration_runs_without_a_transaction(self):
        """
        Case: Run a RunPython that leaves atomic at its default as the only operation of a migration with
              atomic = False.
        Expected: Django's apply opens no transaction for it. In such a migration only a RunPython with atomic=True gets
                  one (see Django's migrations documentation).
        """
        self.assertEqual(self.apply_in_a_non_atomic_migration(RunPython(RunPython.noop)), 0)

    def test_separate_database_and_state_runs_its_database_operations_between_their_states(self):
        """
        Case: Run a SeparateDatabaseAndState with two database operations that record the states they get.
        Expected: Each gets the states computed along with the migration's states. The SeparateDatabaseAndState does not
                  derive them again for each schema.
        """
        received_states = []
        migration = Migration('0099_test', 'migration_tests')
        migration.operations = [
            SeparateDatabaseAndState(
                database_operations=[StateRecordingOperation(received_states), StateRecordingOperation(received_states)]
            )
        ]
        states = compute_states_around_migration_operations(migration, self.initial_migration_states[-1])
        database_operation_states = states.database_operation_states[0]

        self.apply_operations_in_schema_editor(states, migration)

        self.assertEqual(len(received_states), 2)
        self.assertIs(received_states[0][0], database_operation_states[0])
        self.assertIs(received_states[0][1], database_operation_states[1])
        self.assertIs(received_states[1][0], database_operation_states[1])
        self.assertIs(received_states[1][1], database_operation_states[2])

    def test_nested_separate_database_and_state_runs_between_its_states(self):
        """
        Case: Run a SeparateDatabaseAndState with another SeparateDatabaseAndState as its database operation. The inner
              one has a database operation that records the states it gets.
        Expected: That operation gets the states computed along with the migration's states.
        """
        received_states = []
        migration = Migration('0099_test', 'migration_tests')
        migration.operations = [
            SeparateDatabaseAndState(
                database_operations=[
                    SeparateDatabaseAndState(database_operations=[StateRecordingOperation(received_states)])
                ]
            )
        ]
        states = compute_states_around_migration_operations(migration, self.initial_migration_states[-1])
        inner_states = states.database_operation_states[0].database_operation_states[0]

        self.apply_operations_in_schema_editor(states, migration)

        self.assertEqual(len(received_states), 1)
        self.assertIs(received_states[0][0], inner_states[0])
        self.assertIs(received_states[0][1], inner_states[1])

    def test_separate_database_and_state_subclass_runs_its_own_database_forwards(self):
        """
        Case: Run a SeparateDatabaseAndState subclass with its own database_forwards that records the states it gets.
        Expected: Its database_forwards runs between the states around it, like any other operation. The states around
                  its database operations are not computed, and those operations do not run in its place.
        """
        received_states = []
        recorded = []
        migration = Migration('0099_test', 'migration_tests')
        migration.operations = [
            StateRecordingSeparateDatabaseAndState(
                received_states, database_operations=[StateRecordingOperation(recorded)]
            )
        ]
        states = compute_states_around_migration_operations(migration, self.initial_migration_states[-1])

        self.apply_operations_in_schema_editor(states, migration)

        self.assertEqual(states.database_operation_states, {})
        self.assertEqual(received_states, [(states[0], states[1])])
        self.assertEqual(recorded, [])

    def test_bound_operation_can_be_copied(self):
        """
        Case: Copy an operation bound to its states.
        Expected: The copy is bound to the same operation and states. (copy.copy creates the new object without
                  attributes, so looking up operation on it must not be passed on to the wrapped operation.)
        """
        operation = RunPython(RunPython.noop)
        bound = OperationBoundToStates(
            operation, self.initial_migration_states[0], self.initial_migration_states[1], self.migration
        )

        copied = copy.copy(bound)

        self.assertIs(copied.operation, operation)
        self.assertIs(copied.from_state, self.initial_migration_states[0])
        self.assertIs(copied.to_state, self.initial_migration_states[1])

    def test_bound_operation_answers_for_its_operation(self):
        """
        Case: Bind an operation to its states.
        Expected: The wrapped operation answers everything except running: atomic, reversible and describe().
        """
        operation = RunPython(RunPython.noop, atomic=False)
        bound = OperationBoundToStates(
            operation, self.initial_migration_states[0], self.initial_migration_states[1], self.migration
        )

        self.assertIs(bound.atomic, operation.atomic)
        self.assertIs(bound.reversible, operation.reversible)
        self.assertEqual(bound.describe(), operation.describe())


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class SharedStatesMigrationExecutorApplyFromSharedStatesTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        self.executor = SharedStatesMigrationExecutor(connection)
        self.migration = self.executor.loader.graph.nodes[INITIAL_MIGRATION]
        self.shared_operation_states = SharedOperationStates()

    def test_applies_from_the_shared_states_as_migrate_does_for_one_node(self):
        """
        Case: Apply a migration from the shared states, with fake and fake_initial set.
        Expected: The same steps as Django's migrate for a plan of one node, in order. Create the schema's migrations
                  table. Apply the migration from the run's shared states, with both flags. Record the squashes that are
                  complete.
        """
        calls = []
        with (
            mock.patch.object(self.executor.recorder, 'ensure_schema', side_effect=lambda: calls.append('table')),
            mock.patch.object(
                self.executor,
                'apply_migration_between_states',
                side_effect=lambda *args, **kwargs: (calls.append('apply'), mock.DEFAULT)[1],
            ) as mock_apply,
            mock.patch.object(self.executor, 'check_replacements', side_effect=lambda: calls.append('squashes')),
        ):
            self.executor.apply_from_shared_states(
                self.migration, self.shared_operation_states, fake=True, fake_initial=True
            )

        self.assertEqual(calls, ['table', 'apply', 'squashes'])
        mock_apply.assert_called_once_with(
            self.shared_operation_states.get_states_around_operations(self.executor, self.migration),
            self.migration,
            fake=True,
            fake_initial=True,
        )

    def test_leaves_the_migration_applied(self):
        """
        Case: Apply a migration from the shared states.
        Expected: It is applied and recorded for the schema.
        """
        self.executor.apply_from_shared_states(self.migration, self.shared_operation_states)

        self.assertTableExists('migration_tests_author')
        self.assertIn(INITIAL_MIGRATION, self.executor.recorder.applied_migrations())

    def count_renders_for_second_schema(self, fake_initial):
        """
        Apply the migration from the shared states to the template schemas of both nodes. Both schemas are at the same
        point. Return how many times historical models were rendered for the second schema.
        """
        with use_shard(node_name='default', schema_name=get_template_name()) as env:
            SharedStatesMigrationExecutor(env.connection).apply_from_shared_states(
                self.migration, self.shared_operation_states, fake_initial=fake_initial
            )

        with use_shard(node_name='other', schema_name=get_template_name()) as env:
            executor = SharedStatesMigrationExecutor(env.connection)
            with mock.patch.object(
                StateApps, '__init__', autospec=True, side_effect=StateApps.__init__
            ) as mock_state_apps_init:
                executor.apply_from_shared_states(
                    self.migration, self.shared_operation_states, fake_initial=fake_initial
                )

            self.assertIn('migration_tests_author', env.connection.introspection.table_names())

        return mock_state_apps_init.call_count

    def test_second_schema_renders_no_models(self):
        """
        Case: Apply a migration from the shared states to two schemas at the same point.
        Expected: No historical models are rendered for the second schema. It migrates between the states rendered for
                  the first, whichever way Django's executor reaches them.
        """
        self.assertEqual(self.count_renders_for_second_schema(fake_initial=False), 0)

    def test_second_schema_renders_no_models_with_fake_initial(self):
        """
        Case: Apply a migration from the shared states to two schemas at the same point, with fake_initial=True.
        Expected: No historical models are rendered for the second schema. This includes Django's check for an
                  already-applied initial migration, which asks for the migration's end state.
        """
        self.assertEqual(self.count_renders_for_second_schema(fake_initial=True), 0)


SQUASHED_MIGRATION = ('migration_tests', '0001_squashed_0002')


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_squashed'})
class SharedStatesMigrationExecutorReplacementsTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        self.executor = SharedStatesMigrationExecutor(connection)

    def test_check_replacements_keeps_applied_set_current(self):
        """
        Case: Record every migration a squash replaces, then check the replacements.
        Expected: The squash is recorded as applied, both in the database and in the loader's applied set. So the
                  executor kept for the whole run agrees with the database.
        """
        self.executor.record_migration(*INITIAL_MIGRATION)
        self.executor.record_migration(*SECOND_MIGRATION)

        self.executor.check_replacements()

        self.assertIn(SQUASHED_MIGRATION, self.executor.recorder.applied_migrations())
        self.assertIn(SQUASHED_MIGRATION, self.executor.loader.applied_migrations)

    def test_check_replacements_reads_the_database_until_nothing_can_change(self):
        """
        Case: Check the replacements of an app with a squashed migration twice. Then record each migration it replaces,
              and check after each.
        Expected: The first check reads the applied migrations from the database, the same as Django. (A squash can be
                  complete there without being recorded.) The second check reads nothing, since nothing was recorded in
                  between. After each recorded migration the next check reads again, and the last check records the
                  squash.
        """
        with mock.patch.object(
            self.executor.recorder, 'applied_migrations', wraps=self.executor.recorder.applied_migrations
        ) as mock_applied_migrations:
            self.executor.check_replacements()
            self.executor.check_replacements()
            self.assertEqual(mock_applied_migrations.call_count, 1)

            self.executor.record_migration(*INITIAL_MIGRATION)
            self.executor.check_replacements()
            self.assertEqual(mock_applied_migrations.call_count, 2)
            self.assertNotIn(SQUASHED_MIGRATION, self.executor.loader.applied_migrations)

            self.executor.record_migration(*SECOND_MIGRATION)
            self.executor.check_replacements()
            self.assertEqual(mock_applied_migrations.call_count, 3)

        self.assertIn(SQUASHED_MIGRATION, self.executor.recorder.applied_migrations())
        self.assertIn(SQUASHED_MIGRATION, self.executor.loader.applied_migrations)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
@mock.patch.object(MigrationLoader, 'load_disk', autospec=True, side_effect=MigrationLoader.load_disk)
class SharedStatesMigrationExecutorLoadedFilesTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def test_executors_sharing_loaded_migration_files_load_them_once(self, mock_load_disk):
        """
        Case: Build two executors that get the same dict for sharing what they load from disk.
        Expected: The migration modules are loaded from disk once. Both loaders hold the same migrations and build their
                  graphs from them.
        """
        loaded_migration_files = {}
        first = SharedStatesMigrationExecutor(connection, loaded_migration_files=loaded_migration_files)
        second = SharedStatesMigrationExecutor(connection, loaded_migration_files=loaded_migration_files)

        self.assertEqual(mock_load_disk.call_count, 1)
        self.assertIs(second.loader.disk_migrations, first.loader.disk_migrations)
        self.assertIn(INITIAL_MIGRATION, second.loader.graph.nodes)

    def test_executors_with_their_own_loaded_migration_files_load_them_each(self, mock_load_disk):
        """
        Case: Build two executors that each get their own dict, or none.
        Expected: Each loads the migration modules from disk itself.
        """
        SharedStatesMigrationExecutor(connection, loaded_migration_files={})
        SharedStatesMigrationExecutor(connection)

        self.assertEqual(mock_load_disk.call_count, 2)

    def test_applied_migrations_hold_the_rows_and_the_recorded_migrations(self, mock_load_disk):
        """
        Case: Build an executor for a schema that has applied two migrations, one on disk and one not. Then record
              another migration through it.
        Expected: Its applied set holds the database row for each migration applied before. For the recorded migration
                  it holds the migration from disk. (Django's loader does the same: rows for applied migrations, and the
                  migration itself for a squash it finds applied.)
        """
        MigrationExecutor(connection).migrate([INITIAL_MIGRATION])
        MigrationRecorder(connection).record_applied('migration_tests', '0099_gone')
        executor = SharedStatesMigrationExecutor(connection)

        executor.record_migration(*SECOND_MIGRATION)

        for key in (INITIAL_MIGRATION, ('migration_tests', '0099_gone')):
            with self.subTest(key=key):
                row = executor.loader.applied_migrations[key]
                self.assertIsInstance(row, MigrationRecorder.Migration)
                self.assertEqual((row.app, row.name), key)
                self.assertIsNotNone(row.applied)

        self.assertIs(
            executor.loader.applied_migrations[SECOND_MIGRATION], executor.loader.disk_migrations[SECOND_MIGRATION]
        )


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
@mock.patch.object(
    SharedStatesMigrationExecutor,
    '_create_project_state',
    autospec=True,
    side_effect=MigrationExecutor._create_project_state,
)
class SharedOperationStatesTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        self.shared_operation_states = SharedOperationStates()
        self.initial = SharedStatesMigrationExecutor(connection).loader.graph.nodes[INITIAL_MIGRATION]
        self.second = SharedStatesMigrationExecutor(connection).loader.graph.nodes[SECOND_MIGRATION]
        self.third = SharedStatesMigrationExecutor(connection).loader.graph.nodes[THIRD_MIGRATION]

    def executor_with_applied_migrations(self, *applied):
        executor = SharedStatesMigrationExecutor(connection)
        for key in applied:
            executor.loader.applied_migrations[key] = None

        return executor

    def test_same_point_shares_states(self, mock_create_project_state):
        """
        Case: Ask for the states of a migration for two schemas that have applied the same migrations.
        Expected: Both get the same list, built once.
        """
        states = self.shared_operation_states.get_states_around_operations(
            self.executor_with_applied_migrations(), self.initial
        )

        self.assertIs(
            self.shared_operation_states.get_states_around_operations(
                self.executor_with_applied_migrations(), self.initial
            ),
            states,
        )
        self.assertEqual(mock_create_project_state.call_count, 1)

    def test_different_points_get_their_own_states(self, mock_create_project_state):
        """
        Case: Ask for the states of a migration for two schemas that have applied different migrations.
        Expected: Each gets its own list, built from its own applied migrations. (Django would build the same list.)
        """
        states = self.shared_operation_states.get_states_around_operations(
            self.executor_with_applied_migrations(INITIAL_MIGRATION), self.third
        )
        other_states = self.shared_operation_states.get_states_around_operations(
            self.executor_with_applied_migrations(INITIAL_MIGRATION, SECOND_MIGRATION), self.third
        )

        self.assertIsNot(other_states, states)
        self.assertIn(('migration_tests', 'tribble'), states[0].models)
        self.assertNotIn(('migration_tests', 'tribble'), other_states[0].models)
        self.assertEqual(mock_create_project_state.call_count, 2)

    def test_advance_starts_the_next_migration_where_the_last_ended(self, mock_create_project_state):
        """
        Case: Get the states of a migration and advance past it. Then get the next migration's states for a schema that
              has applied the first.
        Expected: The next migration starts from the end state of the first, without building a state from scratch.
        """
        states = self.shared_operation_states.get_states_around_operations(
            self.executor_with_applied_migrations(), self.initial
        )
        self.shared_operation_states.advance_to_next_migration()

        next_states = self.shared_operation_states.get_states_around_operations(
            self.executor_with_applied_migrations(INITIAL_MIGRATION), self.second
        )

        self.assertIs(next_states[0], states[-1])
        self.assertEqual(mock_create_project_state.call_count, 1)

    def test_different_migrations_at_the_same_point_get_their_own_states(self, mock_create_project_state):
        """
        Case: Ask for the states of two different migrations for schemas at the same point, without advancing in
              between.
        Expected: Each migration gets the states around its own operations. Both start from one state built for that
                  point.
        """
        states = self.shared_operation_states.get_states_around_operations(
            self.executor_with_applied_migrations(INITIAL_MIGRATION), self.second
        )
        other_states = self.shared_operation_states.get_states_around_operations(
            self.executor_with_applied_migrations(INITIAL_MIGRATION), self.third
        )

        self.assertIsNot(other_states, states)
        self.assertEqual(len(states), len(self.second.operations) + 1)
        self.assertEqual(len(other_states), len(self.third.operations) + 1)
        self.assertIs(other_states[0], states[0])
        self.assertEqual(mock_create_project_state.call_count, 1)

    def test_advance_forgets_points_no_schema_moved_from(self, mock_create_project_state):
        """
        Case: Get the states of a migration and advance past it. Then get the next migration's states for a schema that
              did not apply the first.
        Expected: That schema's state is built from scratch, not taken from the schemas that applied it. So a schema
                  that falls out of step still gets its own state.
        """
        states = self.shared_operation_states.get_states_around_operations(
            self.executor_with_applied_migrations(), self.initial
        )
        self.shared_operation_states.advance_to_next_migration()

        # 0003_third only creates a model, so it applies to a schema at any point.
        next_states = self.shared_operation_states.get_states_around_operations(
            self.executor_with_applied_migrations(), self.third
        )

        self.assertIsNot(next_states[0], states[-1])
        self.assertNotIn(('migration_tests', 'author'), next_states[0].models)
        self.assertEqual(mock_create_project_state.call_count, 2)

    def test_render_is_reported_through_the_executors_callback(self, mock_create_project_state):
        """
        Case: Get the states of a migration for an executor with a progress callback.
        Expected: The callback is called for the render of the migration's starting state. (Django's executor calls it
                  too.)
        """
        executor = self.executor_with_applied_migrations()
        executor.progress_callback = mock.Mock()

        self.shared_operation_states.get_states_around_operations(executor, self.initial)

        self.assertEqual(
            executor.progress_callback.call_args_list, [mock.call('render_start'), mock.call('render_success')]
        )

    def test_state_rendered_before_the_first_operation(self, mock_create_project_state):
        """
        Case: Get the states of a migration.
        Expected: Its starting state has its models rendered. (Django also renders it first, so each operation
                  re-renders only what it touches.)
        """
        states = self.shared_operation_states.get_states_around_operations(
            self.executor_with_applied_migrations(), self.initial
        )

        self.assertIn('apps', states[0].__dict__)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
@mock.patch.object(
    MigrationExecutor,
    '_create_project_state',
    autospec=True,
    side_effect=MigrationExecutor._create_project_state,
)
class SharedStartingStatesTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        self.shared_starting_states = SharedStartingStates()
        self.initial = SharedFilesMigrationExecutor(connection).loader.graph.nodes[INITIAL_MIGRATION]

    def executor_with_applied_migrations(self, *applied):
        executor = SharedFilesMigrationExecutor(connection)
        for key in applied:
            executor.loader.applied_migrations[key] = None

        return executor

    def state_built_by_django(self, *applied):
        """
        Return the state Django's own executor builds from scratch for a schema that has applied the given migrations.
        """
        return MigrationExecutor._create_project_state(
            self.executor_with_applied_migrations(*applied), with_applied_migrations=True
        )

    def test_same_point_builds_the_state_once(self, mock_create_project_state):
        """
        Case: Ask for the starting state of two schemas that have applied the same migrations.
        Expected: The state is built once. Each schema gets its own copy, equal to the state Django builds.
        """
        state = self.shared_starting_states.get_starting_state(self.executor_with_applied_migrations(INITIAL_MIGRATION))
        other_state = self.shared_starting_states.get_starting_state(
            self.executor_with_applied_migrations(INITIAL_MIGRATION)
        )

        self.assertEqual(mock_create_project_state.call_count, 1)
        self.assertIsNot(other_state, state)
        self.assertEqual(state, self.state_built_by_django(INITIAL_MIGRATION))
        self.assertEqual(other_state, self.state_built_by_django(INITIAL_MIGRATION))

    def test_copies_are_handed_out_unrendered(self, mock_create_project_state):
        """
        Case: Ask for the starting state of a schema, then render the copy's models and change it. Then ask again for a
              schema at the same point.
        Expected: Each copy comes unrendered, and changes to the first do not reach the second. Each schema renders its
                  own historical models. (Django also renders them for every schema.)
        """
        state = self.shared_starting_states.get_starting_state(self.executor_with_applied_migrations(INITIAL_MIGRATION))
        self.assertNotIn('apps', state.__dict__)
        state.apps
        state.remove_model('migration_tests', 'author')

        other_state = self.shared_starting_states.get_starting_state(
            self.executor_with_applied_migrations(INITIAL_MIGRATION)
        )

        self.assertNotIn('apps', other_state.__dict__)
        self.assertIn(('migration_tests', 'author'), other_state.models)

    def test_different_points_get_their_own_state(self, mock_create_project_state):
        """
        Case: Ask for the starting state of two schemas that have applied different migrations.
        Expected: Each gets the state Django builds for its applied migrations.
        """
        state = self.shared_starting_states.get_starting_state(self.executor_with_applied_migrations())
        other_state = self.shared_starting_states.get_starting_state(
            self.executor_with_applied_migrations(INITIAL_MIGRATION)
        )

        self.assertEqual(mock_create_project_state.call_count, 2)
        self.assertEqual(state, self.state_built_by_django())
        self.assertEqual(other_state, self.state_built_by_django(INITIAL_MIGRATION))

    def test_advance_moves_each_point_past_the_migration(self, mock_create_project_state):
        """
        Case: Ask for the starting state of a schema and advance past a migration. Then ask again for a schema that has
              applied it.
        Expected: The state is not built from scratch again, and it equals the state Django builds for that point.
        """
        self.shared_starting_states.get_starting_state(self.executor_with_applied_migrations())
        self.shared_starting_states.advance_past_migration(self.initial)

        state = self.shared_starting_states.get_starting_state(self.executor_with_applied_migrations(INITIAL_MIGRATION))

        self.assertEqual(mock_create_project_state.call_count, 1)
        self.assertEqual(state, self.state_built_by_django(INITIAL_MIGRATION))

    def test_advance_forgets_points_no_schema_moved_from(self, mock_create_project_state):
        """
        Case: Ask for the starting state of a schema and advance past a migration twice. Then ask for a schema at the
              point the first advance reached.
        Expected: That state is built from scratch. An advance only moves the points that schemas asked for since the
                  previous advance.
        """
        self.shared_starting_states.get_starting_state(self.executor_with_applied_migrations())
        self.shared_starting_states.advance_past_migration(self.initial)
        self.shared_starting_states.advance_past_migration(self.initial)

        state = self.shared_starting_states.get_starting_state(self.executor_with_applied_migrations(INITIAL_MIGRATION))

        self.assertEqual(mock_create_project_state.call_count, 2)
        self.assertEqual(state, self.state_built_by_django(INITIAL_MIGRATION))

    @mock.patch.object(Migration, 'mutate_state', autospec=True, side_effect=Migration.mutate_state)
    def test_advance_moves_the_points_when_a_schema_next_asks(self, mock_mutate_state, mock_create_project_state):
        """
        Case: Ask for the starting state of a schema and advance past a migration. Then ask again for a schema that has
              applied it.
        Expected: The point moves past the migration only when a schema asks again. So nothing moves after the last
                  migration of a run, or after a migration that stops the run.
        """
        self.shared_starting_states.get_starting_state(self.executor_with_applied_migrations())
        mock_mutate_state.reset_mock()
        self.shared_starting_states.advance_past_migration(self.initial)

        self.assertFalse(mock_mutate_state.called)

        self.shared_starting_states.get_starting_state(self.executor_with_applied_migrations(INITIAL_MIGRATION))

        self.assertEqual(mock_mutate_state.call_count, 1)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class SharedFilesMigrationExecutorTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def test_starting_state_comes_from_the_starting_states(self):
        """
        Case: Ask an executor that has starting states for the state of its applied migrations.
        Expected: The starting states return it.
        """
        starting_states = mock.Mock(spec=SharedStartingStates)
        executor = SharedFilesMigrationExecutor(connection, starting_states=starting_states)

        state = executor._create_project_state(with_applied_migrations=True)

        starting_states.get_starting_state.assert_called_once_with(executor)
        self.assertIs(state, starting_states.get_starting_state.return_value)

    def test_empty_state_is_djangos(self):
        """
        Case: Ask an executor that has starting states for a state without applied migrations.
        Expected: Django's code builds an empty state, without asking the starting states.
        """
        starting_states = mock.Mock(spec=SharedStartingStates)
        executor = SharedFilesMigrationExecutor(connection, starting_states=starting_states)

        state = executor._create_project_state()

        self.assertFalse(starting_states.get_starting_state.called)
        self.assertEqual(state.models, {})

    @mock.patch.object(
        MigrationExecutor, '_create_project_state', autospec=True, side_effect=MigrationExecutor._create_project_state
    )
    def test_without_starting_states_builds_as_django_does(self, mock_create_project_state):
        """
        Case: Ask an executor without starting states for the state of its applied migrations.
        Expected: Django builds it.
        """
        executor = SharedFilesMigrationExecutor(connection)

        executor._create_project_state(with_applied_migrations=True)

        mock_create_project_state.assert_called_once_with(executor, with_applied_migrations=True)

    def test_migrate_applies_from_the_starting_states(self):
        """
        Case: Migrate two schemas forwards one migration at a time, through executors that share starting states. (The
              command migrates the same way.)
        Expected: Both schemas get the tables. The starting state of each migration is built once for both.
        """
        starting_states = SharedStartingStates()
        loaded_migration_files = {}
        schemas = [('default', 'public'), ('default', 'template')]
        plan = SharedFilesMigrationExecutor(connection).migration_plan([THIRD_MIGRATION])

        with mock.patch.object(
            MigrationExecutor,
            '_create_project_state',
            autospec=True,
            side_effect=MigrationExecutor._create_project_state,
        ) as mock_create_project_state:
            for node in plan:
                for node_name, schema_name in schemas:
                    with use_shard(node_name=node_name, schema_name=schema_name) as env:
                        executor = SharedFilesMigrationExecutor(
                            env.connection,
                            loaded_migration_files=loaded_migration_files,
                            starting_states=starting_states,
                        )
                        executor.migrate(targets=None, plan=[node])
                starting_states.advance_past_migration(node[0])

        # Built from scratch once for the public schema and once for the template. They start from different points,
        # because the public schema holds the migrations applied when the test database was built.
        self.assertEqual(mock_create_project_state.call_count, 2)
        for node_name, schema_name in schemas:
            with self.subTest(schema_name=schema_name):
                with use_shard(node_name=node_name, schema_name=schema_name, include_public=False) as env:
                    self.assertIn('migration_tests_book', env.connection.introspection.table_names())

        for node_name, schema_name in schemas:
            with use_shard(node_name=node_name, schema_name=schema_name) as env:
                MigrationExecutor(env.connection).migrate([('migration_tests', None)])


DATA_MIGRATION = ('migration_tests', '0099_data')


class CachedLabelManager(Manager):
    """
    A manager for the historical models. It computes a value once per manager, on first use.
    """

    use_in_migrations = True

    @cached_property
    def cached_label(self):
        return 'authors'


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class HistoricalModelsGuardTestCase(MigrationTestCase):
    """
    Every schema in a run shares the historical models that a RunPython gets. A data migration that leaves state on them
    would pass it on to the schemas that follow, so the migration fails instead.
    """

    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        self.executor = SharedStatesMigrationExecutor(connection)
        self.executor.migrate([INITIAL_MIGRATION])
        self.executor.loader.build_graph()

    def apply_data_migration_between_states(self, *operations, author_managers=None, atomic=True):
        """
        Apply a migration made of operations from precomputed states. (The command applies it to each schema the same
        way.) Keep the historical Author model that its RunPython operations get as self.author. Give that model
        author_managers as its managers, if given. The migration is atomic unless atomic is False.
        """
        migration = Migration(DATA_MIGRATION[1], DATA_MIGRATION[0])
        migration.atomic = atomic
        migration.operations = list(operations)
        state = self.executor._create_project_state(with_applied_migrations=True)
        if author_managers is not None:
            state.models[('migration_tests', 'author')].managers = author_managers
        # Render the state before the first operation. SharedOperationStates does this too.
        state.apps
        states = compute_states_around_migration_operations(migration, state)
        self.author = states[0].apps.get_model('migration_tests', 'Author')

        self.executor.apply_migration_between_states(states, migration)

    def assert_data_migration_fails(self, *operations, atomic=True):
        with self.assertRaises(HistoricalModelsChanged) as context:
            self.apply_data_migration_between_states(*operations, atomic=atomic)

        self.assertNotIn(DATA_MIGRATION, self.executor.recorder.applied_migrations())
        return str(context.exception)

    def test_changes_of_an_atomic_data_migration_are_rolled_back(self):
        """
        Case: Run a data migration that writes a row and then memoises a value on a historical model class.
        Expected: It fails, the attribute is removed again, and the row is gone. The migration ran in a transaction, and
                  the error rolls it back. The error message says so.
        """

        def write_and_memoise(apps, schema_editor):
            author = apps.get_model('migration_tests', 'Author')
            author.objects.using(schema_editor.connection.alias).create(name='Ada', slug='ada')
            author._default_name = 'shard one'

        message = self.assert_data_migration_fails(RunPython(write_and_memoise))

        self.assertIn('rolled back', message)
        self.assertNotIn('_default_name', vars(self.author))
        self.assertFalse(self.author.objects.using(connection.alias).filter(slug='ada').exists())

    def test_changes_of_a_non_atomic_data_migration_stay(self):
        """
        Case: Run a data migration with atomic = False. Its RunPython, also with atomic=False, writes a row and then
              memoises a value on a historical model class.
        Expected: It fails and is not recorded, the attribute is removed again, and the row stays. The RunPython ran
                  without a transaction, so there is nothing to roll back (the same as for any other error there).
                  Running the migration again on this schema runs the RunPython again.
        """

        def write_and_memoise(apps, schema_editor):
            author = apps.get_model('migration_tests', 'Author')
            author.objects.using(schema_editor.connection.alias).create(name='Ada', slug='ada')
            author._default_name = 'shard one'

        self.assert_data_migration_fails(RunPython(write_and_memoise, atomic=False), atomic=False)

        self.assertNotIn('_default_name', vars(self.author))
        self.assertTrue(self.author.objects.using(connection.alias).filter(slug='ada').exists())

    def test_adding_a_class_attribute_fails(self):
        """
        Case: Run a data migration that memoises a value on a historical model class.
        Expected: It fails, naming the migration and the attribute, and is not recorded as applied. The attribute is
                  removed again, so later schemas do not see it.
        """

        def memoise(apps, schema_editor):
            apps.get_model('migration_tests', 'Author')._default_name = 'shard one'

        message = self.assert_data_migration_fails(RunPython(memoise))

        self.assertIn('migration_tests.0099_data', message)
        self.assertIn('Author._default_name', message)
        self.assertNotIn('_default_name', vars(self.author))

    def test_reassigning_a_class_attribute_fails(self):
        """
        Case: Run a data migration that replaces an attribute a historical model class already has.
        Expected: It fails, and the original attribute is put back.
        """
        original = {}

        def replace_class_attribute(apps, schema_editor):
            author = apps.get_model('migration_tests', 'Author')
            original['DoesNotExist'] = author.DoesNotExist
            author.DoesNotExist = type('DoesNotExist', (Exception,), {})

        message = self.assert_data_migration_fails(RunPython(replace_class_attribute))

        self.assertIn('Author.DoesNotExist', message)
        self.assertIs(self.author.DoesNotExist, original['DoesNotExist'])

    def test_removing_a_class_attribute_fails(self):
        """
        Case: Run a data migration that deletes an attribute of a historical model class.
        Expected: It fails, and the attribute is put back.
        """
        original = {}

        def remove_class_attribute(apps, schema_editor):
            author = apps.get_model('migration_tests', 'Author')
            original['MultipleObjectsReturned'] = author.MultipleObjectsReturned
            del author.MultipleObjectsReturned

        message = self.assert_data_migration_fails(RunPython(remove_class_attribute))

        self.assertIn('Author.MultipleObjectsReturned', message)
        self.assertIs(self.author.MultipleObjectsReturned, original['MultipleObjectsReturned'])

    def test_adding_a_manager_attribute_fails(self):
        """
        Case: Run a data migration that memoises a value on the manager of a historical model.
        Expected: It fails, naming the manager, and the attribute is removed again.
        """

        def memoise(apps, schema_editor):
            apps.get_model('migration_tests', 'Author').objects._cached = 1

        message = self.assert_data_migration_fails(RunPython(memoise))

        self.assertIn('Author.objects._cached', message)
        self.assertNotIn('_cached', vars(self.author.objects))

    def test_run_python_within_separate_database_and_state_is_guarded(self):
        """
        Case: Run a data migration whose RunPython is one of the database operations of a SeparateDatabaseAndState.
        Expected: It fails the same way.
        """

        def memoise(apps, schema_editor):
            apps.get_model('migration_tests', 'Author')._default_name = 'shard one'

        self.assert_data_migration_fails(SeparateDatabaseAndState(database_operations=[RunPython(memoise)]))

        self.assertNotIn('_default_name', vars(self.author))

    def test_run_python_within_separate_database_and_state_after_a_delayed_field_is_guarded(self):
        """
        Case: Run a data migration that adds a non-relational field and then memoises a value on a historical model
              class. The memoising RunPython is a database operation of a SeparateDatabaseAndState. (The AddField delays
              re-rendering the related models.)
        Expected: It fails, naming the attribute. The attribute is removed from the class the RunPython got.
        """
        received_models = {}

        def memoise(apps, schema_editor):
            received_models['author'] = apps.get_model('migration_tests', 'Author')
            received_models['author']._default_name = 'shard one'

        message = self.assert_data_migration_fails(
            AddField('Author', 'pages', IntegerField(default=0)),
            SeparateDatabaseAndState(database_operations=[RunPython(memoise)]),
        )

        self.assertIn('Author._default_name', message)
        self.assertNotIn('_default_name', vars(received_models['author']))

    def test_run_python_after_a_database_operation_within_separate_database_and_state_is_guarded(self):
        """
        Case: Run a data migration with a SeparateDatabaseAndState. Its database operations add a non-relational field
              and then memoise a value on a historical model class in a RunPython.
        Expected: It fails, naming the attribute. The attribute is removed from the class the RunPython got.
        """
        received_models = {}

        def memoise(apps, schema_editor):
            received_models['author'] = apps.get_model('migration_tests', 'Author')
            received_models['author']._default_name = 'shard one'

        message = self.assert_data_migration_fails(
            SeparateDatabaseAndState(
                database_operations=[AddField('Author', 'pages', IntegerField(default=0)), RunPython(memoise)]
            )
        )

        self.assertIn('Author._default_name', message)
        self.assertNotIn('_default_name', vars(received_models['author']))

    def test_run_python_within_a_separate_database_and_state_subclass_is_guarded(self):
        """
        Case: Run a data migration whose RunPython is a database operation of a SeparateDatabaseAndState subclass. The
              subclass has its own database_forwards, which calls Django's.
        Expected: It fails the same way. The subclass runs as a whole and is checked as a whole.
        """

        class LoggingSeparateDatabaseAndState(SeparateDatabaseAndState):
            def database_forwards(self, app_label, schema_editor, from_state, to_state):
                super().database_forwards(app_label, schema_editor, from_state, to_state)

        def memoise(apps, schema_editor):
            apps.get_model('migration_tests', 'Author')._default_name = 'shard one'

        message = self.assert_data_migration_fails(
            LoggingSeparateDatabaseAndState(database_operations=[RunPython(memoise)])
        )

        self.assertIn('Author._default_name', message)
        self.assertNotIn('_default_name', vars(self.author))

    @mock.patch(
        'djanquiltdb.management.executor.snapshot_historical_model_attributes',
        wraps=snapshot_historical_model_attributes,
    )
    def test_noop_run_python_is_not_checked(self, mock_snapshot_historical_model_attributes):
        """
        Case: Run a data migration whose RunPython runs RunPython.noop.
        Expected: It is applied without collecting the attributes of the historical models. No migration code runs.
        """
        self.apply_data_migration_between_states(RunPython(RunPython.noop))

        self.assertIn(DATA_MIGRATION, self.executor.recorder.applied_migrations())
        self.assertFalse(mock_snapshot_historical_model_attributes.called)

    @mock.patch(
        'djanquiltdb.management.executor.snapshot_historical_model_attributes',
        wraps=snapshot_historical_model_attributes,
    )
    def test_run_python_the_router_does_not_allow_is_not_checked(self, mock_snapshot_historical_model_attributes):
        """
        Case: Run a data migration whose RunPython would memoise a value on a historical model class. The router does
              not allow it on the schema.
        Expected: It is applied. Its code does not run, and the attributes of the historical models are not collected.
        """
        ran = []

        def memoise(apps, schema_editor):
            ran.append(True)
            apps.get_model('migration_tests', 'Author')._default_name = 'shard one'

        with mock.patch.object(router, 'allow_migrate', return_value=False):
            self.apply_data_migration_between_states(RunPython(memoise))

        self.assertEqual(ran, [])
        self.assertIn(DATA_MIGRATION, self.executor.recorder.applied_migrations())
        self.assertFalse(mock_snapshot_historical_model_attributes.called)

    def test_run_python_subclass_is_checked(self):
        """
        Case: Run a data migration with a RunPython subclass whose code is RunPython.noop. Its own database_forwards
              memoises a value on a historical model class.
        Expected: It fails. Only a plain RunPython is known to run nothing for RunPython.noop.
        """

        class MemoisingRunPython(RunPython):
            def database_forwards(self, app_label, schema_editor, from_state, to_state):
                from_state.apps.get_model('migration_tests', 'Author')._default_name = 'shard one'

        message = self.assert_data_migration_fails(MemoisingRunPython(RunPython.noop))

        self.assertIn('Author._default_name', message)

    def test_changes_are_undone_when_the_data_migration_raises(self):
        """
        Case: Run a data migration that memoises a value on a historical model class and then raises an error.
        Expected: The migration's own error is raised. The attribute is still removed, so it does not carry over.
        """

        def memoise_and_fail(apps, schema_editor):
            apps.get_model('migration_tests', 'Author')._default_name = 'shard one'
            raise ValueError('no default author')

        with self.assertRaisesMessage(ValueError, 'no default author'):
            self.apply_data_migration_between_states(RunPython(memoise_and_fail))

        self.assertNotIn('_default_name', vars(self.author))

    def test_reading_the_annotations_of_a_class_passes(self):
        """
        Case: Run a data migration that reads the annotations of a historical model class. (Python caches them in the
              class's own attributes.)
        Expected: It is applied, and the cache is removed again, so it does not carry over.
        """

        def read_annotations(apps, schema_editor):
            apps.get_model('migration_tests', 'Author').__annotations__

        self.apply_data_migration_between_states(RunPython(read_annotations))

        self.assertIn(DATA_MIGRATION, self.executor.recorder.applied_migrations())
        self.assertNotIn('__annotations_cache__', vars(self.author))

    def test_reading_a_cached_property_of_a_manager_passes(self):
        """
        Case: Run a data migration that reads a cached_property of a historical model's manager. (The property stores
              the value in the manager's own attributes.)
        Expected: It is applied, and the cached value is removed again, so it does not carry over.
        """
        read = {}

        def read_label(apps, schema_editor):
            read['label'] = apps.get_model('migration_tests', 'Author').objects.cached_label

        self.apply_data_migration_between_states(
            RunPython(read_label), author_managers=[('objects', CachedLabelManager())]
        )

        self.assertEqual(read['label'], 'authors')
        self.assertIn(DATA_MIGRATION, self.executor.recorder.applied_migrations())
        self.assertNotIn('label', vars(self.author.objects))

    def test_data_migration_using_the_models_passes(self):
        """
        Case: Run a data migration that queries and writes through the historical models. It keeps what it needs in
              local variables.
        Expected: It is applied. Using the models leaves the classes and their managers unchanged.
        """

        def create_and_rename(apps, schema_editor):
            authors = apps.get_model('migration_tests', 'Author').objects.using(schema_editor.connection.alias)
            author = authors.create(name='Ada', slug='ada')
            authors.filter(pk=author.pk).update(name='Ada Lovelace')
            list(authors.filter(name__startswith='Ada').values_list('name', flat=True))

        self.apply_data_migration_between_states(RunPython(create_and_rename))

        self.assertIn(DATA_MIGRATION, self.executor.recorder.applied_migrations())
        self.assertTrue(self.author.objects.using(connection.alias).filter(name='Ada Lovelace').exists())
