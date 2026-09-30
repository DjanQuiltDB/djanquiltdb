import copy
import functools
import inspect
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar

from django.db import router
from django.db.migrations import RunPython, SeparateDatabaseAndState
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.recorder import MigrationRecorder
from django.db.migrations.state import ProjectState
from django.utils.functional import cached_property

ATTRIBUTE_MISSING = object()

# Whether migrate shares the project states of each migration between schemas.
_shared_states_enabled = ContextVar('shared_states_enabled', default=False)


@contextmanager
def enable_shared_migration_states():
    """
    Make migrate share the project states of each migration between schemas, for the duration of the block. The database
    creation uses this while Django builds a new test database, where every schema starts empty.
    """
    token = _shared_states_enabled.set(True)
    try:
        yield
    finally:
        _shared_states_enabled.reset(token)


def shared_migration_states_enabled():
    """
    Return whether migrate shares the project states of each migration between schemas.
    """
    return _shared_states_enabled.get()


class HistoricalModelsChanged(Exception):
    """
    Raised when a data migration leaves state on the historical models. Those models are shared by every schema in the
    run.
    """

    pass


def is_or_contains_run_python(operation):
    """
    Return whether the operation runs data migration code. This is a RunPython, or a SeparateDatabaseAndState with a
    RunPython among its database operations.
    """
    if isinstance(operation, RunPython):
        return True
    if isinstance(operation, SeparateDatabaseAndState):
        return any(
            is_or_contains_run_python(database_operation) for database_operation in operation.database_operations
        )
    return False


def is_plain_separate_database_and_state(operation):
    """
    Return whether the operation is a plain SeparateDatabaseAndState, so we can run its database operations between
    states we computed ourselves. A subclass may run them differently, so it is run as a whole.
    """
    return type(operation) is SeparateDatabaseAndState


def is_plain_run_python_without_code_for_schema(operation, app_label, schema_editor):
    """
    Return whether the operation is a plain RunPython that runs no code on this schema. That is the case when its code
    is RunPython.noop, or when the router does not allow it on the schema (checked the same way RunPython checks it). A
    subclass may run code of its own, so it never counts.
    """
    if type(operation) is not RunPython:
        return False
    if operation.code is RunPython.noop:
        return True
    return not router.allow_migrate(schema_editor.connection.alias, app_label, **operation.hints)


def snapshot_historical_model_attributes(apps):
    """
    Return a copy of the attributes of every historical model class in apps and of their managers, keyed by the class or
    manager.
    """
    attributes = {}
    for model in apps.get_models(include_auto_created=True):
        attributes[model] = dict(vars(model))
        for manager in model._meta.managers:
            attributes[manager] = dict(vars(manager))

    return attributes


def is_attribute_cached_on_first_use(owner, name):
    """
    Return whether the attribute is a value cached on first use: a dunder (such as the annotations Python caches on a
    class) or the value of a cached_property.
    """
    if name.startswith('__') and name.endswith('__'):
        return True
    return isinstance(inspect.getattr_static(type(owner), name, None), (functools.cached_property, cached_property))


def restore_historical_model_attributes(snapshot):
    """
    Put the historical models back as they were in the snapshot, and return the names of the attributes that changed.
    Cached values are put back too, but not returned.
    """
    changed = []
    for owner, before in snapshot.items():
        after = dict(vars(owner))
        names = sorted(
            name
            for name in before.keys() | after.keys()
            if before.get(name, ATTRIBUTE_MISSING) is not after.get(name, ATTRIBUTE_MISSING)
        )
        for name in names:
            if name in before:
                setattr(owner, name, before[name])
            else:
                delattr(owner, name)

        names = [name for name in names if not is_attribute_cached_on_first_use(owner, name)]
        if isinstance(owner, type):
            changed += ['{}.{}'.format(owner.__name__, name) for name in names]
        else:
            changed += ['{}.{}.{}'.format(owner.model.__name__, owner.name, name) for name in names]

    return changed


@contextmanager
def fail_if_historical_models_change(apps, migration):
    """
    Raise HistoricalModelsChanged when the data migration in the block leaves state on the historical models in apps.
    The changes are undone first.

    When the data migration raises an error of its own, the changes are undone as well and that error is raised as is.
    """
    snapshot = snapshot_historical_model_attributes(apps)
    try:
        yield
    except BaseException:
        restore_historical_model_attributes(snapshot)
        raise

    changed = restore_historical_model_attributes(snapshot)
    if changed:
        raise HistoricalModelsChanged(
            '{}.{} changed {} on the historical models. While building a test database, every schema gets the same '
            'historical models, so this change would carry over to the next schema. The change has been undone and '
            'the migration has failed for this schema. If the data migration ran in a transaction, its database '
            'changes are rolled back too. Keep what the data migration needs in local variables of its function '
            "instead, or turn QUILT_DB['SHARED_TEST_MIGRATION_STATES'] off.".format(
                migration.app_label, migration.name, ', '.join(changed)
            )
        )


class StatesAroundOperations(list):
    """
    The project states around each operation in a list, as computed by compute_states_around_migration_operations.

    database_operation_states holds the states around the database operations of each plain SeparateDatabaseAndState
    (see is_plain_separate_database_and_state), keyed by the index of that operation.
    """

    def __init__(self, states=()):
        super().__init__(states)
        self.database_operation_states = {}


def compute_states_around_migration_operations(migration, state, progress_callback=None):
    """
    Return the project states around each operation of the migration, starting from state.

    The list holds one state more than the migration has operations: operation i runs from state i to state i + 1. Each
    state is a clone, built the same way Migration.apply builds it, so the list is never changed afterwards and can be
    used for any number of schemas. The states around the database operations of a plain SeparateDatabaseAndState are
    computed as well.

    The starting state is rendered in place, as Django does before applying a migration, so each operation only
    re-renders the models it touches. progress_callback is told about this render, like Django's executor reports it.
    """
    return compute_states_around_operations(migration.operations, migration.app_label, state, progress_callback)


def compute_states_around_operations(operations, app_label, state, progress_callback=None):
    """
    Return the project states around each operation in operations, for the app app_label. See
    compute_states_around_migration_operations.
    """
    operation_states = StatesAroundOperations([state])
    for index, operation in enumerate(operations):
        if is_or_contains_run_python(operation):
            # RunPython throws away a delayed render of its from_state, which re-renders every model. Rendering the
            # state fully here, once, means the RunPython has nothing to throw away on each schema. (A
            # SeparateDatabaseAndState passes its from_state on to its first database operation.)
            state.clear_delayed_apps_cache()
            state.is_delayed = False
        if 'apps' not in state.__dict__:
            if progress_callback:
                progress_callback('render_start')
            state.apps
            if progress_callback:
                progress_callback('render_success')
        if is_plain_separate_database_and_state(operation):
            operation_states.database_operation_states[index] = compute_states_around_operations(
                operation.database_operations, app_label, state
            )
        state = state.clone()
        operation.state_forwards(app_label, state)
        operation_states.append(state)

    return operation_states


class OperationBoundToStates:
    """
    Wraps an operation together with the precomputed states it runs between, for Migration.apply to run instead of the
    operation itself.

    Migration.apply normally computes the states of each operation as it goes. A wrapped operation ignores those and
    runs between the precomputed states instead, which are the same for every schema. A wrapped plain
    SeparateDatabaseAndState runs each of its database operations between their own precomputed states (held in
    database_operation_states). Any other attribute is looked up on the wrapped operation.

    Operations that run data migration code are run under fail_if_historical_models_change, unless they run no code on
    the schema (see is_plain_run_python_without_code_for_schema).

    This relies on Migration.apply not checking the type of an operation. DjangoCouplingTestCase guards this.
    """

    def __init__(self, operation, from_state, to_state, migration, database_operation_states=None):
        self.operation = operation
        self.from_state = from_state
        self.to_state = to_state
        self.migration = migration
        self.database_operation_states = database_operation_states

    def __getattr__(self, name):
        if name == 'operation':
            # copy.copy asks for this before the attributes are set. Looking it up on the operation would recurse.
            raise AttributeError(name)
        return getattr(self.operation, name)

    def state_forwards(self, app_label, state):
        pass

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if self.database_operation_states is not None:
            for operation in bind_operations_to_states(
                self.operation.database_operations, self.database_operation_states, self.migration
            ):
                operation.database_forwards(app_label, schema_editor, operation.from_state, operation.to_state)
            return

        if is_or_contains_run_python(self.operation) and not is_plain_run_python_without_code_for_schema(
            self.operation, app_label, schema_editor
        ):
            guard = fail_if_historical_models_change(self.from_state.apps, self.migration)
        else:
            guard = nullcontext()

        with guard:
            self.operation.database_forwards(app_label, schema_editor, self.from_state, self.to_state)


def bind_operations_to_states(operations, states, migration):
    """
    Return the operations of the migration, each wrapped with the states around it (see OperationBoundToStates).
    """
    return [
        OperationBoundToStates(operation, from_state, to_state, migration, states.database_operation_states.get(index))
        for index, (operation, from_state, to_state) in enumerate(zip(operations, states, states[1:]))
    ]


class SharedFilesMigrationLoader(MigrationLoader):
    """
    A MigrationLoader that loads the migration files from disk only once for all loaders that share one
    loaded_migration_files dict.

    The files on disk are the same for every schema. Only the applied migrations differ, and those are still read from
    each schema's database.
    """

    def __init__(self, connection, loaded_migration_files):
        self.loaded_migration_files = loaded_migration_files
        super().__init__(connection)

    def load_disk(self):
        if not self.loaded_migration_files:
            super().load_disk()
            self.loaded_migration_files.update(
                disk_migrations=self.disk_migrations,
                migrated_apps=self.migrated_apps,
                unmigrated_apps=self.unmigrated_apps,
            )

        self.disk_migrations = self.loaded_migration_files['disk_migrations']
        self.migrated_apps = self.loaded_migration_files['migrated_apps']
        self.unmigrated_apps = self.loaded_migration_files['unmigrated_apps']


class SharedFilesMigrationExecutor(MigrationExecutor):
    """
    A MigrationExecutor that loads the migration files from disk only once for all executors that share one
    loaded_migration_files dict. When given starting_states, it gets the state each schema starts from there.
    """

    def __init__(self, connection, progress_callback=None, loaded_migration_files=None, starting_states=None):
        # Same as MigrationExecutor.__init__, but with a loader that shares what it loads from disk.
        self.connection = connection
        self.loader = SharedFilesMigrationLoader(
            connection, {} if loaded_migration_files is None else loaded_migration_files
        )
        self.recorder = MigrationRecorder(connection)
        self.progress_callback = progress_callback
        self.starting_states = starting_states

    def _create_project_state(self, with_applied_migrations=False):
        if not with_applied_migrations or self.starting_states is None:
            return super()._create_project_state(with_applied_migrations=with_applied_migrations)

        return self.starting_states.get_starting_state(self)


def applied_migrations_in_graph(loader):
    """
    Return the keys of the migrations applied to the loader's schema that are in its graph. Schemas with the same keys
    are at the same point, and SharedStartingStates and SharedOperationStates use these keys to look up their states.
    """
    return frozenset(key for key in loader.applied_migrations if key in loader.graph.nodes)


class SharedStartingStates:
    """
    The project states schemas start a migration from, keyed by the migrations each schema has applied. Schemas at the
    same point share one state, instead of each building it from scratch.

    The states are kept unrendered, and each schema gets its own copy, which Django then renders and moves forward for
    that schema. So every schema still gets its own historical models.

    Only the points asked for since the last advance are kept. They are moved past the applied migration the next time a
    schema asks for a state.
    """

    def __init__(self):
        # Applied migration keys -> the state those migrations leave behind, unrendered.
        self._states_by_applied_migrations = {}
        # The applied migration keys asked for since the last advance.
        self._asked_applied_migrations = set()
        # The migration of the last advance, which the asked points have not been moved past yet.
        self._pending_migration = None

    def get_starting_state(self, executor):
        """
        Return a copy of the state left behind by the migrations applied to the executor's schema.
        """
        self.move_asked_states_past_pending_migration()

        applied = applied_migrations_in_graph(executor.loader)
        if applied not in self._states_by_applied_migrations:
            self._states_by_applied_migrations[applied] = MigrationExecutor._create_project_state(
                executor, with_applied_migrations=True
            )

        self._asked_applied_migrations.add(applied)
        return self._states_by_applied_migrations[applied].clone()

    def advance_past_migration(self, migration):
        """
        Move on past the migration the run just applied. The points asked for since the last advance are moved past it
        when a schema next asks for a state, so no work is done after the last migration or after an error.
        """
        if self._pending_migration is not None:
            # No schema asked for a state since the last advance, so there is nothing left to move on.
            self._states_by_applied_migrations = {}
            self._asked_applied_migrations = set()

        self._pending_migration = migration

    def move_asked_states_past_pending_migration(self):
        """
        Move each point asked for before the last advance past the migration of that advance.
        """
        if self._pending_migration is None:
            return

        key = (self._pending_migration.app_label, self._pending_migration.name)
        states = {}
        for applied in self._asked_applied_migrations:
            state = self._states_by_applied_migrations[applied].clone()
            self._pending_migration.mutate_state(state, preserve=False)
            states[applied | {key}] = state

        self._states_by_applied_migrations = states
        self._asked_applied_migrations = set()
        self._pending_migration = None


class SharedStatesMigrationExecutor(SharedFilesMigrationExecutor):
    """
    A MigrationExecutor that applies migrations from precomputed project states, shared between the schemas of a run.
    """

    def __init__(self, connection, progress_callback=None, loaded_migration_files=None):
        super().__init__(connection, progress_callback, loaded_migration_files=loaded_migration_files)
        self.squashed_migrations_checked = False

    def apply_from_shared_states(self, migration, shared_operation_states, fake=False, fake_initial=False):
        """
        Apply the migration to this schema, using the shared states, the same way migrate applies a plan of one
        migration.

        Like migrate, this creates the migrations table if needed, applies the migration and records any squashed
        migrations that are now complete. We can't call migrate itself with the shared states: it would change the
        shared starting state in place and clone it for every schema, and it plans the whole run again for every
        migration.

        This only works forwards. Unapplying needs states built for each schema, so the command does not share states
        when a run unapplies migrations.
        """
        self.recorder.ensure_schema()
        self.apply_migration_between_states(
            shared_operation_states.get_states_around_operations(self, migration),
            migration,
            fake=fake,
            fake_initial=fake_initial,
        )
        self.check_replacements()

    def record_migration(self, app_label, name, forward=True):
        """
        Record the migration, and update the loader's applied migrations to match.

        The executor is kept for the whole run and its loader is not reloaded, so the applied migrations are updated
        here. The migration from disk is stored as the value, like Django's loader does for squashed migrations. Django
        also calls this for each migration a squash replaces, and when unapplying.
        """
        super().record_migration(app_label, name, forward)
        key = (app_label, name)
        if forward:
            self.loader.applied_migrations[key] = self.loader.disk_migrations.get(key)
        else:
            self.loader.applied_migrations.pop(key, None)

        if any(key in migration.replaces for migration in self.loader.replacements.values()):
            self.squashed_migrations_checked = False

    def check_replacements(self):
        """
        Record the squashed migrations whose replaced migrations are all applied, and update the loader's applied
        migrations to match.

        The command calls this after every migration on every schema, so the database is only queried when something can
        have changed: the first time (a squash can be complete in the database without being recorded), and after
        recording a migration that a squash replaces. Without squashed migrations it is never queried.
        """
        if not self.loader.replacements or self.squashed_migrations_checked:
            return

        super().check_replacements()
        self.squashed_migrations_checked = True

        applied = self.loader.applied_migrations
        for key, migration in self.loader.replacements.items():
            if key not in applied and self.loader.all_replaced_applied(key, applied):
                applied[key] = migration

    def apply_migration_between_states(self, states, migration, fake=False, fake_initial=False):
        """
        Run MigrationExecutor.apply_migration between the precomputed states of this migration. Return the last of
        them, like Django's apply_migration returns the state after the migration.

        Django's apply_migration gets a copy of the migration whose apply and mutate_state use those states. That way
        the progress callbacks, --fake, --fake-initial and recording the migration all stay Django's own. The copy keeps
        the original operations, because the --fake-initial check looks at their types.

        This relies on which methods Django's apply_migration and Migration.apply call. DjangoCouplingTestCase guards
        this.
        """

        def apply(project_state, schema_editor, collect_sql=False):
            self.apply_operations_between_states(states, migration, schema_editor)
            return states[-1]

        def mutate_state(project_state, preserve=True):
            return states[-1]

        with_states = copy.copy(migration)
        with_states.apply = apply
        with_states.mutate_state = mutate_state
        return self.apply_migration(states[0], with_states, fake=fake, fake_initial=fake_initial)

    @staticmethod
    def apply_operations_between_states(states, migration, schema_editor):
        """
        Run Migration.apply with each operation wrapped with its precomputed states. Django still runs the SQL, and
        still decides which operations get a transaction of their own.

        The wrapped operations ignore the states Migration.apply computes, so it is given an empty state to start from.
        """
        bound = copy.copy(migration)
        bound.operations = bind_operations_to_states(migration.operations, states, migration)
        bound.apply(ProjectState(), schema_editor)


class SharedOperationStates:
    """
    The project states a migrate run goes through, shared by all schemas that reach them.

    The state of a schema before a migration depends only on the migrations it has applied. So the states are keyed by
    those migrations, and schemas at the same point share one computation.

    Only the states of the migration being applied are kept, plus the state each of them ends in (where the next
    migration starts). Memory use therefore grows with the number of different points the schemas are at, not with the
    number of schemas or the length of the plan.

    When no state is kept for a schema's point, it is built from scratch, as Django does for every migration.
    """

    def __init__(self):
        # Applied migration keys -> the state before the migration about to be applied.
        self._starting_states_by_applied_migrations = {}
        # (applied migration keys, migration key) -> the states around each operation of that migration.
        self._operation_states_by_point = {}

    def get_states_around_operations(self, executor, migration):
        """
        Return the states around each operation of the migration for the executor's schema. They are computed once for
        all schemas at the same point. The executor's progress callback is told about the render.
        """
        applied = applied_migrations_in_graph(executor.loader)
        key = (applied, (migration.app_label, migration.name))

        if key not in self._operation_states_by_point:
            if applied not in self._starting_states_by_applied_migrations:
                self._starting_states_by_applied_migrations[applied] = executor._create_project_state(
                    with_applied_migrations=True
                )

            self._operation_states_by_point[key] = compute_states_around_migration_operations(
                migration, self._starting_states_by_applied_migrations[applied], executor.progress_callback
            )

        return self._operation_states_by_point[key]

    def advance_to_next_migration(self):
        """
        Move on to the next migration in the plan. Schemas that applied the current migration start the next one from
        the state it ended in.
        """
        self._starting_states_by_applied_migrations = {
            applied | {key}: states[-1] for (applied, key), states in self._operation_states_by_point.items()
        }
        self._operation_states_by_point = {}
