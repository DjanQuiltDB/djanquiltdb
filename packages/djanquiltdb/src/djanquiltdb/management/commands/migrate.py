import sys
from importlib import import_module

from django.apps import apps
from django.core.management.base import CommandError
from django.core.management.commands.migrate import Command as MigrateCommand
from django.core.management.sql import emit_post_migrate_signal, emit_pre_migrate_signal
from django.db import connections
from django.db.migrations.loader import AmbiguityError
from django.db.migrations.state import ProjectState
from django.utils.module_loading import module_has_submodule

from djanquiltdb.db import connection
from djanquiltdb.management.base import get_databases_and_schema_from_options, get_shards_by_node
from djanquiltdb.management.executor import (
    SharedFilesMigrationExecutor,
    SharedOperationStates,
    SharedStartingStates,
    SharedStatesMigrationExecutor,
    shared_migration_states_enabled,
)
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import get_all_databases, get_template_name, schema_exists, use_shard


class Command(MigrateCommand):
    """
    This command overrides the normal migration command, so migrating a sharded project is done by running migrate as
    usual. The main handle function is entirely replaced, but most of it is similar.

    Major differences:
        - A plan is made for each shard and the longest is executed.
        - It is executed per node. Each node is done for every shard and template before moving to the next node.
        - Same for fake and reverse operations.
        - The migrations are loaded from disk once per run.
        - Schemas that have applied the same migrations share the state a migration starts from. It is built once, and
          each schema gets its own copy.
        - When building a new test database with SHARED_TEST_MIGRATION_STATES on, the project states of each migration
          are computed once and shared between the public and template schemas (see enable_shared_migration_states).
    """

    help = 'Updates database schema. Manages both apps with migrations and those without.'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # The state of a single run. handle resets the executors and the migrations loaded from disk, and
        # perform_migration resets the states.
        self.use_shared_states = False
        self.shared_operation_states = SharedOperationStates()
        self.executors_by_schema = {}
        self.shared_starting_states = SharedStartingStates()
        self.loaded_migration_files = {}

    def add_arguments(self, parser):
        # Add additional arguments on top of what the
        # native Migration command already accepted.
        super().add_arguments(parser)

        # Since we can now target multiple databased
        # change the default to 'all'
        # and the options to databases allowed.
        parser._option_string_actions['--database'].default = 'all'
        parser._option_string_actions[
            '--database'
        ].help = 'Nominates a database to synchronize. Defaults to all databases.'
        parser._option_string_actions['--database'].choices = ['all'] + get_all_databases()

        parser.add_argument(
            '--schema-name',
            '-s',
            action='store',
            dest='schema_name',
            help='Nominates a schema to synchronize. When empty all schemas will be migrated.',
        )
        parser.add_argument(
            '--check-shard',
            action='store_false',
            dest='check_shard',
            help='A flag used internally by the sharding library.',
        )

    def handle(self, *args, **options):
        self.verbosity = options.get('verbosity', 0)
        self.interactive = options.get('interactive')
        self.show_traceback = options.get('traceback')
        self.load_initial_data = options.get('load_initial_data')

        # Import the 'management' module within each installed app,
        # to register dispatcher events.
        for app_config in filter(lambda x: module_has_submodule(x.module, 'management'), apps.get_app_configs()):
            import_module('.management', app_config.name)

        databases, schema_name = get_databases_and_schema_from_options(options)

        if options.get('list', False):
            self.stderr.write(
                "The 'migrate --list' command is not supported in djanquiltdb. Use 'showmigrations' instead."
            )

        for connection_ in connections:
            connections[connection_].prepare_database()

        # Every run starts with fresh executors and loads the migrations from disk again. Project states are only shared
        # when building a new test database, where there are no shards and every schema starts from scratch.
        self.executors_by_schema = {}
        self.loaded_migration_files = {}
        self.use_shared_states = (
            shared_migration_states_enabled()
            and not schema_name
            and not any(shard for shard, *location in self.iter_schemas_to_migrate(databases))
        )
        with use_shard(node_name=databases[0], schema_name=schema_name or PUBLIC_SCHEMA_NAME) as env:
            executor = self.get_executor_for_schema(env)

        # Before anything else, drop out hard if there are conflicting apps.
        self.check_for_app_conflicts(executor)

        # If they supplied command line arguments, work out what they mean.
        run_syncdb, targets = self.get_targets_from_options(executor, options)
        run_syncdb = options.get('run_syncdb')
        run_syncdb = run_syncdb and executor.loader.unmigrated_apps

        # Work out from which node we need to migrate.
        plan = self.get_plan(targets, databases, schema_name)

        # Shared states only work forwards. Unapplying is left to Django's own migrate.
        self.use_shared_states = self.use_shared_states and not any(backwards for migration, backwards in plan)

        # Run the syncdb phase. Note that we need this for apps that don't have migrations.
        if run_syncdb:
            self.verbosity >= 1 and self.stdout.write(
                self.style.MIGRATE_HEADING('Synchronizing apps without migrations:')
            )
            emit_pre_migrate_signal(self.verbosity, self.interactive, connection.alias, plan=plan)
            self._sync_apps(databases, schema_name, executor.loader.unmigrated_apps)
        else:
            emit_pre_migrate_signal(self.verbosity, self.interactive, connection.alias, plan=plan)

        # Execute the plan
        self.verbosity >= 1 and self.stdout.write(self.style.MIGRATE_HEADING('Running migrations:'))

        error = False
        if not plan:
            executor.check_replacements()
            self.verbosity >= 1 and self.check_for_changes(executor)
        else:
            error = self.perform_migration(
                plan, databases, schema_name, fake=options.get('fake'), fake_initial=options.get('fake_initial')
            )

        emit_post_migrate_signal(self.verbosity, self.interactive, connection.alias)

        if error:
            sys.exit(1)

    def get_targets_from_options(self, executor, options):
        if options.get('app_label') and options.get('migration_name'):
            app_label, migration_name = options['app_label'], options['migration_name']
            if app_label not in executor.loader.migrated_apps:
                raise CommandError(
                    "App '{}' does not have migrations (you cannot selectively sync unmigrated apps)".format(app_label)
                )
            if migration_name == 'zero':
                return False, [(app_label, None)]

            try:
                migration = executor.loader.get_migration_by_prefix(app_label, migration_name)
            except AmbiguityError:
                raise CommandError(
                    "More than one migration matches '{}' in app '{}'. Please be more specific.".format(
                        migration_name, app_label
                    )
                )
            except KeyError:
                raise CommandError(
                    "Cannot find a migration matching '{}' from app '{}'.".format(migration_name, app_label)
                )
            return False, [(app_label, migration.name)]

        if options.get('app_label'):
            app_label = options['app_label']
            if app_label not in executor.loader.migrated_apps:
                raise CommandError(
                    "App '{}' does not have migrations (you cannot selectively sync unmigrated apps)".format(app_label)
                )
            return False, [key for key in executor.loader.graph.leaf_nodes() if key[0] == app_label]

        # Nothing is given, just return all end nodes
        return True, executor.loader.graph.leaf_nodes()

    def check_for_app_conflicts(self, executor):
        """
        Check app to check if there are conflicts. Raise an error if there are.
        """
        conflicts = executor.loader.detect_conflicts()
        if conflicts:
            name_str = '; '.join('{} in {}'.format(', '.join(names), app) for app, names in conflicts.items())
            raise CommandError(
                'Conflicting migrations detected ({}).\nTo fix them run '
                "'python manage.py makemigrations --merge'".format(name_str)
            )

    def get_plan(self, targets, databases, schema_name):
        plan = []

        # If the schema_name is set, get the plan for all schemas on all databases and return the longest
        if schema_name:
            for database in databases:
                schema_plan = self.get_plan_for_shard(targets, database, schema_name)

                plan = schema_plan if len(schema_plan) > len(plan) else plan

            return plan

        # If no schema_name is set, then take the longest plan of all schemas.
        for shard, *location in self.iter_schemas_to_migrate(databases):
            schema_plan = self.get_plan_for_shard(targets, *location)
            if len(schema_plan) > len(plan):
                plan = schema_plan

        return plan

    def iter_schemas_to_migrate(self, databases):
        """
        Yield (shard, node name, schema name) for every schema to migrate on the given databases. The public and
        template schemas come first, with None as their shard. Then come the shards, if the shard table exists.
        """
        template_name = get_template_name()
        for database in databases:
            yield None, database, PUBLIC_SCHEMA_NAME

            if schema_exists(database, template_name):
                yield None, database, template_name

        shards_by_node = get_shards_by_node(databases)
        for database in databases:
            for shard in shards_by_node.get(database, []):
                yield shard, shard.node_name, shard.schema_name

    def get_plan_for_shard(self, targets, database, schema_name):
        with use_shard(node_name=database, schema_name=schema_name) as env:
            return self.get_executor_for_schema(env).migration_plan(targets)

    def check_for_changes(self, executor):
        self.stdout.write('  No migrations to apply.')
        # If there's changes that aren't in migrations yet, tell them how to fix it.
        autodetector = self.autodetector(
            executor.loader.project_state(),
            ProjectState.from_apps(apps),
        )
        changes = autodetector.changes(graph=executor.loader.graph)
        if changes:
            self.stdout.write(
                self.style.NOTICE(
                    "  Your models have changes that are not yet reflected in a migration, and so won't be applied."
                )
            )
            self.stdout.write(
                self.style.NOTICE(
                    "  Run 'manage.py makemigrations' to make new "
                    "migrations, and then re-run 'manage.py migrate' to "
                    'apply them.'
                )
            )

    def perform_migration(self, plan, databases, schema_name, fake, fake_initial):
        self.shared_operation_states = SharedOperationStates()
        self.shared_starting_states = SharedStartingStates()

        if schema_name:  # If we have a targeted shard, just migrate that shard
            for database in databases:
                with use_shard(node_name=database, schema_name=schema_name) as env:
                    self.get_executor_for_schema(env).migrate(
                        targets=None, plan=plan, fake=fake, fake_initial=fake_initial
                    )
            return False  # Report no errors

        # We have multiple shards to migrate. Do this breadth-first
        stop = False

        for node in plan:
            for shard, *location in self.iter_schemas_to_migrate(databases):
                if shard is None:
                    stop |= self.check_or_migrate_schema(*location, node, fake, fake_initial)
                else:
                    stop |= self.check_or_migrate_shard(shard, node, fake, fake_initial)

            self.shared_operation_states.advance_to_next_migration()
            self.shared_starting_states.advance_past_migration(node[0])

            # If one or more migrations failed, don't move to the next.
            if stop:
                self.stdout.write(
                    self.style.ERROR('Migration stopped due to errors after completing {}.'.format(node[0]))
                )
                break
        return stop

    def get_executor_for_schema(self, env):
        """
        Return a migration executor for the schema of env.

        When sharing states, each schema keeps one executor for the whole run. When not sharing states, every call
        returns a new executor, so that each check reads the schema's applied migrations from the database at that
        moment.
        """
        if not self.use_shared_states:
            return SharedFilesMigrationExecutor(
                env.connection,
                self.migration_progress_callback,
                loaded_migration_files=self.loaded_migration_files,
                starting_states=self.shared_starting_states,
            )

        key = (env.options.node_name, env.options.schema_name)
        if key not in self.executors_by_schema:
            self.executors_by_schema[key] = SharedStatesMigrationExecutor(
                env.connection, self.migration_progress_callback, loaded_migration_files=self.loaded_migration_files
            )

        return self.executors_by_schema[key]

    def check_or_migrate_schema(self, database, schema_name, plan_node, fake, fake_initial):
        with use_shard(node_name=database, schema_name=schema_name) as env:
            return self.migrate_schema_if_needed(
                env, '{}|{}'.format(database, schema_name), plan_node, fake, fake_initial
            )

    def check_or_migrate_shard(self, shard, plan_node, fake, fake_initial):
        with use_shard(shard, active_only_schemas=False) as env:
            return self.migrate_schema_if_needed(
                env, '{}|{}'.format(shard.node_name, shard.alias), plan_node, fake, fake_initial
            )

    def migrate_schema_if_needed(self, env, label, plan_node, fake, fake_initial):
        """
        Apply or unapply the migration in plan_node on the schema of env, if that is still needed. label is the schema's
        name in the output. Return True when the migration failed.
        """
        executor = self.get_executor_for_schema(env)
        migration, backwards = plan_node

        # if the node is applied and we're going backwards,
        # or the node is not applied yet and we're going forwards.
        if ((migration.app_label, migration.name) not in executor.loader.applied_migrations) == backwards:
            if self.verbosity >= 2:
                if backwards:
                    self.stdout.write('    {} does not have {} applied yet.\n'.format(label, migration))
                else:
                    self.stdout.write('    {} has {} already applied.\n'.format(label, migration))
            return False

        if self.verbosity >= 2:
            self.stdout.write('    {} {} to {}\n'.format('Unapplying' if backwards else 'Applying', migration, label))
        try:
            if self.use_shared_states and not backwards:
                executor.apply_from_shared_states(migration, self.shared_operation_states, fake, fake_initial)
            else:
                executor.migrate(targets=None, plan=[plan_node], fake=fake, fake_initial=fake_initial)
        except Exception as exception:  # When an error occurs, continue this migration for other shards.
            self.stderr.write('    {}: {} - {}: {}'.format(label, migration, type(exception).__name__, exception))
            return True  # report failure
        return False  # report migration went without troubles

    def migration_progress_callback(self, action, migration=None, fake=False):
        """Appends the current shard details to the migration output"""

        if self.verbosity >= 1:
            if action in ('apply_start', 'unapply_start', 'render_start'):
                self.stdout.write('[{}] '.format(connection.alias), ending='')

        return super().migration_progress_callback(action, migration=migration, fake=fake)

    def _sync_apps(self, databases, schema_name, app_labels):
        """
        Helper method that calls sync apps for all shards available. Or for a specific shard, if schema_name is set.
        """
        created_models = set()

        if schema_name:
            for database in databases:
                with use_shard(node_name=database, schema_name=schema_name) as env:
                    created_models.update(self.sync_apps(env.connection, app_labels) or {})
        else:
            for shard, database, location_schema_name in self.iter_schemas_to_migrate(databases):
                context = use_shard(shard) if shard else use_shard(node_name=database, schema_name=location_schema_name)
                with context as env:
                    created_models.update(self.sync_apps(env.connection, app_labels) or {})

        return created_models

    def get_check_kwargs(self, options):
        check_kwargs = super().get_check_kwargs(options)

        # The base MigrateCommand thinks that migrations can only occur on 1 database at a time, so it takes the
        # --database argument and wraps it in a single-item list for databases (plural) to check. We can of course do
        # multiple databases, and need to convert our "all" alias to actual database aliases.
        if check_kwargs['databases'] == ['all']:
            check_kwargs['databases'] = get_all_databases()

        return check_kwargs
