import sys
from importlib import import_module

from django.apps import apps
from django.core.management.base import CommandError
from django.core.management.commands.migrate import Command as MigrateCommand
from django.core.management.sql import emit_post_migrate_signal, emit_pre_migrate_signal
from django.db import connections
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.loader import AmbiguityError
from django.db.migrations.state import ProjectState
from django.utils.module_loading import module_has_submodule

from sharding.db import connection
from sharding.management.base import get_databases_and_schema_from_options, shard_table_exists
from sharding.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from sharding.utils import get_shard_class, use_shard, get_template_name, get_all_databases, schema_exists


class Command(MigrateCommand):
    """
    migrate_shards extends the normal migration command.
    The main handle function is entirely replaced,
    but most of it is similar.

    Major differences:
        A plan is made for each shard and the longest is executed.
        It is executed per node.
            And each node is done for every shard and template
            before moving to the next node.
        Same for fake and reverse operations.
    """
    help = 'Updates database schema. Manages both apps with migrations and those without.'

    def add_arguments(self, parser):
        # Add additional arguments on top of what the
        # native Migration command already accepted.
        super().add_arguments(parser)

        # Since we can now target multiple databased
        # change the default to 'all'
        # and the options to databases allowed.
        parser._option_string_actions['--database'].default = 'all'
        parser._option_string_actions['--database'].help = \
            'Nominates a database to synchronize. Defaults to all databases.'
        parser._option_string_actions['--database'].choices = ['all'] + get_all_databases()

        parser.add_argument('--schema-name', '-s', action='store', dest='schema_name',
                            help='Nominates a schema to synchronize. When empty all schemas will be migrated.')

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
                "The 'migrate --list' command is not supported in django_sharding. Use 'showmigrations' instead.")

        for connection_ in connections:
            connections[connection_].prepare_database()

        executor = MigrationExecutor(connection)

        # Before anything else, drop out hard if there are conflicting apps.
        self.check_for_app_conflicts(executor)

        # If they supplied command line arguments, work out what they mean.
        run_syncdb, targets = self.get_targets_from_options(executor, options)
        run_syncdb = options.get('run_syncdb')
        run_syncdb = run_syncdb and executor.loader.unmigrated_apps

        # Work out from which node we need to migrate
        plan = self.get_plan(targets, databases, schema_name)

        # Run the syncdb phase. Note that we need this for apps that don't have migrations.
        if run_syncdb:
            self.verbosity >= 1 and self.stdout.write(self.style.MIGRATE_HEADING('Synchronizing apps without '
                                                                                 'migrations:'))
            emit_pre_migrate_signal(self.verbosity, self.interactive, connection.alias)
            self._sync_apps(databases, schema_name, executor.loader.unmigrated_apps)
        else:
            emit_pre_migrate_signal(self.verbosity, self.interactive, connection.alias)

        # Execute the plan
        self.verbosity >= 1 and self.stdout.write(self.style.MIGRATE_HEADING('Running migrations:'))

        error = False
        if not plan:
            executor.check_replacements()
            self.verbosity >= 1 and self.check_for_changes(executor)
        else:
            error = self.perform_migration(plan, databases, schema_name,
                                           fake=options.get('fake'), fake_initial=options.get('fake_initial'))

        emit_post_migrate_signal(self.verbosity, self.interactive, connection.alias)

        if error:
            sys.exit(1)

    def get_targets_from_options(self, executor, options):
        if options.get('app_label') and options.get('migration_name'):
            app_label, migration_name = options['app_label'], options['migration_name']
            if app_label not in executor.loader.migrated_apps:
                raise CommandError(
                    "App '{}' does not have migrations (you cannot selectively "
                    "sync unmigrated apps)".format(app_label)
                )
            if migration_name == 'zero':
                return False, [(app_label, None)]

            try:
                migration = executor.loader.get_migration_by_prefix(app_label, migration_name)
            except AmbiguityError:
                raise CommandError(
                    "More than one migration matches '{}' in app '{}'. "
                    "Please be more specific.".format(migration_name, app_label)
                )
            except KeyError:
                raise CommandError("Cannot find a migration matching '{}' from app '{}'.".format(migration_name,
                                                                                                 app_label))
            return False, [(app_label, migration.name)]

        if options.get('app_label'):
            app_label = options['app_label']
            if app_label not in executor.loader.migrated_apps:
                raise CommandError(
                    "App '{}' does not have migrations (you cannot selectively "
                    "sync unmigrated apps)".format(app_label)
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
            name_str = '; '.join(
                '{} in {}'.format(', '.join(names), app)
                for app, names in conflicts.items()
            )
            raise CommandError(
                "Conflicting migrations detected ({}).\nTo fix them run "
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

        # If no schema_name is set, then collect the longest plan based on the public schema, template schema and all
        # shards on all databases. Note that we only get the plan from the shards if the shard table exists. If not,
        # then we can safely assume no shards exist yet, because they're not in the database.
        if shard_table_exists():
            for shard in get_shard_class().objects.filter(node_name__in=databases):
                shard_plan = self.get_plan_for_shard(targets, shard.node_name, shard.schema_name)
                if len(shard_plan) > len(plan):
                    plan = shard_plan

        template_name = get_template_name()

        for database in databases:  # Do templates and publics
            public_plan = self.get_plan_for_shard(targets, database, PUBLIC_SCHEMA_NAME)
            if len(public_plan) > len(plan):
                plan = public_plan

            if schema_exists(database, template_name):
                template_plan = self.get_plan_for_shard(targets, database, template_name)
                if len(template_plan) > len(plan):
                    plan = template_plan

        return plan

    def get_plan_for_shard(self, targets, database, schema_name):
        with use_shard(node_name=database, schema_name=schema_name) as env:
            shard_executor = MigrationExecutor(env.connection, self.migration_progress_callback)
            return shard_executor.migration_plan(targets)

    def check_for_changes(self, executor):
        self.stdout.write('  No migrations to apply.')
        # If there's changes that aren't in migrations yet, tell them how to fix it.
        autodetector = MigrationAutodetector(
            executor.loader.project_state(),
            ProjectState.from_apps(apps),
        )
        changes = autodetector.changes(graph=executor.loader.graph)
        if changes:
            self.stdout.write(self.style.NOTICE(
                "  Your models have changes that are not yet reflected "
                "in a migration, and so won't be applied."
            ))
            self.stdout.write(self.style.NOTICE(
                "  Run 'manage.py makemigrations' to make new "
                "migrations, and then re-run 'manage.py migrate' to "
                "apply them."
            ))

    def perform_migration(self, plan, databases, schema_name, fake, fake_initial):
        if schema_name:  # If we have a targeted shard, just migrate that shard
            for database in databases:
                with use_shard(node_name=database, schema_name=schema_name) as env:
                    shard_executor = MigrationExecutor(env.connection)
                    shard_executor.migrate(targets=None, plan=plan, fake=fake, fake_initial=fake_initial)
            return False  # Report no errors

        # We have multiple shards to migrate. Do this breadth-first
        template_name = get_template_name()
        stop = False

        for node in plan:
            # Migrate all public schemas and templates
            for database in databases:
                stop |= self.check_or_migrate_schema(database, PUBLIC_SCHEMA_NAME, node, fake, fake_initial)

                if schema_exists(database, template_name):
                    stop |= self.check_or_migrate_schema(database, template_name, node, fake, fake_initial)

            # Migrate all shards, if the shard table exists.
            if shard_table_exists():
                for shard in get_shard_class().objects.filter(node_name__in=databases):
                    stop |= self.check_or_migrate_shard(shard, node, fake, fake_initial)

            # If one or more migrations failed, don't move to the next.
            if stop:
                self.stdout.write(self.style.ERROR(
                    'Migration stopped due to errors after completing {}.'.format(node[0])
                ))
                break
        return stop

    def check_or_migrate_schema(self, database, schema_name, plan_node, fake, fake_initial):
        with use_shard(node_name=database, schema_name=schema_name) as env:
            executor = MigrationExecutor(env.connection, self.migration_progress_callback)
            migration, backwards = plan_node

            # if the node is applied and we're going backwards,
            # or the node is not applied yet and we're going forwards.
            if ((migration.app_label, migration.name) not in executor.loader.applied_migrations) == backwards:
                if self.verbosity >= 2:
                    if backwards:
                        self.stdout.write(
                            '    {}|{} does not have {} applied yet.\n'.format(database, schema_name, migration))
                    else:
                        self.stdout.write(
                            '    {}|{} has {} already applied.\n'.format(database, schema_name, migration))

            else:
                if self.verbosity >= 2:
                    self.stdout.write(
                        '    {} {} to default|public\n'.format('Unapplying' if backwards else 'Applying', migration)
                    )
                try:
                    executor.migrate(targets=None, plan=[plan_node], fake=fake, fake_initial=fake_initial)
                except Exception as exception:  # When an error occurs, continue this migration for other shards.
                    self.stderr.write(
                        '    {}|{}: {} - {}: {}'.format(database, schema_name, migration, type(exception).__name__,
                                                        exception)
                    )
                    return True  # report failure
        return False  # report migration went without troubles

    def check_or_migrate_shard(self, shard, plan_node, fake, fake_initial):
        with use_shard(shard, active_only_schemas=False) as env:
            shard_executor = MigrationExecutor(env.connection, self.migration_progress_callback)
            migration, backwards = plan_node

            # if the node is applied and we're going backwards,
            # or the node is not applied yet and we're going forwards.
            if ((migration.app_label, migration.name) not in shard_executor.loader.applied_migrations) == backwards:
                if self.verbosity >= 2:
                    if backwards:
                        self.stdout.write(
                            '    {}|{} does not have {} applied yet.\n'.format(shard.node_name, shard.alias, migration))
                    else:
                        self.stdout.write(
                            '    {}|{} has {} already applied.\n'.format(shard.node_name, shard.alias, migration))

            else:
                if self.verbosity >= 2:
                    self.stdout.write(
                        '    {} {} to {}|{}\n'.format('Unapplying' if backwards else 'Applying', migration,
                                                      shard.node_name, shard.alias)
                    )
                try:
                    shard_executor.migrate(targets=None, plan=[plan_node], fake=fake, fake_initial=fake_initial)
                except Exception as exception:  # When an error occurs, continue this migration for other shards.
                    self.stderr.write(
                        '    {}|{}: {} - {}: {}'.format(shard.node_name, shard.alias, migration,
                                                        type(exception).__name__, exception)
                    )
                    return True  # report failure
        return False  # report migration went without troubles

    def migration_progress_callback(self, action, migration=None, fake=False):
        """ Appends the current shard details to the migration output """

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
            for database in databases:
                # Public schema
                with use_shard(node_name=database, schema_name='public') as env:
                    created_models.update(self.sync_apps(env.connection, app_labels) or {})

                # Template schema, if it exists.
                template_name = get_template_name()

                if schema_exists(database, template_name):
                    with use_shard(node_name=database, schema_name=template_name) as env:
                        created_models.update(self.sync_apps(env.connection, app_labels) or {})

            # Sync all other shards, if the shards table exist.
            if shard_table_exists():
                for shard in get_shard_class().objects.filter(node_name__in=databases):
                    with use_shard(shard) as env:
                        created_models.update(self.sync_apps(env.connection, app_labels) or {})

        return created_models
