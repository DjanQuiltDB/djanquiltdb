from django.apps import apps
from django.conf import settings
from django.core.management.base import CommandError
from django.core.management.commands.migrate import Command as MigrateCommand
from django.core.management.sql import emit_post_migrate_signal, emit_pre_migrate_signal
from django.db import connections, connection
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.loader import AmbiguityError
from django.db.migrations.state import ProjectState
from django.utils.module_loading import module_has_submodule
from importlib import import_module

from sharding.utils import get_shard_class, use_shard, get_template_name


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
        super(Command, self).add_arguments(parser)
        # Since we can now target multiple databased
        # change the default to 'all'
        # and the options to databases allowed.
        parser._option_string_actions['--database'].default = 'all'
        parser._option_string_actions['--database'].help = \
            'Nominates a database to synchronize. Defaults to all databases.'
        parser._option_string_actions['--database'].choices = ['all'] + self.get_all_databases()

        parser.add_argument('--shard', '-s', action='store', dest='shard',
                            help='Nominates a single shard or schema to synchronize.'
                                 'When empty all shards will be migrated.'
                                 'Format a shard like: <database>|<shard alias, \'public\' of template name>. '
                                 'e.g., `default|public`, `some_node|template` or `other_node|shard1`.')

    def get_all_databases(self):
        return [name for name, db in settings.DATABASES.items()]

    def handle(self, *args, **options):
        self.verbosity = options.get('verbosity', 0)
        self.interactive = options.get('interactive')
        self.show_traceback = options.get('traceback')
        self.load_initial_data = options.get('load_initial_data')

        # Import the 'management' module within each installed app,
        # to register dispatcher events.
        for app_config in apps.get_app_configs():
            if module_has_submodule(app_config.module, 'management'):
                import_module('.management', app_config.name)

        databases, schema_name = self.get_database_and_schema_from_options(options)

        if options.get('list', False):
            self.stderr.write(
                "The 'migrate --list' command is not supported in django_sharding. Use 'showmigrations' instead.")

        for conn in connections:
            connections[conn].prepare_database()

        executor = MigrationExecutor(connection)

        # Before anything else, drop out hard if there are conflicting apps.
        conflicts = executor.loader.detect_conflicts()
        if conflicts:
            name_str = "; ".join(
                "%s in %s" % (", ".join(names), app)
                for app, names in conflicts.items()
            )
            raise CommandError(
                "Conflicting migrations detected (%s).\nTo fix them run "
                "'python manage.py makemigrations --merge'" % name_str
            )

        # If they supplied command line arguments, work out what they mean.
        targets = self.get_targets_from_options(executor, options)

        # Work out from which node we need to migrate
        plan = self.get_plan(targets, databases)

        # Execute the plan
        emit_pre_migrate_signal([], self.verbosity, self.interactive, connection.alias)

        if self.verbosity >= 1:
            self.stdout.write(self.style.MIGRATE_HEADING('Running migrations:'))

        if not plan:
            executor.check_replacements()
            if self.verbosity >= 1:
                self.check_for_changes(executor)
        else:
            self.perform_migration(plan, databases, schema_name,
                                   fake=options.get('fake'), fake_initial=options.get('fake_initial'))

        emit_post_migrate_signal([], self.verbosity, self.interactive, connection.alias)

    def get_database_and_schema_from_options(self, options):
        database_options = options.get('database')
        target_shard = options.get('shard')

        # Get the database we're operating from
        if not database_options or database_options == 'all':
            databases = self.get_all_databases()
        elif database_options not in self.get_all_databases():
            raise CommandError('You must migrate an existing non-primary DB.')
        else:
            databases = [database_options]

        # Process shard option
        schema_name = None
        if target_shard:
            database, shard_name = target_shard.split('|')
            if database in databases:
                databases = [database]
            else:
                raise CommandError('You must migrate an existing non-primary DB.')

            if shard_name in ['public', get_template_name()]:
                schema_name = shard_name
            else:
                if get_shard_class().objects.filter(alias=shard_name).exists():
                    shard = get_shard_class().objects.get(alias=shard_name)
                    schema_name = shard.schema_name
                    if not shard.node_name == database:
                        raise CommandError('Shard {} does not belong to database {}.'.format(shard.alias, database))
                else:
                    raise CommandError('Shard {} is not known.'.format(shard_name))
        return databases, schema_name

    def get_targets_from_options(self, executor, options):
        if options.get('app_label') and options.get('migration_name'):
            app_label, migration_name = options['app_label'], options['migration_name']
            if app_label not in executor.loader.migrated_apps:
                raise CommandError(
                    "App '%s' does not have migrations (you cannot selectively "
                    "sync unmigrated apps)" % app_label
                )
            if migration_name == 'zero':
                return [(app_label, None)]

            try:
                migration = executor.loader.get_migration_by_prefix(app_label, migration_name)
            except AmbiguityError:
                raise CommandError(
                    "More than one migration matches '%s' in app '%s'. "
                    "Please be more specific." %
                    (migration_name, app_label)
                )
            except KeyError:
                raise CommandError("Cannot find a migration matching '%s' from app '%s'." % (
                    migration_name, app_label))
            return [(app_label, migration.name)]

        if options.get('app_label'):
            app_label = options['app_label']
            if app_label not in executor.loader.migrated_apps:
                raise CommandError(
                    "App '%s' does not have migrations (you cannot selectively "
                    "sync unmigrated apps)" % app_label
                )
            return [key for key in executor.loader.graph.leaf_nodes() if key[0] == app_label]

        # nothing is given. just return all end nodes
        return executor.loader.graph.leaf_nodes()

    def get_plan(self, targets, databases):
        plan = []
        template_name = get_template_name()
        for shard in get_shard_class().objects.filter(node_name__in=databases):
            shard_plan = self.get_plan_for_shard(shard.node_name, shard.schema_name, targets)
            if len(shard_plan) > len(plan):
                plan = shard_plan
        for db in databases:  # do templates and publics
            public_plan = self.get_plan_for_shard(db, 'public', targets)
            if len(public_plan) > len(plan):
                plan = public_plan

            template_plan = self.get_plan_for_shard(db, template_name, targets)
            if len(template_plan) > len(plan):
                plan = template_plan
        return plan

    def get_plan_for_shard(self, node_name, schema_name, targets):
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
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
        template_name = get_template_name()
        if schema_name:  # if we have a targeted shard, just migrate that shard
            # If we get a schema_name we assume databases has only one entry
            with use_shard(node_name=databases[0], schema_name=schema_name) as env:
                shard_executor = MigrationExecutor(env.connection)
                shard_executor.migrate(targets=None, plan=plan, fake=fake, fake_initial=fake_initial)
        else:  # we have multiple shards to migrate. Do the breath-first
            stop = False
            for node in plan:
                # migrate all public schemas and templates
                for db in databases:
                    stop |= self.check_or_migrate_schema(db, 'public', node, fake, fake_initial)
                    stop |= self.check_or_migrate_schema(db, template_name, node, fake, fake_initial)

                # migrate all shards
                for shard in get_shard_class().objects.filter(node_name__in=databases):
                    stop |= self.check_or_migrate_shard(shard, node, fake, fake_initial)

                # if one or more migrations failed, don't move to the next.
                if stop:
                    self.stdout.write(self.style.ERROR(
                        'Migration stopped due to errors after completing {}.'.format(node[0])
                    ))
                    break

    def check_or_migrate_schema(self, node_name, schema_name, plan_node, fake, fake_initial):
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            executor = MigrationExecutor(env.connection, self.migration_progress_callback)
            migration, backwards = plan_node

            # if the node is applied and we're going backwards,
            # or the node is not applied yet and we're going forwards.
            if ((migration.app_label, migration.name) not in executor.loader.applied_migrations) == backwards:
                if self.verbosity >= 2:
                    if backwards:
                        self.stdout.write(
                            '    {}|{} does not have {} applied yet.\n'.format(node_name, schema_name, migration))
                    else:
                        self.stdout.write(
                            '    {}|{} has {} already applied.\n'.format(node_name, schema_name, migration))

            else:
                if self.verbosity >= 2:
                    self.stdout.write(
                        '    {} {} to default|public\n'.format('Unapplying' if backwards else 'Applying', migration)
                    )
                try:
                    executor.migrate(targets=None, plan=[plan_node], fake=fake, fake_initial=fake_initial)
                except Exception as exception:  # When an error occurs, continue this migration for other shards.
                    self.stderr.write(
                        '    {}|{}: {} - {}: {}'.format(node_name, schema_name, migration, type(exception).__name__,
                                                        exception)
                    )
                    return True  # rapport failure
        return False  # note migration went without troubles

    def check_or_migrate_shard(self, shard, plan_node, fake, fake_initial):
        with use_shard(shard, active_only_schemas=False) as env:
            shard_executor = MigrationExecutor(env.connection)
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
                    return True  # rapport failure
        return False  # note migration went without troubles
