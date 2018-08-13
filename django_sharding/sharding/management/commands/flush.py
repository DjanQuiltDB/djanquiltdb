from importlib import import_module

from django.apps import apps
from django.core.management import call_command
from django.core.management.base import CommandError
from django.core.management.commands.flush import Command as FlushCommand
from django.core.management.color import no_style
from django.core.management.sql import sql_flush
from django.db import transaction

from sharding.management.base import get_databases_and_schema_from_options, shard_table_exists
from sharding.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from sharding.utils import use_shard, get_all_databases, get_template_name, get_shard_class


class Command(FlushCommand):
    def add_arguments(self, parser):
        super().add_arguments(parser)

        # Since we can now target multiple databases change the default to 'all' and the options to databases allowed.
        parser._option_string_actions['--database'].default = 'all'
        parser._option_string_actions['--database'].help = \
            'Nominates a database to synchronize. Defaults to all databases.'
        parser._option_string_actions['--database'].choices = ['all'] + get_all_databases()

        parser.add_argument(
            '--schema-name', '-s', action='store', dest='schema_name',
            help='Nominates a schema to flush. When empty all schemas will be flushed.'
        )
        parser.add_argument(
            '--check-shard', action='store_true', dest='check_shard', default=True,
            help='If set, checks whether the shard exists in the shard table.'
        )

    def handle(self, **options):
        interactive = options['interactive']

        if interactive:
            confirm = input("You have requested a flush of the database.\n"
                            "This will IRREVERSIBLY DESTROY all data currently in the selected database,\n"
                            "and return each table to an empty state.\n"
                            "Are you sure you want to do this?\n"
                            "    Type 'yes' to continue, or 'no' to cancel: ")

            if confirm != 'yes':
                self.stdout.write('Flush cancelled.\n')
                return

        node_names, schema_name = get_databases_and_schema_from_options(options)

        for node_name in node_names:
            if schema_name:
                with use_shard(node_name=node_name, schema_name=schema_name) as env:
                    self.handle_schema(connection=env.connection, **options)
            else:
                # Pick the public schema of the current node as default connection, so we do checks like for example if
                # the template schema exists and if the shard table exists on the correct node.
                with use_shard(node_name=node_name, schema_name=PUBLIC_SCHEMA_NAME) as public_env:
                    # Flush all schemas on this database, starting with the template schema, if it exists.
                    if public_env.connection.get_ps_schema(get_template_name()):
                        with use_shard(node_name=node_name, schema_name=get_template_name(),
                                       include_public=False) as env:
                            self.handle_schema(connection=env.connection, **options)

                    # And now all other shards, but only if the shard table exists on the public schema. If not, we can
                    # assume that not other shards exists.
                    if shard_table_exists(node_name):
                        for shard in get_shard_class().objects.filter(node_name__in=node_names):
                            with use_shard(shard, include_public=False) as env:
                                self.handle_schema(connection=env.connection, **options)

                    # And finally do the public schema. We do this as last, to make sure we don't face constraints for
                    # flushing.
                    public_options = options.copy()

                    # We can set allow_cascade to True, since there shouldn't be any other entries anymore in other
                    # shards or the template schema.
                    public_options['allow_cascade'] = True

                    self.handle_schema(connection=public_env.connection, **public_options)

    def handle_schema(self, **options):
        """
        This does the actual flushing. Taken from the original flush handle command from Django.
        """
        connection = options['connection']
        verbosity = options['verbosity']
        interactive = options['interactive']
        # The following are stealth options used by Django's internals.
        reset_sequences = options.get('reset_sequences', True)
        allow_cascade = options.get('allow_cascade', False)
        inhibit_post_migrate = options.get('inhibit_post_migrate', False)

        style = no_style()

        # Import the 'management' module within each installed app, to register
        # dispatcher events.
        for app_config in apps.get_app_configs():
            try:
                import_module('.management', app_config.name)
            except ImportError:
                pass

        sql_list = sql_flush(style, connection, only_django=True,
                             reset_sequences=reset_sequences,
                             allow_cascade=allow_cascade)

        try:
            with transaction.atomic(savepoint=connection.features.can_rollback_ddl):
                with connection.cursor() as cursor:
                    for sql in sql_list:
                        cursor.execute(sql)
        except Exception as exc:
            raise CommandError(
                "Database {} couldn't be flushed. Possible reasons:\n"
                "  * The database isn't running or isn't configured correctly.\n"
                "  * At least one of the expected database tables doesn't exist.\n"
                "  * The SQL was invalid.\n"
                "Hint: Look at the output of 'django-admin sqlflush'. "
                "That's the SQL this command wasn't able to run.\n".format(connection)
            ) from exc

        if not inhibit_post_migrate:
            self.emit_post_migrate(verbosity, interactive, connection.alias)

        # Reinstall the initial_data fixture.
        if options.get('load_initial_data'):
            # Reinstall the initial_data fixture for apps without migrations.
            from django.db.migrations.executor import MigrationExecutor
            executor = MigrationExecutor(connection)
            app_options = options.copy()

            app_options['database'] = connection.alias

            for app_label in executor.loader.unmigrated_apps:
                app_options['app_label'] = app_label
                call_command('loaddata', 'initial_data', **app_options)
