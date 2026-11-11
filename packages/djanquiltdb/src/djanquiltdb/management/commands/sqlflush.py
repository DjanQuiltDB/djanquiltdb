from textwrap import indent

from django.core.management import CommandError
from django.core.management.commands.sqlflush import Command as SQLFlushCommand
from django.core.management.sql import sql_flush

from djanquiltdb.management.base import get_databases_and_schema_from_options, get_shards_by_node
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import StateException, get_all_databases, get_template_name, use_shard


class Command(SQLFlushCommand):
    output_transaction = False  # We are going to output our information in a different way

    def add_arguments(self, parser):
        super().add_arguments(parser)

        # Since we can now target multiple databases change the default to 'all' and the options to databases allowed.
        parser._option_string_actions['--database'].default = 'all'
        parser._option_string_actions[
            '--database'
        ].help = 'Nominates a database to print the SQL for. Defaults to all databases.'
        parser._option_string_actions['--database'].choices = ['all'] + get_all_databases()

        parser.add_argument(
            '--schema-name',
            '-s',
            action='store',
            dest='schema_name',
            help='Nominates a schema to print the SQL for. When empty all schemas will be shown.',
        )

        parser.add_argument(
            '--ignore-maintenance',
            action='store_true',
            dest='ignore_maintenance',
            help='Also print the SQL for the shards that are in maintenance.',
        )

    def handle(self, **options):
        node_names, schema_name = get_databases_and_schema_from_options(options)

        shards_by_node = {} if schema_name else get_shards_by_node(node_names)

        # A shard in maintenance does not stop the other schemas, as in flush. It is named in an error after the last
        # schema.
        shards_in_maintenance = []

        for node_name in node_names:
            if schema_name:
                with use_shard(node_name=node_name, schema_name=schema_name) as env:
                    self.handle_schema(connection=env.connection)
            else:
                # Pick the public schema of the current node as default connection, so we do checks like for example if
                # the template schema exists and if the shard table exists on the correct node.
                with use_shard(node_name=node_name, schema_name=PUBLIC_SCHEMA_NAME) as public_env:
                    # Template schema
                    if public_env.connection.get_ps_schema(get_template_name()):
                        with use_shard(
                            node_name=node_name, schema_name=get_template_name(), include_public=False
                        ) as env:
                            self.handle_schema(connection=env.connection)

                    # All of this node's shards, as far as the registry knows them.
                    for shard in shards_by_node.get(node_name, []):
                        try:
                            with use_shard(
                                shard,
                                include_public=False,
                                active_only_schemas=not options.get('ignore_maintenance'),
                            ) as env:
                                self.handle_schema(connection=env.connection)
                        except StateException:
                            shards_in_maintenance.append(shard)

                    # Public schema, with CASCADE, as flush truncates it.
                    self.handle_schema(connection=public_env.connection, allow_cascade=True)

        if shards_in_maintenance:
            raise CommandError(
                'The SQL of shard(s) {} is not printed. The SQL of the other schemas is printed.\n{}'.format(
                    ', '.join(shard.alias for shard in shards_in_maintenance),
                    '\n'.join(
                        'Shard {}: it is in maintenance. Pass --ignore-maintenance to print its SQL.'.format(
                            shard.alias
                        )
                        for shard in shards_in_maintenance
                    ),
                )
            )

    def handle_schema(self, connection, allow_cascade=False):
        self.stdout.write('SQL on {}\n'.format(connection))
        self.stdout.write(indent('\n'.join(sql_flush(self.style, connection, allow_cascade=allow_cascade)), ' ' * 4))
        self.stdout.write('\n')
