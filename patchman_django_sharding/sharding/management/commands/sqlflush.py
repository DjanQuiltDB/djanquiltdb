from __future__ import unicode_literals

from textwrap import indent

from django.core.management.commands.sqlflush import Command as SQLFlushCommand
from django.core.management.sql import sql_flush

from sharding.management.base import get_databases_and_schema_from_options, shard_table_exists
from sharding.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from sharding.utils import get_all_databases, use_shard, get_shard_class, get_template_name


class Command(SQLFlushCommand):
    output_transaction = False  # We are going to output our information in a different way

    def add_arguments(self, parser):
        super().add_arguments(parser)

        # Since we can now target multiple databases change the default to 'all' and the options to databases allowed.
        parser._option_string_actions['--database'].default = 'all'
        parser._option_string_actions['--database'].help = \
            'Nominates a database to print the SQL for. Defaults to all databases.'
        parser._option_string_actions['--database'].choices = ['all'] + get_all_databases()

        parser.add_argument(
            '--schema-name', '-s', action='store', dest='schema_name',
            help='Nominates a schema to print the SQL for. When empty all schemas will be shown.'
        )

    def handle(self, **options):
        node_names, schema_name = get_databases_and_schema_from_options(options)

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
                        with use_shard(node_name=node_name, schema_name=get_template_name(),
                                       include_public=False) as env:
                            self.handle_schema(connection=env.connection)

                    # All shards, if we can determine that from the shard table.
                    if shard_table_exists(node_name):
                        for shard in get_shard_class().objects.filter(node_name__in=node_names):
                            with use_shard(shard, include_public=False) as env:
                                self.handle_schema(connection=env.connection)

                    # Public schema
                    self.handle_schema(connection=public_env.connection)

    def handle_schema(self, connection):
        self.stdout.write('SQL on {}\n'.format(connection))
        self.stdout.write(indent('\n'.join(sql_flush(self.style, connection, only_django=True)), ' ' * 4))
        self.stdout.write('\n')
