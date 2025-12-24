from django.core.management.commands.flush import Command as FlushCommand
from django.db import connections

from djanquiltdb.management.base import get_databases_and_schema_from_options, shard_table_exists
from djanquiltdb.options import ShardOptions
from djanquiltdb.utils import get_all_databases, get_shard_class, get_template_name


class Command(FlushCommand):
    def add_arguments(self, parser):
        super().add_arguments(parser)

        # Since we can now target multiple databases change the default to 'all' and the options to databases allowed.
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
            help='Nominates a schema to flush. When empty all schemas will be flushed.',
        )

    def handle(self, **options):
        interactive = options['interactive']

        if interactive:
            confirm = input(
                'You have requested a flush of the database.\n'
                'This will IRREVERSIBLY DESTROY all data currently in the selected database,\n'
                'and return each table to an empty state.\n'
                'Are you sure you want to do this?\n'
                "    Type 'yes' to continue, or 'no' to cancel: "
            )

            if confirm != 'yes':
                self.stdout.write('Flush cancelled.\n')
                return

        node_names, schema_name = get_databases_and_schema_from_options(options)

        options['interactive'] = False  # Don't want this each time we flush a schema as well

        template_name = get_template_name()

        for node_name in node_names:
            if schema_name:
                schema_options = options.copy()
                schema_options['database'] = ShardOptions(node_name=node_name, schema_name=schema_name)
                super().handle(**schema_options)
            else:
                connection_ = connections[node_name]
                # Pick the public schema of the current node as default connection, so we do checks like for example if
                # the template schema exists and if the shard table exists on the correct node.

                # Flush all schemas on this database, starting with the template schema, if it exists.
                if connection_.get_ps_schema(template_name):
                    template_options = options.copy()
                    template_options['database'] = ShardOptions(
                        node_name=node_name, schema_name=template_name, include_public=False
                    )
                    super().handle(**template_options)

                # And now all other shards, but only if the shard table exists on the public schema. If not, we can
                # assume that not other shards exists.
                if shard_table_exists(node_name):
                    for shard in get_shard_class().objects.filter(node_name__in=node_names):
                        shard_options = options.copy()
                        shard_options['database'] = ShardOptions.from_shard(shard)
                        super().handle(**shard_options)

                # And finally do the public schema. We do this as last, to make sure we don't face constraints for
                # flushing.
                public_options = options.copy()
                public_options['database'] = node_name

                # We can set allow_cascade to True, since there shouldn't be any other entries anymore in other
                # shards or the template schema.
                public_options['allow_cascade'] = True
                super().handle(**public_options)
