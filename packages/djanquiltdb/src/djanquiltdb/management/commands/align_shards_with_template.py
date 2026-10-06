from django.core.management import BaseCommand, CommandError
from django.db import DatabaseError, connections, transaction

from djanquiltdb.management.base import get_databases_and_schema_from_options, get_shards_by_node
from djanquiltdb.utils import get_all_databases, get_template_name, schema_exists, use_shard


class Command(BaseCommand):
    help = (
        'Give each shard the sequence ownership, sequence settings and persistence, primary key names and identity '
        "sequence names of its node's template, and make its serial columns take their values from its own sequences."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--database',
            action='store',
            dest='database',
            default='all',
            choices=['all'] + get_all_databases(),
            help='Nominates a database whose shards are aligned. Defaults to all databases.',
        )
        parser.add_argument(
            '--schema-name',
            '-s',
            action='store',
            dest='schema_name',
            help='Nominates the schema of the one shard to align. When empty, all shards are aligned.',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            dest='dry_run',
            help='Print the statements that align each shard without running them.',
        )
        parser.add_argument(
            '--skip-name-clashes',
            action='store_true',
            dest='skip_name_clashes',
            help=(
                'Skip each rename to a name that is already in use in the shard and print it, instead of stopping at '
                'the first shard that has one.'
            ),
        )

    def handle(self, **options):
        dry_run = options['dry_run']
        skip_name_clashes = options['skip_name_clashes']
        options['check_shard'] = False  # A schema name is matched against the shards of the selected nodes below.
        node_names, schema_name = get_databases_and_schema_from_options(options)

        shards_by_node = get_shards_by_node(node_names)
        if schema_name:
            shards_by_node = {
                node_name: [shard for shard in shards if shard.schema_name == schema_name]
                for node_name, shards in shards_by_node.items()
            }
            if not any(shards_by_node.values()):
                raise CommandError("No shard has the schema name '{}'.".format(schema_name))

        # Every node and shard is checked before any shard is aligned, so that a run stopped here has changed nothing.
        missing_schemas = []
        for node_name in node_names:
            shards = shards_by_node.get(node_name, [])
            if shards and not schema_exists(node_name, get_template_name()):
                missing_schemas.append("Node '{}' has no template schema.".format(node_name))
            for shard in shards:
                if not schema_exists(node_name, shard.schema_name):
                    missing_schemas.append('Shard {}|{} has no schema.'.format(node_name, shard.schema_name))
        if missing_schemas:
            raise CommandError('Nothing was aligned. {}'.format(' '.join(missing_schemas)))

        shards_of_nodes = [
            (node_name, sorted(shards_by_node.get(node_name, []), key=lambda shard: shard.schema_name))
            for node_name in node_names
        ]
        shards_in_order = [shard for _, node_shards in shards_of_nodes for shard in node_shards]
        index = 0
        for node_name, node_shards in shards_of_nodes:
            if not node_shards and not schema_name:
                self.stdout.write("Node '{}' has no shards.\n".format(node_name))
            for position, shard in enumerate(node_shards):
                try:
                    if not dry_run and position == 0:
                        # Installed once per node, outside the shards' transactions. Otherwise a shard's transaction
                        # would keep a lock on the functions while it waits for the locks its statements need, which
                        # would block every shard created on the node in the meantime.
                        connections[node_name].set_clone_function()

                    self.apply_alignment_to_shard(shard, dry_run, skip_name_clashes)
                except DatabaseError as error:
                    raise CommandError(
                        '{}|{} cannot be aligned with the template: {} Shards not aligned: {}.'.format(
                            node_name,
                            shard.schema_name,
                            (str(error).splitlines() or [type(error).__name__])[0],
                            ', '.join(
                                '{}|{}'.format(remaining_shard.node_name, remaining_shard.schema_name)
                                for remaining_shard in shards_in_order[index:]
                            ),
                        )
                    ) from error
                index += 1

        if dry_run:
            self.stdout.write('Dry run: nothing was changed.\n')

    def apply_alignment_to_shard(self, shard, dry_run, skip_name_clashes):
        # Taking the shard's lock waits for a move of the shard to finish, and the transaction keeps the lock until the
        # statements have run. A dry run installs the functions that list the statements itself, and rolls that back
        # together with everything else. It installs them inside the shard's transaction, because an install that is
        # rolled back on its own would leave no functions for that transaction to call. The functions' catalog rows
        # then stay locked while the shard's statements are listed and printed. A dry run runs none of the statements,
        # so a shard created on the node in the meantime waits at most that long.
        with transaction.atomic(using=shard.node_name), use_shard(shard, active_only_schemas=False) as env:
            if dry_run:
                env.connection.set_clone_function()
            statements, clashes = env.connection.list_template_alignment_statements(
                get_template_name(), shard.schema_name, skip_clashes=skip_name_clashes
            )

            if not statements and not clashes:
                self.stdout.write('{}|{} matches the template.\n'.format(shard.node_name, shard.schema_name))
            if statements:
                self.stdout.write('Aligning {}|{} with the template:\n'.format(shard.node_name, shard.schema_name))
                for statement in statements:
                    self.stdout.write('    {};\n'.format(statement))
            if clashes:
                self.stdout.write(
                    'Renames skipped in {}|{} because the name is already in use:\n'.format(
                        shard.node_name, shard.schema_name
                    )
                )
                for clash in clashes:
                    self.stdout.write('    {}\n'.format(clash))

            if dry_run:
                transaction.set_rollback(True, using=shard.node_name)
            else:
                cursor = env.connection.cursor()
                for statement in statements:
                    cursor.execute(statement)
