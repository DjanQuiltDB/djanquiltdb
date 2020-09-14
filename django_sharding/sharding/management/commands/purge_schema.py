from django.core.management import BaseCommand, CommandError
from django.db.utils import IntegrityError
from django.utils import termcolors

from sharding.options import ShardOptions
from sharding.utils import get_shard_class, get_all_databases


class Command(BaseCommand):
    help = 'Remove a schema and all data on it.'

    shard, options = None, {}

    def __init__(self, stdout=None, stderr=None, no_color=False):
        super().__init__(stdout=stdout, stderr=stderr, no_color=no_color)

        if no_color:
            self.style.ACCENT = termcolors.make_style()
            self.style.BOLD = termcolors.make_style()
        else:
            self.style.ACCENT = termcolors.make_style(fg='magenta')
            self.style.BOLD = termcolors.make_style(opts=('bold',))

    def add_arguments(self, parser):
        parser.add_argument('--schema-name', '-s',
                            action='store',
                            dest='schema_name',
                            help='Name of the schema to be deleted.')
        parser.add_argument('--node-alias', '-n',
                            action='store',
                            dest='node_alias',
                            help="alias of the node where the schema resides.",
                            default=False)
        parser.add_argument('-q', '--quiet', '--silent', action='store_true', dest='quiet', help='Suppress output.',
                            default=False)
        parser.add_argument('--noinput', '--no-input',
                            action='store_false',
                            dest='interactive',
                            help='Do NOT prompt the user for input of any kind and assume "yes" on all questions.',
                            default=True)

    def print(self, *args):
        if not self.quiet:
            print(*args)

    def handle(self, *args, **options):
        self.options.update(options)

        self.quiet = self.options.get('quiet')
        self.node = self.get_node(self.options)
        self.node_options = ShardOptions(node_name=self.node, schema_name='public')
        self.schema_name = self.get_schema_name(self.options, self.node_options)

        if not self.confirm():
            self.print('\nOperation cancelled.')
            return

        self.delete_schema()

        self.print("\nDone. schema '{}' has been removed from node '{}'".format(self.schema_name, self.node))

    def get_node(self, options):
        node = options['node_alias']
        if node not in get_all_databases():
            raise CommandError("Could not find node '{}' in known set of databases".format(node))

        return node

    def get_schema_name(self, options, node_options):
        schema_name = options['schema_name']
        error = CommandError("Could not find schema '{}' on node '{}'".format(schema_name, node_options.node_name))
        with node_options.use() as env:
            try:
                if not env.connection.get_ps_schema(schema_name):
                    raise error
            except IntegrityError:
                raise error

        if get_shard_class().objects.filter(schema_name=schema_name, node_name=node_options.node_name).exists():
            raise CommandError("schema '{}' on node '{}' is in use! Delete the Shard object using it if you want to "
                               "remove it."
                               .format(schema_name, node_options.node_name))

        return schema_name

    def confirm(self):
        if self.options['interactive']:
            confirm_msg = \
                "\nYou have requested to delete schema '{}' from node {}.\nThis " \
                "will IRREVERSIBLY DESTROY all data that lives on this schema.\n" \
                "Are you sure you want to do this?\n" \
                "\n\tType 'yes' to continue, or 'no' to cancel: ".format(
                    self.style.BOLD(self.schema_name), self.style.BOLD(self.node)
                )

            confirm = input(confirm_msg)

            if confirm != 'yes':
                return False

        return True

    def delete_schema(self):
        with self.node_options.use() as env:
            env.connection.delete_schema(self.schema_name)
