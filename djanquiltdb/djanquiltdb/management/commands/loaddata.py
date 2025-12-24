from django.core.management.commands.loaddata import Command as LoadDataCommand

from djanquiltdb.options import ShardOptions


class Command(LoadDataCommand):
    def handle(self, *fixture_labels, **options):
        database = options.pop('database', None)

        # The command expects `database` to be a string, and it doesn't work out-of-the-box with something else. So we
        # cast the database options to a string here.
        if not isinstance(database, str):
            # `database` could be a tuple, a shard or ShardOptions instance. So we pass it to ShardOptions.from_alias(),
            # so we end up with a ShardOptions instance, from where we can get the node name and schema name.
            shard_options = ShardOptions.from_alias(database)
            database = '{}|{}'.format(shard_options.node_name, shard_options.schema_name)

        super().handle(*fixture_labels, database=database, **options)
