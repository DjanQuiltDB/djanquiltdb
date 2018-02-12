from django.core.management import BaseCommand, CommandError
from django.core.management.commands.migrate import Command as MigrateCommand
from django.apps import apps
from django.db import ProgrammingError

from example.models import Shard
from sharding.utils import use_shard, get_all_databases, move_model_to_schema, ShardingMode, State, \
    create_template_schema


class Command(BaseCommand):
    """

    """
    help = 'Move all models flagged as Sharded from public to a newly made sharded schema.'

    def add_arguments(self, parser):
        parser.add_argument('--database', action='store', dest='database', default='all',
                            help='Nominates a database to execute the moving of tables. Defaults to all databases.',
                            choices=['all'] + get_all_databases())
        parser.add_argument('--target_schema_name', action='store', dest='target_schema_name', default='default_shard',
                            help='Name of the to be created schema which will receive the moved tables.')

    def handle(self, *args, **options):
        # make list of databases
        database_options = options.get('database')
        if not database_options or database_options == 'all':
            databases = get_all_databases()
        elif database_options not in get_all_databases():
            raise CommandError('You must select an existing non-primary DB.')
        else:
            databases = [database_options]

        target_schema_name = options.get('target_schema_name')

        # get all sharded models
        models = apps.get_models()
        sharded_models = [model for model in models if getattr(model, 'sharding_mode', None) == ShardingMode.SHARDED]

        for database in databases:
            # create a new shard
            create_template_schema(database)
            shard, _ = Shard.objects.get_or_create(node_name=database, schema_name=target_schema_name)
            shard.state = State.ACTIVE
            shard.save()
            # flush the shard
            with use_shard(shard) as env:
                env.connection.flush_schema(shard.schema_name)

            # move the tables over
            for model in sharded_models:
                try:
                    move_model_to_schema(shard, model, shard.schema_name)
                except ProgrammingError:
                    print("Model {} already on schema {}".format(model, shard.schema_name))
