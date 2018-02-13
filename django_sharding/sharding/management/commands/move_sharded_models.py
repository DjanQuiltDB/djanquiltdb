from django.core.management import BaseCommand, CommandError
from django.apps import apps
from django.db import ProgrammingError

from sharding.utils import use_shard, get_all_databases, move_model_to_schema, ShardingMode, State, \
    create_template_schema, get_shard_class


class Command(BaseCommand):
    """
    This command creates a new schema and moves all sharded tables over from public to that new schema.
    It does so for each database known.
    """
    help = 'Move all models flagged as sharded from public to a newly made sharded schema.'

    def add_arguments(self, parser):
        parser.add_argument('--database', action='store', dest='database', default='all',
                            help='Nominates a database to execute the moving of tables. Defaults to all databases.',
                            choices=['all'] + get_all_databases())
        parser.add_argument('--target_schema_name', action='store', dest='target_schema_name', default='default_shard',
                            help='Name of the to be created schema which will receive the moved tables.')

    def handle(self, *args, **options):
        # Make list of databases
        database_options = options.get('database')
        if not database_options or database_options == 'all':
            databases = get_all_databases()
        elif database_options not in get_all_databases():
            raise CommandError('You must select an existing non-primary DB.')
        else:
            databases = [database_options]

        target_schema_name = options.get('target_schema_name')

        Shard = get_shard_class()

        # Check if the target_schema does not already exist
        for database in databases:
            with use_shard(node_name=database, schema_name='public'):
                if Shard.objects.filter(schema_name=target_schema_name).exists():
                    raise ValueError('The target schema {} already exists on node {}.'.format(target_schema_name,
                                                                                              database))

        # Get all sharded models
        models = apps.get_models()
        sharded_models = [model for model in models if getattr(model, 'sharding_mode', None) == ShardingMode.SHARDED]

        for database in databases:
            # Create a new shard
            create_template_schema(database)
            shard = Shard.objects.create(node_name=database, schema_name=target_schema_name, state=State.ACTIVE)

            # Flush the shard
            with use_shard(shard) as env:
                env.connection.flush_schema(shard.schema_name)

            # Move the tables over
            for model in sharded_models:
                try:
                    move_model_to_schema(model=model, node_name='default', from_schema_name='public',
                                         to_schema_name=shard.schema_name)
                except ProgrammingError:
                    print('Model {} already on schema {}'.format(model, shard.schema_name))
