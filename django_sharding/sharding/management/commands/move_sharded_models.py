from django.core.management import BaseCommand, CommandError
from django.apps import apps
from django.db import ProgrammingError

from sharding.utils import use_shard, get_all_databases, move_model_to_schema, ShardingMode, State, \
    create_template_schema, get_shard_class, transaction_for_every_node


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
        parser.add_argument('--no_input', action='store', dest='no_input', default=False, help='Skip confirmation.')

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

        if target_schema_name in ['public', 'template']:
            raise CommandError("Target schema name cannot be 'public' nor 'tempalte'.")

        Shard = get_shard_class()

        # Check if the target_schema does not already exist
        if Shard.objects.filter(schema_name=target_schema_name).exists():
            raise ValueError("The target schema '{}' already exist.".format(target_schema_name))

        # Get all sharded models
        models = apps.get_models()
        sharded_models = [model for model in models if getattr(model, 'sharding_mode', None) == ShardingMode.SHARDED]

        if not options.get('no_input'):
            confirm = input("Type 'yes' if you are sure if you want to move the following models from {} to {}:\n{}: "
                            .format('public', target_schema_name, [model._meta.db_table for model in sharded_models]))
            if confirm != 'yes':
                return

        with transaction_for_every_node():
            for node_name in databases:
                # Create a new shard
                create_template_schema(node_name)
                shard = Shard.objects.create(alias=target_schema_name, node_name=node_name,
                                             schema_name=target_schema_name, state=State.ACTIVE)

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
