from django.core.management import BaseCommand, CommandError
from django.db import ProgrammingError

from sharding.utils import use_shard, get_all_databases, move_model_to_schema, State, \
    create_template_schema, get_shard_class, get_all_sharded_models, get_template_name


class Command(BaseCommand):
    """
    This command creates a new schema and moves all sharded tables over from public to that new schema.
    It does so for a single database.
    """
    help = 'Move all models flagged as sharded from public to a newly made sharded schema.'

    def add_arguments(self, parser):
        parser.add_argument('--database', action='store', dest='database', default='default',
                            help="Nominates a database to execute the moving of tables. Defaults to 'default'.",
                            choices=get_all_databases())
        parser.add_argument('--target_schema_name', action='store', dest='target_schema_name', default='default_shard',
                            help='Name of the to be created schema which will receive the moved tables.')
        parser.add_argument('--no_input', action='store', dest='no_input', default=False, help='Skip confirmation.')

    def handle(self, *args, **options):
        # Make list of databases
        database = options.get('database')
        if database not in get_all_databases():
            raise CommandError('You must select an existing database.')

        target_schema_name = options.get('target_schema_name')

        if target_schema_name in ['public', get_template_name()]:
            raise CommandError("Target schema name cannot be 'public' nor '{}'.".format(get_template_name()))

        # Check if the target_schema does not already exist
        if get_shard_class().objects.filter(node_name=database, schema_name=target_schema_name).exists():
            raise ValueError("The target schema '{}' on node '{}' already exist.".format(target_schema_name, database))

        sharded_models = get_all_sharded_models()

        if not options.get('no_input'):
            confirm = input("Type 'yes' if you are sure if you want to move the following models from {} to {}:\n{}: "
                            .format('public', target_schema_name, [model._meta.db_table for model in sharded_models]))
            if confirm != 'yes':
                return

        self.move_models(node_name=database, target_schema_name=target_schema_name, sharded_models=sharded_models)

    def move_models(self, node_name, target_schema_name, sharded_models):
        """
        Move all given models from public to the target schema
        """
        # Create a new shard
        with use_shard(node_name=node_name, schema_name='public'):
            create_template_schema(node_name)
            shard = get_shard_class().objects.create(alias=target_schema_name, node_name=node_name,
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
