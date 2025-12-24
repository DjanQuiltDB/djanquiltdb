from django.core.exceptions import ValidationError
from django.core.management import BaseCommand, CommandError
from django.db import transaction

from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import use_shard, get_all_databases, move_model_to_schema, State, \
    create_template_schema, get_shard_class, get_all_sharded_models, get_template_name, create_schema_on_node


class Command(BaseCommand):
    """
    This command creates a new schema and moves all sharded tables over from public to that new schema.
    It does so for a single database.
    """
    help = 'Move all models marked as sharded from public to a newly made sharded schema.'

    def add_arguments(self, parser):
        parser.add_argument('--database', action='store', dest='database', default='default',
                            help="Nominates a database to execute the moving of tables. Defaults to 'default'.",
                            choices=get_all_databases())
        parser.add_argument('--target-schema-name', action='store', dest='target_schema_name', default='default_shard',
                            help='Name of the to be created schema which will receive the moved tables.')
        parser.add_argument('--noinput', '--no-input', action='store_true', dest='no_input', default=False,
                            help='Skips confirmations.')

    def handle(self, *args, **options):
        self.no_input = options.get('no_input')

        # Make list of databases
        database = options.get('database')
        if database not in get_all_databases():
            raise CommandError('You must select an existing database.')

        target_schema_name = options.get('target_schema_name')

        if target_schema_name in [PUBLIC_SCHEMA_NAME, get_template_name()]:
            raise CommandError("Target schema name cannot be 'public' nor '{}'.".format(get_template_name()))

        create_template_schema(database)
        with transaction.atomic():
            # Create or get target shard
            self.shard, shard_created = get_shard_class().objects.get_or_create(node_name=database,
                                                                                schema_name=target_schema_name,
                                                                                defaults={'alias': target_schema_name})

            if not shard_created and not self.no_input:
                confirm = input("Type 'yes' if you are sure that you want to remove all data of existing shard {}|{} "
                                "to prepare to receive the tables from public: "
                                .format(self.shard.node_name, self.shard.schema_name))
                if confirm != 'yes':
                    return

            sharded_models = get_all_sharded_models(include_auto_created=True)  # Include many-to-many models

            if not self.no_input:
                confirm = input("Type 'yes' if you are sure if you want to move the following models "
                                "from {} to {}:\n{}: "
                                .format(PUBLIC_SCHEMA_NAME,
                                        target_schema_name,
                                        [model._meta.db_table for model in sharded_models]))
                if confirm != 'yes':
                    return

            # Ensure schema exists. If it does so already, this is a no-op.
            # We do this, for people might use a dummy object in the Shard model as target.
            # No need to migrate, since it has to be empty, and will be flushed anyway.
            create_schema_on_node(node_name=self.shard.node_name, schema_name=self.shard.schema_name, migrate=False)

            self.move_models(target_shard=self.shard, sharded_models=sharded_models)
            self.copy_migration_table(target_shard=self.shard)
            self.validate(target_shard=self.shard)

            self.shard.state = State.ACTIVE
            self.shard.save(update_fields=['state'])

    def move_models(self, target_shard, sharded_models):
        """
        Move all given models from public to the target schema
        """
        with use_shard(node_name=target_shard.node_name, schema_name=PUBLIC_SCHEMA_NAME):
            # Flush the shard
            with use_shard(target_shard, lock=False, active_only_schemas=False) as env:
                env.connection.flush_schema(target_shard.schema_name)

            # Move the tables over
            for model in sharded_models:
                move_model_to_schema(model=model, node_name='default', from_schema_name=PUBLIC_SCHEMA_NAME,
                                     to_schema_name=target_shard.schema_name)

    def copy_migration_table(self, target_shard):
        """
        Copy the django_migrations table by performing the following steps:
        - Copy Sequence
        - Create table
        - Copy contents
        - Copy sequence
        - Set default value for id to the new sequence
        """
        with use_shard(target_shard, include_public=False, lock=False, active_only_schemas=False) as env:
            sql = """
            CREATE SEQUENCE django_migrations_id_seq;
            SELECT setval('{sequence}',
                          (SELECT last_value FROM "{source_schema}"."{sequence}"),
                          (SELECT is_called FROM "{source_schema}"."{sequence}"));
            CREATE TABLE "{target_schema}"."{table_name}" (LIKE "{source_schema}"."{table_name}" INCLUDING ALL);
            INSERT INTO "{target_schema}"."{table_name}" (SELECT * FROM "{source_schema}"."{table_name}");
            ALTER TABLE "{target_schema}"."{table_name}" ALTER COLUMN "id"
            SET DEFAULT nextval('{target_schema}.{sequence}');
            """.format(target_schema=target_shard.schema_name,  # nosec
                       source_schema=PUBLIC_SCHEMA_NAME,
                       sequence='django_migrations_id_seq',
                       table_name='django_migrations')

            cursor = env.connection.cursor()
            cursor.execute(sql)

    def validate(self, target_shard):
        """Validate against template schema"""

        template_schema_name = get_template_name()
        with use_shard(node_name=target_shard.node_name, schema_name=template_schema_name, lock=False,
                       include_public=False) as env:
            template_tables = sorted(env.connection.get_all_table_headers())

        with use_shard(target_shard, lock=False, active_only_schemas=False, include_public=False) as env:
            target_tables = sorted(env.connection.get_all_table_headers())

        if target_tables != template_tables:
            raise(ValidationError('The following tables are not moved: {}'
                                  .format(set(template_tables) - set(target_tables))))
