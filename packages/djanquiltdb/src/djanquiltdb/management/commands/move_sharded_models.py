from django.core.exceptions import ValidationError
from django.core.management import BaseCommand, CommandError
from django.db import transaction

from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import (
    State,
    create_schema_on_node,
    create_template_schema,
    get_all_databases,
    get_all_sharded_models,
    get_shard_class,
    get_template_name,
    move_model_to_schema,
    use_shard,
)


class Command(BaseCommand):
    """
    This command creates a new schema and moves all sharded tables over from public to that new schema.
    It does so for a single database.
    """

    help = 'Move all models marked as sharded from public to a newly made sharded schema.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--database',
            action='store',
            dest='database',
            default='default',
            help="Nominates a database to execute the moving of tables. Defaults to 'default'.",
            choices=get_all_databases(),
        )
        parser.add_argument(
            '--target-schema-name',
            action='store',
            dest='target_schema_name',
            default='default_shard',
            help='Name of the to be created schema which will receive the moved tables.',
        )
        parser.add_argument(
            '--noinput', '--no-input', action='store_true', dest='no_input', default=False, help='Skips confirmations.'
        )

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
            self.shard, shard_created = get_shard_class().objects.get_or_create(
                node_name=database, schema_name=target_schema_name, defaults={'alias': target_schema_name}
            )

            if not shard_created and not self.no_input:
                confirm = input(
                    "Type 'yes' if you are sure that you want to remove all data of existing shard {}|{} "
                    'to prepare to receive the tables from public: '.format(
                        self.shard.node_name, self.shard.schema_name
                    )
                )
                if confirm != 'yes':
                    return

            sharded_models = get_all_sharded_models(include_auto_created=True)  # Include many-to-many models

            if not self.no_input:
                confirm = input(
                    "Type 'yes' if you are sure if you want to move the following models from {} to {}:\n{}: ".format(
                        PUBLIC_SCHEMA_NAME, target_schema_name, [model._meta.db_table for model in sharded_models]
                    )
                )
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
                move_model_to_schema(
                    model=model,
                    node_name='default',
                    from_schema_name=PUBLIC_SCHEMA_NAME,
                    to_schema_name=target_shard.schema_name,
                )

    def copy_migration_table(self, target_shard):
        """
        Copy the django_migrations table by performing the following steps:
        - Create table (LIKE source INCLUDING ALL copies identity column settings)
        - Copy contents
        - Reset identity sequence to continue from max(id) + 1
        """
        with use_shard(target_shard, include_public=False, lock=False, active_only_schemas=False) as env:
            cursor = env.connection.cursor()

            # Create table with same structure (including identity column if present)
            cursor.execute(
                'CREATE TABLE "{target_schema}"."{table_name}" (LIKE "{source_schema}"."{table_name}" INCLUDING ALL)'.format(
                    target_schema=target_shard.schema_name,
                    source_schema=PUBLIC_SCHEMA_NAME,
                    table_name='django_migrations',
                )
            )

            # Copy data
            cursor.execute(
                'INSERT INTO "{target_schema}"."{table_name}" (SELECT * FROM "{source_schema}"."{table_name}")'.format(
                    target_schema=target_shard.schema_name,
                    source_schema=PUBLIC_SCHEMA_NAME,
                    table_name='django_migrations',
                )
            )

            # Reset identity sequence to continue from max(id) + 1 to avoid conflicts
            # Django 6.0 uses identity columns (GENERATED BY DEFAULT AS IDENTITY) instead of sequences
            # pg_get_serial_sequence works for both sequences and identity columns
            cursor.execute(
                """
                DO $$
                DECLARE
                    seq_name TEXT;
                    max_id_val BIGINT;
                BEGIN
                    seq_name := pg_get_serial_sequence('{target_schema}.{table_name}', 'id');
                    IF seq_name IS NOT NULL THEN
                        SELECT COALESCE(MAX(id), 0) INTO max_id_val FROM "{target_schema}"."{table_name}";
                        PERFORM setval(seq_name, max_id_val + 1, false);
                    END IF;
                END $$;
            """.format(target_schema=target_shard.schema_name, table_name='django_migrations')
            )

    def validate(self, target_shard):
        """Validate against template schema"""

        template_schema_name = get_template_name()
        with use_shard(
            node_name=target_shard.node_name, schema_name=template_schema_name, lock=False, include_public=False
        ) as env:
            template_tables = sorted(env.connection.get_all_table_headers())

        with use_shard(target_shard, lock=False, active_only_schemas=False, include_public=False) as env:
            target_tables = sorted(env.connection.get_all_table_headers())

        if target_tables != template_tables:
            raise (
                ValidationError(
                    'The following tables are not moved: {}'.format(set(template_tables) - set(target_tables))
                )
            )
