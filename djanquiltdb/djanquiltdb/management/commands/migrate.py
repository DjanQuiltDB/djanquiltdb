from djanquiltdb.management.commands.migrate_shards import Command as MigrateCommand


class Command(MigrateCommand):
    """
    This basically points the migrate command to the migrate_shards command. This is here because in previous versions
    migrate_shards was the command to do migrations in a sharded environment. With recent changes, the migrate_shards
    command can also handle initial migrations (migrations were no tables in the public schema are created yet).
    """
    pass
