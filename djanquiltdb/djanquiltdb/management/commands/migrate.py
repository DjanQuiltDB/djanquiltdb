from djanquiltdb.management.commands.migrate_shards import Command as MigrateCommand


class Command(MigrateCommand):
    """
    This basically aliases the migrate command to the migrate_shards command. This is here because in previous versions
    migrate_shards was the command to do migrations in a sharded environment, and migrate was still used for initial
    migrations. With recent changes, the migrate_shards command can also handle initial migrations (migrations were no
    tables in the public schema are created yet).

    There are also utilities (like the test runner in Django) that will call migrate, and this makes that compatible
    without requiring excessive monkey patching.
    """

    pass
